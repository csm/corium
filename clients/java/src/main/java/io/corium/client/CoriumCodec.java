package io.corium.client;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Encoder and decoder for the Corium composite wire format. */
public final class CoriumCodec {
    private static final int NIL = 0x00;
    private static final int BOOL = 0x10;
    private static final int LONG = 0x20;
    private static final int DOUBLE = 0x30;
    private static final int INSTANT = 0x40;
    private static final int UUID_TAG = 0x50;
    private static final int KEYWORD = 0x61;
    private static final int STRING = 0x71;
    private static final int BYTES = 0x81;
    private static final int REF = 0x90;
    private static final int LIST = 0xA0;
    private static final int VECTOR = 0xA1;
    private static final int MAP = 0xA2;
    private static final int SET = 0xA3;
    private static final int TAGGED = 0xA4;
    private static final int SYMBOL = 0xA5;

    private CoriumCodec() {}

    public static byte[] encode(Object value) {
        Writer writer = new Writer();
        writer.write(value);
        return writer.bytes();
    }

    public static Object decode(byte[] bytes) {
        Reader reader = new Reader(bytes);
        Object value = reader.read();
        reader.expectEnd();
        return value;
    }

    static List<Datom> decodeDatoms(byte[] bytes) {
        Reader reader = new Reader(bytes);
        int count = reader.count();
        List<Datom> datoms = new ArrayList<>(Math.min(count, 65_536));
        for (int index = 0; index < count; index++) {
            EntityId entity = EntityId.fromRaw(reader.varint());
            EntityId attribute = EntityId.fromRaw(reader.varint());
            EntityId transaction = EntityId.fromRaw(reader.varint());
            boolean added = reader.takeByte() != 0;
            datoms.add(new Datom(entity, attribute, reader.readValue(), transaction, added));
        }
        reader.expectEnd();
        return Collections.unmodifiableList(datoms);
    }

    private static Object lower(Object value) {
        Object current = value;
        while (current instanceof EdnForm) {
            current = ((EdnForm) current).toEdn();
        }
        return current;
    }

    private static final class Writer {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private final Map<String, Long> interned = new LinkedHashMap<>();

        byte[] bytes() {
            return output.toByteArray();
        }

        void write(Object input) {
            Object value = lower(input);
            if (value == null) {
                output.write(NIL);
            } else if (value instanceof Boolean) {
                output.write(BOOL);
                output.write((Boolean) value ? 1 : 0);
            } else if (value instanceof Byte || value instanceof Short
                    || value instanceof Integer || value instanceof Long) {
                output.write(LONG);
                zigzag(((Number) value).longValue());
            } else if (value instanceof Float || value instanceof Double) {
                output.write(DOUBLE);
                writeDouble(((Number) value).doubleValue());
            } else if (value instanceof String) {
                output.write(STRING);
                intern((String) value);
            } else if (value instanceof Keyword) {
                output.write(KEYWORD);
                intern(((Keyword) value).name());
            } else if (value instanceof Symbol) {
                output.write(SYMBOL);
                intern(((Symbol) value).name());
            } else if (value instanceof Instant) {
                tagged("inst", ((Instant) value).toEpochMilli());
            } else if (value instanceof UUID) {
                UUID uuid = (UUID) value;
                tagged("uuid", String.format("%016x%016x", uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
            } else if (value instanceof byte[]) {
                tagged("bytes", toHex((byte[]) value));
            } else if (value instanceof EntityId) {
                EntityId id = (EntityId) value;
                if (id.raw() < 0) {
                    throw new IllegalArgumentException("entity id exceeds the signed EDN boundary range");
                }
                tagged("eid", id.raw());
            } else if (value instanceof Tagged) {
                Tagged tagged = (Tagged) value;
                tagged(tagged.tag(), tagged.value());
            } else if (value instanceof EdnList) {
                sequence(LIST, ((EdnList) value).items());
            } else if (value instanceof List<?>) {
                sequence(VECTOR, (List<?>) value);
            } else if (value instanceof Object[]) {
                sequence(VECTOR, Arrays.asList((Object[]) value));
            } else if (value instanceof Set<?>) {
                sequence(SET, new ArrayList<>((Set<?>) value));
            } else if (value instanceof Map<?, ?>) {
                output.write(MAP);
                Map<?, ?> map = (Map<?, ?>) value;
                varint(map.size());
                map.forEach((key, item) -> {
                    write(key);
                    write(item);
                });
            } else {
                throw new IllegalArgumentException("unsupported Corium boundary value: " + value.getClass().getName());
            }
        }

        private void tagged(String tag, Object value) {
            output.write(TAGGED);
            intern(tag);
            write(value);
        }

        private void sequence(int tag, List<?> values) {
            output.write(tag);
            varint(values.size());
            values.forEach(this::write);
        }

        private void writeDouble(double value) {
            long bits = Double.doubleToRawLongBits(value);
            long sortable = bits < 0 ? ~bits : bits ^ Long.MIN_VALUE;
            for (int shift = 56; shift >= 0; shift -= 8) {
                output.write((int) (sortable >>> shift) & 0xff);
            }
        }

        private void zigzag(long value) {
            varint((value << 1) ^ (value >> 63));
        }

        private void varint(long value) {
            long remaining = value;
            while (true) {
                int next = (int) (remaining & 0x7f);
                remaining >>>= 7;
                if (remaining == 0) {
                    output.write(next);
                    return;
                }
                output.write(next | 0x80);
            }
        }

        private void intern(String value) {
            Long existing = interned.get(value);
            if (existing != null) {
                varint(existing);
                return;
            }
            interned.put(value, (long) interned.size() + 1);
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            varint(0);
            varint(bytes.length);
            output.write(bytes, 0, bytes.length);
        }
    }

    private static final class Reader {
        private final byte[] input;
        private final List<String> interned = new ArrayList<>();
        private int position;

        Reader(byte[] input) {
            this.input = Arrays.copyOf(input, input.length);
        }

        Object read() {
            int tag = takeByte();
            switch (tag) {
                case NIL: return null;
                case BOOL: return takeByte() != 0;
                case LONG: return unzigzag(varint());
                case DOUBLE: return readDouble();
                case STRING: return intern();
                case KEYWORD: return new Keyword(intern());
                case SYMBOL: return new Symbol(intern());
                case LIST: return new EdnList(items());
                case VECTOR: return Collections.unmodifiableList(items());
                case SET: return Collections.unmodifiableSet(new LinkedHashSet<>(items()));
                case MAP: return map();
                case TAGGED: return readTagged(intern(), read());
                default: throw error(String.format("unknown wire tag 0x%02x", tag));
            }
        }

        void expectEnd() {
            if (position != input.length) throw error("trailing bytes after wire payload");
        }

        private List<Object> items() {
            int count = count();
            List<Object> values = new ArrayList<>(Math.min(count, 4096));
            for (int index = 0; index < count; index++) values.add(read());
            return values;
        }

        private Map<Object, Object> map() {
            int count = count();
            Map<Object, Object> values = new LinkedHashMap<>();
            for (int index = 0; index < count; index++) values.put(read(), read());
            return Collections.unmodifiableMap(values);
        }

        private Object readTagged(String tag, Object value) {
            try {
                switch (tag) {
                    case "eid": return EntityId.of((Long) value);
                    case "inst": return Instant.ofEpochMilli((Long) value);
                    case "uuid": return uuidFromHex((String) value);
                    case "bytes": return fromHex((String) value);
                    default: return new Tagged(tag, value);
                }
            } catch (ClassCastException | IllegalArgumentException error) {
                throw new DecodeException("invalid #" + tag + " boundary value", error);
            }
        }

        private double readDouble() {
            long sortable = 0;
            for (int index = 0; index < 8; index++) sortable = (sortable << 8) | takeByte();
            long bits = sortable >= 0 ? ~sortable : sortable ^ Long.MIN_VALUE;
            return Double.longBitsToDouble(bits);
        }

        private Object readValue() {
            int tag = takeByte();
            switch (tag) {
                case BOOL: return takeByte() != 0;
                case LONG: return unzigzag(varint());
                case DOUBLE: return readDouble();
                case INSTANT: return Instant.ofEpochMilli(unzigzag(varint()));
                case UUID_TAG:
                    long high = 0;
                    long low = 0;
                    for (int index = 0; index < 8; index++) high = (high << 8) | takeByte();
                    for (int index = 0; index < 8; index++) low = (low << 8) | takeByte();
                    return new UUID(high, low);
                case KEYWORD: return new Keyword(intern());
                case STRING: return intern();
                case BYTES:
                    int length = count();
                    require(length);
                    byte[] bytes = Arrays.copyOfRange(input, position, position + length);
                    position += length;
                    return bytes;
                case REF: return EntityId.fromRaw(varint());
                default: throw error(String.format("unknown value tag 0x%02x", tag));
            }
        }

        private String intern() {
            long index = varint();
            if (index == 0) {
                int length = count();
                require(length);
                String value;
                try {
                    value = StandardCharsets.UTF_8.newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(input, position, length)).toString();
                } catch (CharacterCodingException error) {
                    throw new DecodeException("invalid UTF-8 string at byte " + position, error);
                }
                position += length;
                interned.add(value);
                return value;
            }
            if (index < 0 || Long.compareUnsigned(index, interned.size()) > 0) {
                throw error("invalid intern reference " + Long.toUnsignedString(index));
            }
            return interned.get((int) index - 1);
        }

        private int count() {
            long value = varint();
            if (value < 0 || value > Integer.MAX_VALUE) throw error("wire length out of range");
            return (int) value;
        }

        private long varint() {
            long value = 0;
            int shift = 0;
            while (shift <= 63) {
                int next = takeByte();
                value |= (long) (next & 0x7f) << shift;
                if ((next & 0x80) == 0) return value;
                shift += 7;
            }
            throw error("wire length out of range");
        }

        private int takeByte() {
            require(1);
            return input[position++] & 0xff;
        }

        private void require(int count) {
            if (count < 0 || position > input.length - count) throw error("truncated wire payload");
        }

        private DecodeException error(String message) {
            return new DecodeException(message + " at byte " + position);
        }
    }

    private static long unzigzag(long value) {
        return (value >>> 1) ^ -(value & 1);
    }

    private static String toHex(byte[] bytes) {
        char[] chars = new char[bytes.length * 2];
        char[] alphabet = "0123456789abcdef".toCharArray();
        for (int index = 0; index < bytes.length; index++) {
            chars[index * 2] = alphabet[(bytes[index] >>> 4) & 0xf];
            chars[index * 2 + 1] = alphabet[bytes[index] & 0xf];
        }
        return new String(chars);
    }

    private static byte[] fromHex(String value) {
        if ((value.length() & 1) != 0) throw new IllegalArgumentException("odd-length hex value");
        byte[] output = new byte[value.length() / 2];
        for (int index = 0; index < output.length; index++) {
            int high = Character.digit(value.charAt(index * 2), 16);
            int low = Character.digit(value.charAt(index * 2 + 1), 16);
            if (high < 0 || low < 0) throw new IllegalArgumentException("invalid hex value");
            output[index] = (byte) ((high << 4) | low);
        }
        return output;
    }

    private static UUID uuidFromHex(String value) {
        if (value.length() != 32) throw new IllegalArgumentException("UUID must contain 32 hex digits");
        return new UUID(Long.parseUnsignedLong(value.substring(0, 16), 16),
                Long.parseUnsignedLong(value.substring(16), 16));
    }
}
