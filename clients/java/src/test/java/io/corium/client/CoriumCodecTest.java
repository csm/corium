package io.corium.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

final class CoriumCodecTest {
    @Test
    void matchesTheRustInterningFixture() {
        byte[] encoded = CoriumCodec.encode(List.of("foo", "foo", new Keyword("foo")));
        assertArrayEquals(new byte[] {
                (byte) 0xa1, 0x03,
                0x71, 0x00, 0x03, 'f', 'o', 'o',
                0x71, 0x01,
                0x61, 0x01,
        }, encoded);
    }

    @Test
    void roundTripsCompositeAndDedicatedBoundaryValues() {
        Map<Object, Object> value = new LinkedHashMap<>();
        value.put(new Keyword("person/name"), "Ada");
        value.put(new Keyword("person/age"), 36L);
        value.put(new Keyword("flags"), Set.of(true, false));
        value.put(new Keyword("list"), EdnList.of(new Symbol("?x"), -0.5));
        value.put(new Keyword("instant"), Instant.ofEpochMilli(1_700_000_000_123L));
        value.put(new Keyword("uuid"), UUID.fromString("12345678-9abc-def0-1122-334455667788"));

        Object decoded = CoriumCodec.decode(CoriumCodec.encode(value));
        assertEquals(value, decoded);

        byte[] bytes = {0, 1, 2, (byte) 0xff};
        assertArrayEquals(bytes, (byte[]) CoriumCodec.decode(CoriumCodec.encode(bytes)));
        assertEquals(EntityId.of(42), CoriumCodec.decode(CoriumCodec.encode(EntityId.of(42))));
    }

    @Test
    void roundTripsSignedLongAndDoubleEdges() {
        for (long value : new long[] {Long.MIN_VALUE, -1, 0, 1, Long.MAX_VALUE}) {
            assertEquals(value, CoriumCodec.decode(CoriumCodec.encode(value)));
        }
        for (double value : new double[] {-Double.MAX_VALUE, -0.5, -0.0, 0.0, 0.5, Double.MAX_VALUE}) {
            double decoded = (Double) CoriumCodec.decode(CoriumCodec.encode(value));
            assertEquals(Double.doubleToRawLongBits(value), Double.doubleToRawLongBits(decoded));
        }
    }

    @Test
    void rejectsMalformedPayloads() {
        assertThrows(DecodeException.class, () -> CoriumCodec.decode(new byte[0]));
        assertThrows(DecodeException.class, () -> CoriumCodec.decode(new byte[] {0, 0}));
        assertThrows(DecodeException.class, () -> CoriumCodec.decode(new byte[] {0x71, 0x01}));
        assertThrows(DecodeException.class,
                () -> CoriumCodec.decode(new byte[] {0x71, 0, 1, (byte) 0xff}));
    }

    @Test
    void permitsNilInsideForms() {
        Object decoded = CoriumCodec.decode(CoriumCodec.encode(
                new TxBuilder().add("ada", "person/email", null)));
        assertEquals(null, ((List<?>) ((List<?>) decoded).get(0)).get(3));
    }

    @Test
    void decodesDatomWireValues() {
        byte[] encoded = {
                0x01, // count
                0x01, 0x02, 0x03, 0x01, // e, a, tx, added
                0x71, 0x00, 0x03, 'A', 'd', 'a', // string value
        };
        List<Datom> datoms = CoriumCodec.decodeDatoms(encoded);
        assertEquals(1, datoms.size());
        assertEquals(EntityId.of(1), datoms.get(0).entity());
        assertEquals(EntityId.of(2), datoms.get(0).attribute());
        assertEquals(EntityId.of(3), datoms.get(0).transaction());
        assertEquals("Ada", datoms.get(0).value());
        assertTrue(datoms.get(0).added());
    }
}
