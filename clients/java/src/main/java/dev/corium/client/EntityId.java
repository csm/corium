package dev.corium.client;

/** An unsigned 64-bit Corium entity reference. */
public final class EntityId {
    private final long raw;

    private EntityId(long raw) {
        this.raw = raw;
    }

    public static EntityId of(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("entity id must be non-negative");
        }
        return new EntityId(value);
    }

    public static EntityId fromUnsignedString(String value) {
        return new EntityId(Long.parseUnsignedLong(value));
    }

    static EntityId fromRaw(long raw) {
        return new EntityId(raw);
    }

    long raw() {
        return raw;
    }

    public String toUnsignedString() {
        return Long.toUnsignedString(raw);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof EntityId && raw == ((EntityId) other).raw;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(raw);
    }

    @Override
    public String toString() {
        return "#eid " + toUnsignedString();
    }
}
