package dev.corium.client;

import java.util.Objects;

/** One datom returned by a covering-index scan. */
public final class Datom {
    private final EntityId entity;
    private final EntityId attribute;
    private final Object value;
    private final EntityId transaction;
    private final boolean added;

    public Datom(EntityId entity, EntityId attribute, Object value, EntityId transaction, boolean added) {
        this.entity = Objects.requireNonNull(entity, "entity");
        this.attribute = Objects.requireNonNull(attribute, "attribute");
        this.value = value;
        this.transaction = Objects.requireNonNull(transaction, "transaction");
        this.added = added;
    }

    public EntityId entity() { return entity; }
    public EntityId attribute() { return attribute; }
    public Object value() { return value; }
    public EntityId transaction() { return transaction; }
    public boolean added() { return added; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Datom)) return false;
        Datom that = (Datom) other;
        return entity.equals(that.entity) && attribute.equals(that.attribute)
                && Objects.deepEquals(value, that.value)
                && transaction.equals(that.transaction) && added == that.added;
    }

    @Override
    public int hashCode() {
        int valueHash = value instanceof byte[]
                ? java.util.Arrays.hashCode((byte[]) value) : Objects.hashCode(value);
        return Objects.hash(entity, attribute, valueHash, transaction, added);
    }
}
