package dev.corium.client;

import java.util.Objects;

/** A query value together with its declared result shape. */
public final class QueryResult {
    public enum Shape { RELATION, COLLECTION, TUPLE, SCALAR }

    private final Shape shape;
    private final Object value;

    public QueryResult(Shape shape, Object value) {
        this.shape = Objects.requireNonNull(shape, "shape");
        this.value = value;
    }

    public Shape shape() { return shape; }
    public Object value() { return value; }
}
