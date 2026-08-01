package io.corium.client;

import java.util.List;
import java.util.Arrays;

/** A unique-attribute lookup reference. */
public final class LookupRef implements EdnForm {
    private final Keyword attribute;
    private final Object value;

    public LookupRef(String attribute, Object value) {
        this.attribute = new Keyword(attribute);
        this.value = value;
    }

    @Override
    public Object toEdn() {
        return Arrays.asList(attribute, value);
    }
}
