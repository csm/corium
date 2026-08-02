package dev.corium.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** An EDN list, distinct from a Java List (which represents an EDN vector). */
public final class EdnList {
    private final List<Object> items;

    public EdnList(List<?> items) {
        this.items = Collections.unmodifiableList(new ArrayList<>(items));
    }

    public static EdnList of(Object... items) {
        return new EdnList(Arrays.asList(items));
    }

    public List<Object> items() {
        return items;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof EdnList && items.equals(((EdnList) other).items);
    }

    @Override
    public int hashCode() {
        return items.hashCode();
    }

    @Override
    public String toString() {
        return "(" + items + ")";
    }
}
