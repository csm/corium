package io.corium.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** An immutable builder for Corium Pull patterns. */
public final class Pull implements EdnForm {
    private final List<Object> items;

    public Pull() {
        this(List.of());
    }

    private Pull(List<Object> items) {
        this.items = List.copyOf(items);
    }

    public Pull wildcard() { return append(new Symbol("*")); }
    public Pull dbId() { return append(new Keyword("db/id")); }
    public Pull attr(String attribute) { return append(new Keyword(attribute)); }

    public Pull reverse(String attribute) {
        String name = new Keyword(attribute).name();
        int slash = name.lastIndexOf('/');
        return attr(slash < 0 ? "_" + name : name.substring(0, slash + 1) + "_" + name.substring(slash + 1));
    }

    public Pull nested(String attribute, Pull pattern) {
        Map<Object, Object> item = new LinkedHashMap<>();
        item.put(new Keyword(attribute), pattern.toEdn());
        return append(Collections.unmodifiableMap(item));
    }

    public Pull recurse(String attribute, long depth) {
        if (depth < 0) throw new IllegalArgumentException("recursion depth must be non-negative");
        Map<Object, Object> item = new LinkedHashMap<>();
        item.put(new Keyword(attribute), depth);
        return append(Collections.unmodifiableMap(item));
    }

    public Pull recurseUnbounded(String attribute) {
        Map<Object, Object> item = new LinkedHashMap<>();
        item.put(new Keyword(attribute), new Symbol("..."));
        return append(Collections.unmodifiableMap(item));
    }

    private Pull append(Object item) {
        List<Object> next = new ArrayList<>(items);
        next.add(item);
        return new Pull(next);
    }

    @Override
    public Object toEdn() {
        return items;
    }
}
