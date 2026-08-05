package dev.corium.client;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** An immutable map-form transaction entity. */
public final class EntityMap implements EdnForm {
    private final Map<Object, Object> values;

    public EntityMap() {
        this.values = Map.of();
    }

    private EntityMap(Map<Object, Object> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    public static EntityMap withId(Object entity) {
        return new EntityMap().set("db/id", entity);
    }

    public EntityMap set(String attribute, Object value) {
        Map<Object, Object> next = new LinkedHashMap<>(values);
        next.put(new Keyword(attribute), value);
        return new EntityMap(next);
    }

    @Override
    public Object toEdn() {
        return values;
    }
}
