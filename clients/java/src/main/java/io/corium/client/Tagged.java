package io.corium.client;

import java.util.Objects;
import java.util.Set;

/** A custom EDN tagged value. */
public final class Tagged {
    private static final Set<String> RESERVED = Set.of("bytes", "eid", "inst", "uuid");
    private final String tag;
    private final Object value;

    public Tagged(String tag, Object value) {
        this.tag = Objects.requireNonNull(tag, "tag");
        if (tag.isEmpty() || tag.startsWith(":") || tag.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("tag must be non-empty and contain no ':' prefix or whitespace");
        }
        if (RESERVED.contains(tag)) {
            throw new IllegalArgumentException("tag is reserved for a dedicated Corium boundary type: " + tag);
        }
        this.value = value;
    }

    public String tag() {
        return tag;
    }

    public Object value() {
        return value;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Tagged)) return false;
        Tagged that = (Tagged) other;
        return tag.equals(that.tag) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, value);
    }
}
