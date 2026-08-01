package io.corium.client;

import java.util.Objects;

/** A Corium keyword, distinct from an ordinary Java string. */
public final class Keyword {
    private final String name;

    public Keyword(String name) {
        Objects.requireNonNull(name, "name");
        this.name = name.startsWith(":") ? name.substring(1) : name;
        if (this.name.isEmpty() || this.name.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("keyword must be non-empty and contain no whitespace");
        }
    }

    public static Keyword of(String name) {
        return new Keyword(name);
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Keyword && name.equals(((Keyword) other).name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return ":" + name;
    }
}
