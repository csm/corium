package io.corium.client;

import java.util.Objects;

/** An EDN symbol. */
public final class Symbol {
    private final String name;

    public Symbol(String name) {
        this.name = Objects.requireNonNull(name, "name");
        if (name.isEmpty() || name.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("symbol must be non-empty and contain no whitespace");
        }
    }

    public static Symbol of(String name) {
        return new Symbol(name);
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Symbol && name.equals(((Symbol) other).name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return name;
    }
}
