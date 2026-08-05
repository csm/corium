package dev.corium.client;

/** A value that can lower itself to a Corium EDN boundary value. */
public interface EdnForm {
    Object toEdn();
}
