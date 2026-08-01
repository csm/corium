package io.corium.client;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

final class RemotePeerTest {
    @Test
    void rejectsTokenOnPlaintextByDefault() {
        assertThrows(IllegalArgumentException.class, () -> RemotePeer.builder(
                "http://127.0.0.1:4336", "people").token("secret").build());
    }

    @Test
    void requiresAnOriginEndpoint() {
        assertThrows(IllegalArgumentException.class,
                () -> RemotePeer.builder("127.0.0.1:4336", "people").build());
        assertThrows(IllegalArgumentException.class,
                () -> RemotePeer.builder("http://127.0.0.1:4336/path", "people").build());
    }
}
