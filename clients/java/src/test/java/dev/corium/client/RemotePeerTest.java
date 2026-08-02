package dev.corium.client;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

final class RemotePeerTest {
    @Test
    void rejectsTokenOnPlaintextByDefault() {
        assertThrows(IllegalArgumentException.class, () -> RemoteClient.builder(
                "http://127.0.0.1:4336").token("secret").build());
    }

    @Test
    void requiresAnOriginEndpoint() {
        assertThrows(IllegalArgumentException.class,
                () -> RemoteClient.builder("127.0.0.1:4336").build());
        assertThrows(IllegalArgumentException.class,
                () -> RemoteClient.builder("http://127.0.0.1:4336/path").build());
    }
}
