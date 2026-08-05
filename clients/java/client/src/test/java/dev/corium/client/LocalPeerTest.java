package dev.corium.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Validation a local peer performs before it loads the native engine. */
final class LocalPeerTest {
    @Test
    void requiresAtLeastOneEndpoint() {
        assertThrows(IllegalArgumentException.class,
                () -> LocalPeer.builder(List.of(), "people").connect());
    }

    @Test
    void rejectsMixedEndpointSchemes() {
        assertThrows(IllegalArgumentException.class, () -> LocalPeer
                .builder(List.of("https://one:4335", "http://two:4335"), "people")
                .connect());
    }

    @Test
    void rejectsBearerTokensOverPlaintext() {
        assertThrows(IllegalArgumentException.class, () -> LocalPeer
                .builder("http://127.0.0.1:4335", "people")
                .token("secret")
                .connect());
    }

    @Test
    void rejectsCustomTlsOverPlaintext() {
        assertThrows(IllegalArgumentException.class, () -> LocalPeer
                .builder("http://127.0.0.1:4335", "people")
                .tlsDomain("corium.internal")
                .connect());
    }

    @Test
    void rejectsAnEmptyDatabaseName() {
        assertThrows(IllegalArgumentException.class,
                () -> LocalPeer.builder("http://127.0.0.1:4335", ""));
    }

    @Test
    void boundsTheSegmentCache() {
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCache(Paths.get("cache"), 0));
        assertThrows(IllegalArgumentException.class,
                () -> new SegmentCache(Paths.get("cache"), 1024, 2048));
        SegmentCache cache = new SegmentCache(Paths.get("cache"), 4096);
        assertEquals(4096, cache.capacityBytes());
        assertEquals(0, cache.memoryCapacityBytes());
    }

    @Test
    void discoversStorageWithAndWithoutACache() {
        assertNull(DirectStorage.discover().cache());
        SegmentCache cache = new SegmentCache(Paths.get("cache"), 4096);
        assertEquals(cache, DirectStorage.discover(cache).cache());
    }

    @Test
    void namesNativeLibrariesForThisPlatform() {
        String classifier = NativeLibraries.classifier();
        assertTrue(classifier.matches("(linux|macos|windows)-.+"), classifier);
        String fileName = NativeLibraries.libraryFileName("corium_jni");
        assertTrue(fileName.equals("libcorium_jni.so")
                || fileName.equals("libcorium_jni.dylib")
                || fileName.equals("corium_jni.dll"), fileName);
    }
}
