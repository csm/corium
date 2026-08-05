package dev.corium.client;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Loads the engine out of a published platform jar. The release workflow runs
 * this on every supported platform, with that platform's jar on the classpath,
 * so a jar the client cannot load fails before it is published.
 */
@EnabledIfSystemProperty(named = "corium.native.classifier", matches = ".+")
final class NativeArtifactTest {
    @Test
    void loadsTheEngineFromThePlatformJar() {
        String classifier = System.getProperty("corium.native.classifier");
        assertTrue(classifier.equals(NativeLibraries.classifier()),
                "jar classifier " + classifier + " does not match this platform "
                        + NativeLibraries.classifier());
        String resource = "dev/corium/native/" + classifier + "/"
                + NativeLibraries.libraryFileName("corium_jni");
        assertNotNull(NativeLibraries.class.getClassLoader().getResource(resource),
                "the platform jar is not on the test classpath: " + resource);

        assertTrue(LocalPeer.availableStorageBackends().contains("filesystem"),
                "the engine reported no filesystem storage backend");
    }
}
