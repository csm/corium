package dev.corium.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.corium.client.StoragePlugin;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/** The engine only finds this plugin through service registration. */
final class S3StoragePluginTest {
    @Test
    void registersItselfForServiceDiscovery() {
        boolean registered = false;
        for (StoragePlugin plugin : ServiceLoader.load(StoragePlugin.class)) {
            if (plugin instanceof S3StoragePlugin) {
                assertEquals("s3", plugin.backend());
                registered = true;
            }
        }
        assertTrue(registered, "the S3 storage plugin is not registered");
    }
}
