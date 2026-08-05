package dev.corium.turso;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.corium.client.StoragePlugin;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/** The engine only finds this plugin through service registration. */
final class TursoStoragePluginTest {
    @Test
    void registersItselfForServiceDiscovery() {
        boolean registered = false;
        for (StoragePlugin plugin : ServiceLoader.load(StoragePlugin.class)) {
            if (plugin instanceof TursoStoragePlugin) {
                assertEquals("turso", plugin.backend());
                registered = true;
            }
        }
        assertTrue(registered, "the Turso storage plugin is not registered");
    }
}
