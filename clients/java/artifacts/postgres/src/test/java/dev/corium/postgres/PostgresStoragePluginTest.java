package dev.corium.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.corium.client.StoragePlugin;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;

/** The engine only finds this plugin through service registration. */
final class PostgresStoragePluginTest {
    @Test
    void registersItselfForServiceDiscovery() {
        boolean registered = false;
        for (StoragePlugin plugin : ServiceLoader.load(StoragePlugin.class)) {
            if (plugin instanceof PostgresStoragePlugin) {
                assertEquals("postgres", plugin.backend());
                registered = true;
            }
        }
        assertTrue(registered, "the PostgreSQL storage plugin is not registered");
    }
}
