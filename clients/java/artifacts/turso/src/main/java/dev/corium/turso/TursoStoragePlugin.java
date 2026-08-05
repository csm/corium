package dev.corium.turso;

import dev.corium.client.NativeLibraries;
import dev.corium.client.StoragePlugin;

import java.nio.file.Path;

/** Provides the official Corium Turso direct-storage plugin. */
public final class TursoStoragePlugin implements StoragePlugin {
    @Override
    public String backend() {
        return "turso";
    }

    @Override
    public Path libraryPath() {
        return NativeLibraries.resolve(TursoStoragePlugin.class, "corium_store_turso");
    }
}
