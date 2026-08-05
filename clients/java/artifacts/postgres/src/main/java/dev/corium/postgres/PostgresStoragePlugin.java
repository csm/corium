package dev.corium.postgres;

import dev.corium.client.NativeLibraries;
import dev.corium.client.StoragePlugin;

import java.nio.file.Path;

/** Provides the official Corium PostgreSQL direct-storage plugin. */
public final class PostgresStoragePlugin implements StoragePlugin {
    @Override
    public String backend() {
        return "postgres";
    }

    @Override
    public Path libraryPath() {
        return NativeLibraries.resolve(PostgresStoragePlugin.class, "corium_store_postgres");
    }
}
