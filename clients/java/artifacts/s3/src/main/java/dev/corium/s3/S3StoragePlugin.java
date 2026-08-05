package dev.corium.s3;

import dev.corium.client.NativeLibraries;
import dev.corium.client.StoragePlugin;

import java.nio.file.Path;

/** Provides the official Corium S3 direct-storage plugin. */
public final class S3StoragePlugin implements StoragePlugin {
    @Override
    public String backend() {
        return "s3";
    }

    @Override
    public Path libraryPath() {
        return NativeLibraries.resolve(S3StoragePlugin.class, "corium_store_s3");
    }
}
