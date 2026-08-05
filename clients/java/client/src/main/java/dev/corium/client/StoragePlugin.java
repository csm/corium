package dev.corium.client;

import java.nio.file.Path;

/**
 * One direct-storage plugin available to an in-process peer.
 *
 * <p>Implementations are discovered with {@link java.util.ServiceLoader} when
 * the native engine loads, and each returns the path of a plugin library the
 * engine registers. The official {@code corium-turso}, {@code corium-postgres},
 * and {@code corium-s3} artifacts each provide one.
 */
public interface StoragePlugin {
    /** The storage backend kind this plugin serves, such as {@code "turso"}. */
    String backend();

    /** The on-disk path of the plugin library. */
    Path libraryPath();
}
