package dev.corium.client;

/**
 * The native Corium engine, or one of its storage plugins, is missing or could
 * not be loaded. A remote peer never raises this.
 */
public final class NativeExtensionException extends CoriumException {
    public NativeExtensionException(String message) {
        super(message, ErrorKind.NATIVE, null, null);
    }

    public NativeExtensionException(String message, Throwable cause) {
        super(message, ErrorKind.NATIVE, null, cause);
    }
}
