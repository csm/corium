package dev.corium.client;

/** A Corium composite value could not be decoded losslessly. */
public final class DecodeException extends CoriumException {
    public DecodeException(String message) {
        super(message, ErrorKind.DECODE, null, null);
    }

    public DecodeException(String message, Throwable cause) {
        super(message, ErrorKind.DECODE, null, cause);
    }
}
