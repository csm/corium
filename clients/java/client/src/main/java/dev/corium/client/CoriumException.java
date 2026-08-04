package dev.corium.client;

/** Base runtime exception for Corium client failures. */
public class CoriumException extends RuntimeException {
    private final String grpcCode;

    public CoriumException(String message) {
        this(message, null, null);
    }

    public CoriumException(String message, String grpcCode, Throwable cause) {
        super(message, cause);
        this.grpcCode = grpcCode;
    }

    public String grpcCode() {
        return grpcCode;
    }
}
