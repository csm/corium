package dev.corium.client;

/** Base runtime exception for Corium client failures. */
public class CoriumException extends RuntimeException {
    private final ErrorKind kind;
    private final String grpcCode;

    public CoriumException(String message) {
        this(message, ErrorKind.PROTOCOL, null, null);
    }

    public CoriumException(String message, String grpcCode, Throwable cause) {
        this(message, ErrorKind.fromGrpcCode(grpcCode), grpcCode, cause);
    }

    public CoriumException(String message, ErrorKind kind, String grpcCode, Throwable cause) {
        super(message, cause);
        this.kind = kind == null ? ErrorKind.PROTOCOL : kind;
        this.grpcCode = grpcCode;
    }

    /** The stable failure category, shared by the local and remote peers. */
    public ErrorKind kind() {
        return kind;
    }

    /** The gRPC status name, when the failure came from an RPC. */
    public String grpcCode() {
        return grpcCode;
    }
}
