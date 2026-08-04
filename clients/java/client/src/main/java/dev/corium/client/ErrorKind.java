package dev.corium.client;

/** The stable failure category carried by every {@link CoriumException}. */
public enum ErrorKind {
    /** A peer or server could not be reached. */
    CONNECTION,
    /** Credentials were missing or rejected. */
    AUTHENTICATION,
    /** The caller is authenticated but not authorized. */
    PERMISSION_DENIED,
    /** The peer violated or rejected the protocol contract. */
    PROTOCOL,
    /** Query parsing or execution failed. */
    QUERY,
    /** Transaction validation or commit failed. */
    TRANSACTION,
    /** A boundary value could not be decoded. */
    DECODE,
    /** Direct storage access failed. */
    STORAGE,
    /** Query execution consumed its fuel budget. */
    FUEL_EXHAUSTED,
    /** The peer was closed. */
    CLOSED,
    /**
     * The native engine or a storage plugin could not be loaded, or the engine
     * failed outside the categories above.
     */
    NATIVE;

    /** Maps a category name reported by the native adapter. */
    static ErrorKind fromNative(String name) {
        switch (name) {
            case "connection": return CONNECTION;
            case "authentication": return AUTHENTICATION;
            case "permission_denied": return PERMISSION_DENIED;
            case "query": return QUERY;
            case "transaction": return TRANSACTION;
            case "decode": return DECODE;
            case "storage": return STORAGE;
            case "fuel_exhausted": return FUEL_EXHAUSTED;
            case "closed": return CLOSED;
            case "native": return NATIVE;
            default: return PROTOCOL;
        }
    }

    /** Maps a gRPC status name the same way the native adapter maps its codes. */
    static ErrorKind fromGrpcCode(String code) {
        if (code == null) return PROTOCOL;
        switch (code) {
            case "UNAUTHENTICATED": return AUTHENTICATION;
            case "PERMISSION_DENIED": return PERMISSION_DENIED;
            case "UNAVAILABLE":
            case "DEADLINE_EXCEEDED": return CONNECTION;
            case "RESOURCE_EXHAUSTED": return FUEL_EXHAUSTED;
            case "INVALID_ARGUMENT": return QUERY;
            case "FAILED_PRECONDITION":
            case "ABORTED": return TRANSACTION;
            default: return PROTOCOL;
        }
    }
}
