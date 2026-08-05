package dev.corium.client;

import java.util.concurrent.CompletableFuture;

/**
 * A connection to one Corium database.
 *
 * <p>{@link RemotePeer} talks to a {@code corium peer-server} over gRPC, and
 * {@link LocalPeer} runs a full peer in this process through the native Corium
 * engine. Both produce the same immutable {@link Db} values.
 */
public interface Peer extends AutoCloseable {
    /** The database this peer is connected to. */
    String databaseName();

    /** Returns the current immutable database value. */
    CompletableFuture<Db> db();

    /** Synchronizes with the transactor and returns the resulting database. */
    CompletableFuture<Db> sync();

    /** Submits raw forms or typed transaction data. */
    CompletableFuture<TxReport> transact(Object transactionData);

    /** Idempotently releases this peer; derived {@link Db} values stop working. */
    @Override
    void close();
}
