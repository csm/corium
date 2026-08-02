package dev.corium.client;

import java.util.concurrent.CompletableFuture;

public interface Peer {
    Db db();
    CompletableFuture<Db> sync();
    CompletableFuture<TxReport> transact(Object transactionData);
}
