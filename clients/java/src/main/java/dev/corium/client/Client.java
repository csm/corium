package dev.corium.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Client<T extends Peer> {
    T peer(String dbName);
    CompletableFuture<List<String>> listDatabases();
}
