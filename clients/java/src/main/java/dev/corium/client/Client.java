package dev.corium.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Client {
    CompletableFuture<List<String>> listDatabases();
}
