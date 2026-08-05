package dev.corium.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The peer-specific half of a {@link Db}. Views stay values on the Java side,
 * so one {@code Db} implementation serves both the local and the remote peer.
 */
interface DbBackend {
    String databaseName();

    /**
     * Executes a query whose first view is {@code $}. A backend that cannot
     * join across databases rejects more than one view.
     */
    CompletableFuture<QueryResult> query(
            List<DbView> views, Object query, List<?> arguments, long fuel);

    CompletableFuture<Object> pull(DbView view, Object pattern, Object entity);

    CompletableFuture<List<Datom>> datoms(
            DbView view, Index index, List<?> components, long limit);

    CompletableFuture<DbStats> stats(DbView view);
}
