package dev.corium.client;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** An immutable database value served by a local or remote Corium peer. */
public final class Db {
    private final DbBackend backend;
    private final DbView view;

    Db(DbBackend backend, DbView view) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.view = Objects.requireNonNull(view, "view");
    }

    public String databaseName() { return view.databaseName(); }

    public Db asOf(long t) { return derive(DbView.AS_OF, nonnegative(t, "t")); }
    public Db since(long t) { return derive(DbView.SINCE, nonnegative(t, "t")); }
    public Db history() { return derive(DbView.HISTORY, 0); }
    public Db asOf(Instant instant) {
        return derive(DbView.AS_OF_INSTANT, Objects.requireNonNull(instant, "instant").toEpochMilli());
    }
    public Db since(Instant instant) {
        return derive(DbView.SINCE_INSTANT, Objects.requireNonNull(instant, "instant").toEpochMilli());
    }

    public CompletableFuture<QueryResult> query(Object query, Object... arguments) {
        return query(query, Arrays.asList(arguments), 0);
    }

    /** Executes a query. Fuel {@code 0} requests the peer default. */
    public CompletableFuture<QueryResult> query(Object query, List<?> arguments, long fuel) {
        return backend.query(List.of(view), query, arguments, nonnegative(fuel, "fuel"));
    }

    /**
     * Executes a query with this view as {@code $} and additional views in
     * positional order. Only a remote peer can join across databases.
     */
    public CompletableFuture<QueryResult> query(
            Object query, List<Db> additionalDatabases, List<?> arguments, long fuel) {
        List<DbView> views = new ArrayList<>();
        views.add(view);
        additionalDatabases.forEach(database -> views.add(database.view));
        return backend.query(views, query, arguments, nonnegative(fuel, "fuel"));
    }

    public CompletableFuture<Object> pull(Object pattern, Object entity) {
        return backend.pull(view, pattern, entity);
    }

    /** Scans an index. Limit {@code 0} requests an unbounded scan. */
    public CompletableFuture<List<Datom>> datoms(Index index, List<?> components, long limit) {
        return backend.datoms(view, Objects.requireNonNull(index, "index"), components,
                nonnegative(limit, "limit"));
    }

    public CompletableFuture<List<Datom>> datoms(Index index, Object... components) {
        return datoms(index, Arrays.asList(components), 0);
    }

    public CompletableFuture<DbStats> stats() {
        return backend.stats(view);
    }

    public CompletableFuture<Long> basisT() {
        return stats().thenApply(DbStats::basisT);
    }

    private Db derive(int kind, long value) {
        return new Db(backend, view.derive(kind, value));
    }

    private static long nonnegative(long value, String name) {
        if (value < 0) throw new IllegalArgumentException(name + " must be non-negative");
        return value;
    }
}
