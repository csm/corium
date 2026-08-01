package dev.corium.client;

import corium.v1.Corium;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** An immutable database view backed by a remote Corium peer server. */
public final class Db {
    private enum View { CURRENT, AS_OF, SINCE, HISTORY, AS_OF_INSTANT, SINCE_INSTANT }

    private final RemotePeer peer;
    private final View view;
    private final long value;

    Db(RemotePeer peer) {
        this(peer, View.CURRENT, 0);
    }

    private Db(RemotePeer peer, View view, long value) {
        this.peer = Objects.requireNonNull(peer, "peer");
        this.view = view;
        this.value = value;
    }

    public String databaseName() { return peer.databaseName(); }

    public Db asOf(long t) { return new Db(peer, View.AS_OF, nonnegative(t, "t")); }
    public Db since(long t) { return new Db(peer, View.SINCE, nonnegative(t, "t")); }
    public Db history() { peer.ensureOpen(); return new Db(peer, View.HISTORY, 0); }
    public Db asOf(Instant instant) {
        return new Db(peer, View.AS_OF_INSTANT, Objects.requireNonNull(instant, "instant").toEpochMilli());
    }
    public Db since(Instant instant) {
        return new Db(peer, View.SINCE_INSTANT, Objects.requireNonNull(instant, "instant").toEpochMilli());
    }

    public CompletableFuture<QueryResult> query(Object query, Object... arguments) {
        return query(query, Arrays.asList(arguments), 0);
    }

    /** Executes a query. Fuel {@code 0} requests the server default. */
    public CompletableFuture<QueryResult> query(Object query, List<?> arguments, long fuel) {
        return peer.query(List.of(viewSpec()), query, arguments, nonnegative(fuel, "fuel"));
    }

    /** Executes a query with this view as {@code $} and additional views in positional order. */
    public CompletableFuture<QueryResult> query(
            Object query, List<Db> additionalDatabases, List<?> arguments, long fuel) {
        List<Corium.DbViewSpec> views = new java.util.ArrayList<>();
        views.add(viewSpec());
        additionalDatabases.stream().map(Db::viewSpec).forEach(views::add);
        return peer.query(views, query, arguments, nonnegative(fuel, "fuel"));
    }

    public CompletableFuture<Object> pull(Object pattern, Object entity) {
        return peer.pull(viewSpec(), pattern, entity);
    }

    /** Scans an index. Limit {@code 0} requests an unbounded scan. */
    public CompletableFuture<List<Datom>> datoms(Index index, List<?> components, long limit) {
        return peer.datoms(viewSpec(), Objects.requireNonNull(index, "index"), components,
                nonnegative(limit, "limit"));
    }

    public CompletableFuture<List<Datom>> datoms(Index index, Object... components) {
        return datoms(index, Arrays.asList(components), 0);
    }

    public CompletableFuture<DbStats> stats() {
        return peer.stats(viewSpec());
    }

    public CompletableFuture<Long> basisT() {
        return stats().thenApply(DbStats::basisT);
    }

    Corium.DbViewSpec viewSpec() {
        peer.ensureOpen();
        Corium.DbViewSpec.Builder builder = Corium.DbViewSpec.newBuilder().setDb(databaseName());
        switch (view) {
            case AS_OF: builder.setAsOf(value); break;
            case SINCE: builder.setSince(value); break;
            case HISTORY: builder.setHistory(true); break;
            case AS_OF_INSTANT: builder.setAsOfInstant(value); break;
            case SINCE_INSTANT: builder.setSinceInstant(value); break;
            default: break;
        }
        return builder.build();
    }

    private static long nonnegative(long value, String name) {
        if (value < 0) throw new IllegalArgumentException(name + " must be non-negative");
        return value;
    }
}
