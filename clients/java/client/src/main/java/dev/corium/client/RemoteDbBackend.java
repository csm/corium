package dev.corium.client;

import dev.corium.protocol.v1.Corium;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Serves database values from a {@code corium peer-server} over gRPC. */
final class RemoteDbBackend implements DbBackend {
    private final RemotePeer peer;

    RemoteDbBackend(RemotePeer peer) {
        this.peer = peer;
    }

    @Override
    public String databaseName() {
        return peer.databaseName();
    }

    @Override
    public CompletableFuture<QueryResult> query(
            List<DbView> views, Object query, List<?> arguments, long fuel) {
        List<Corium.DbViewSpec> specs = new ArrayList<>(views.size());
        for (DbView view : views) {
            specs.add(spec(view));
        }
        return peer.query(specs, query, arguments, fuel);
    }

    @Override
    public CompletableFuture<Object> pull(DbView view, Object pattern, Object entity) {
        return peer.pull(spec(view), pattern, entity);
    }

    @Override
    public CompletableFuture<List<Datom>> datoms(
            DbView view, Index index, List<?> components, long limit) {
        return peer.datoms(spec(view), index, components, limit);
    }

    @Override
    public CompletableFuture<DbStats> stats(DbView view) {
        return peer.stats(spec(view));
    }

    private Corium.DbViewSpec spec(DbView view) {
        peer.ensureOpen();
        Corium.DbViewSpec.Builder builder =
                Corium.DbViewSpec.newBuilder().setDb(view.databaseName());
        switch (view.kind()) {
            case DbView.AS_OF: builder.setAsOf(view.value()); break;
            case DbView.SINCE: builder.setSince(view.value()); break;
            case DbView.HISTORY: builder.setHistory(true); break;
            case DbView.AS_OF_INSTANT: builder.setAsOfInstant(view.value()); break;
            case DbView.SINCE_INSTANT: builder.setSinceInstant(view.value()); break;
            default: break;
        }
        return builder.build();
    }
}
