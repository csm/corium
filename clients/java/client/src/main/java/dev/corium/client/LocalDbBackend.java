package dev.corium.client;

import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Serves database values from the in-process Corium engine. */
final class LocalDbBackend implements DbBackend {
    private final long handle;
    private final String databaseName;

    LocalDbBackend(long handle, String databaseName) {
        this.handle = handle;
        this.databaseName = databaseName;
        NativeHandles.releaseDbWith(this, handle);
    }

    /** Wraps one native database handle as an immutable database value. */
    static Db value(long handle, String databaseName) {
        return new Db(new LocalDbBackend(handle, databaseName), DbView.current(databaseName));
    }

    @Override
    public String databaseName() {
        return databaseName;
    }

    @Override
    public CompletableFuture<QueryResult> query(
            List<DbView> views, Object query, List<?> arguments, long fuel) {
        if (views.size() != 1) {
            throw new UnsupportedOperationException(
                    "an in-process peer cannot join across databases");
        }
        DbView view = views.get(0);
        byte[][] encoded = encodeAll(arguments);
        CompletableFuture<Object[]> future = new CompletableFuture<>();
        Native.dbQuery(handle, view.kind(), view.value(), CoriumCodec.encode(query), encoded,
                fuel, future);
        Reference.reachabilityFence(this);
        return future.thenApply(LocalDbBackend::queryResult);
    }

    @Override
    public CompletableFuture<Object> pull(DbView view, Object pattern, Object entity) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        Native.dbPull(handle, view.kind(), view.value(), CoriumCodec.encode(pattern),
                CoriumCodec.encode(entity), future);
        Reference.reachabilityFence(this);
        return future.thenApply(CoriumCodec::decode);
    }

    @Override
    public CompletableFuture<List<Datom>> datoms(
            DbView view, Index index, List<?> components, long limit) {
        byte[][] encoded = encodeAll(components);
        CompletableFuture<Object[]> future = new CompletableFuture<>();
        Native.dbDatoms(handle, view.kind(), view.value(), index.wireName(), encoded, limit,
                future);
        Reference.reachabilityFence(this);
        return future.thenApply(LocalDbBackend::datoms);
    }

    @Override
    public CompletableFuture<DbStats> stats(DbView view) {
        CompletableFuture<long[]> future = new CompletableFuture<>();
        Native.dbStats(handle, view.kind(), view.value(), future);
        Reference.reachabilityFence(this);
        return future.thenApply(stats -> new DbStats(stats[0], stats[1], stats[2], stats[3]));
    }

    private static byte[][] encodeAll(List<?> values) {
        byte[][] encoded = new byte[values.size()][];
        for (int index = 0; index < encoded.length; index++) {
            encoded[index] = CoriumCodec.encode(values.get(index));
        }
        return encoded;
    }

    private static QueryResult queryResult(Object[] result) {
        return new QueryResult(shape((String) result[0]), CoriumCodec.decode((byte[]) result[1]));
    }

    private static QueryResult.Shape shape(String name) {
        switch (name) {
            case "relation": return QueryResult.Shape.RELATION;
            case "collection": return QueryResult.Shape.COLLECTION;
            case "tuple": return QueryResult.Shape.TUPLE;
            case "scalar": return QueryResult.Shape.SCALAR;
            default: throw new DecodeException("query result has unspecified shape");
        }
    }

    private static List<Datom> datoms(Object[] result) {
        long[] scalars = (long[]) result[0];
        byte[][] values = (byte[][]) result[1];
        List<Datom> datoms = new ArrayList<>(values.length);
        for (int index = 0; index < values.length; index++) {
            int base = index * 4;
            datoms.add(new Datom(
                    EntityId.fromRaw(scalars[base]),
                    EntityId.fromRaw(scalars[base + 1]),
                    CoriumCodec.decode(values[index]),
                    EntityId.fromRaw(scalars[base + 2]),
                    scalars[base + 3] != 0));
        }
        return java.util.Collections.unmodifiableList(datoms);
    }
}
