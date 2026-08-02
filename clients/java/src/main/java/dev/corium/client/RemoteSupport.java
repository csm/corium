package dev.corium.client;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

class RemoteSupport {
    interface Decoder<I, O> { O decode(I input); }

    static <I, O> StreamObserver<I> unary(CompletableFuture<O> future, Decoder<I, O> decoder) {
        return new StreamObserver<>() {
            private boolean received;

            @Override
            public void onNext(I value) {
                if (received) {
                    future.completeExceptionally(new DecodeException("unary RPC returned multiple responses"));
                    return;
                }
                received = true;
                try {
                    future.complete(decoder.decode(value));
                } catch (Throwable error) {
                    future.completeExceptionally(error);
                }
            }

            @Override
            public void onError(Throwable error) {
                future.completeExceptionally(mapError(error));
            }

            @Override
            public void onCompleted() {
                if (!received && !future.isDone())
                    future.completeExceptionally(new DecodeException("unary RPC returned no response"));
            }
        };
    }

    static CoriumException mapError(Throwable error) {
        if (!(error instanceof StatusRuntimeException) && !(error instanceof StatusException)) {
            return new CoriumException(error.getMessage(), null, error);
        }
        Status status = Status.fromThrowable(error);
        String message = status.getDescription() == null ? status.getCode().name() : status.getDescription();
        return new CoriumException(message, status.getCode().name(), error);
    }

    static URI parseEndpoint(String value) {
        try {
            URI uri = new URI(Objects.requireNonNull(value, "endpoint"));
            if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                    || uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null
                    || uri.getFragment() != null
                    || (uri.getPath() != null && !uri.getPath().isEmpty() && !"/".equals(uri.getPath()))) {
                throw new IllegalArgumentException("endpoint must be an http:// or https:// origin");
            }
            return uri;
        } catch (URISyntaxException error) {
            throw new IllegalArgumentException("invalid endpoint", error);
        }
    }
}
