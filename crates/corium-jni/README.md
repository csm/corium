# corium-jni

`corium-jni` is the JVM adapter for the Corium Java client. It is to
`clients/java` what `corium-python` is to `clients/python`: a thin layer over
`corium-ffi` that owns the JVM boundary and nothing else. The public API stays
in Java.

Composite values cross the boundary as bytes that `dev.corium.client.CoriumCodec`
has already encoded, so the Java value mapping is written once and serves both
the in-process and the remote peer. Only entity ids, statistics, result shapes,
and tempids cross as primitives or arrays.

Every asynchronous method takes a `java.util.concurrent.CompletableFuture` and
returns immediately. The facade future is driven on one shared multi-threaded
Tokio runtime, and the completion attaches to the JVM only for the duration of
the `complete` or `completeExceptionally` call: an engine thread that stayed
attached would count as a live non-daemon thread and keep the JVM from exiting.

Failures become `dev.corium.client.CoriumException` with the `ErrorKind` that
`corium-ffi` categorized, so both Java peers report failures identically.

Peer and database handles are opaque `jlong` pointers. Java owns their
lifetime: `close()` releases the live peer deterministically, and a
`java.lang.ref.Cleaner` frees the handle itself once its Java owner is
unreachable. Every native call the Java layer makes ends in a
`Reference.reachabilityFence`, so a handle cannot be freed while a call still
holds it.

The `postgres`, `turso`, and `s3` features statically link those drivers for
platforms without `dlopen`. Published Java artifacts leave them off and load
the same dynamic storage plugins the Python client uses.
