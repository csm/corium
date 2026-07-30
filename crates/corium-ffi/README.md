# corium-ffi

`corium-ffi` is the runtime-neutral native-binding facade for Corium clients.
It wraps local and remote `corium-client` peers behind the same opaque handle
types and exchanges owned values using Corium's composite protocol encoding.

The facade deliberately contains no Python, JVM, or C ABI code. Language
adapters are responsible for runtime value conversion, future integration,
panic containment, and mapping the stable `ErrorKind` values to native
exceptions.

Calling `PeerHandle::close` is idempotent. It rejects new work and invalidates
all database handles derived from that peer so connections do not remain alive
until language-runtime garbage collection.

`ClientTlsOptions` enables platform roots by default and can add a PEM
certificate authority or override the certificate DNS name. Certificate bytes
remain owned by the facade and connect-option debug output reveals only whether
TLS is enabled.

`LocalConnectOptions::direct_storage` discovers the transactor's read-only
storage connection and optionally places a bounded segment cache in front of
it. Filesystem support is built in. The `postgres`, `turso`, and `s3` features
are independent so a base/remote artifact does not pull their driver graphs.
Filesystem and Turso connections require the advertised local path to exist;
the facade never creates a missing store during discovery.
The S3 feature refreshes temporary credentials through `GetStorageInfo` and
pins refreshes to the originally discovered bucket, prefix, region, and
endpoint.
