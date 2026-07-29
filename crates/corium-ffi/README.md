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
