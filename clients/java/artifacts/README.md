# Direct-storage plugin artifacts

Corium publishes `corium-turso`, `corium-postgres`, and `corium-s3` beside
`corium-client`. Each is a small jar holding one `StoragePlugin` provider, plus
one platform-classified jar per supported platform holding that backend's
library. The engine itself ships the same way, as classified jars beside
`corium-client`, so a remote-only dependency stays pure Java.

Providers are discovered through `java.util.ServiceLoader` when the engine
loads. Each returns the path of its library, which the loader extracts from the
classpath to a cache directory and registers with the engine. The loader
rejects duplicate backend kinds and incompatible ABI layouts.

Build the engine and the three storage libraries:

```shell
cargo build --release \
  -p corium-jni \
  -p corium-store-turso \
  -p corium-store-postgres \
  -p corium-store-s3
```

Then build the jars for the current platform:

```shell
python3 clients/java/scripts/build_native_jars.py \
  --target-directory target/release \
  --classifier linux-x86_64 \
  --output dist
```

Use the classifier that matches the build host: `linux-x86_64`,
`linux-aarch64`, `macos-x86_64`, `macos-aarch64`, or `windows-x86_64`. The
release workflow builds all five and attaches them through the `native` Maven
profile.
