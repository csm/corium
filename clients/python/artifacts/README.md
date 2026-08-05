# Direct-storage plugin packages

Corium publishes `corium-turso`, `corium-postgres`, and `corium-s3`. Each wheel
contains one storage library and one Python path provider. The base `corium`
wheel contains the engine and filesystem storage.

Each provider uses the `corium.store_plugins` entry-point group. The Python
loader calls the provider and loads the returned library path.

Build the three storage libraries:

```shell
cargo build --release \
  -p corium-store-turso \
  -p corium-store-postgres \
  -p corium-store-s3
```

Then build the wheels for the current platform:

```shell
python clients/python/scripts/build_plugin_wheels.py \
  --target-directory target/release \
  --platform-tag macosx_11_0_arm64 \
  --output dist
```

Use the correct wheel platform tag for the build host. The release workflow
builds all supported Linux, macOS, and Windows wheels. It applies the
`manylinux_2_28` policy to Linux wheels.

The loader rejects duplicate backend kinds and incompatible ABI layouts.
