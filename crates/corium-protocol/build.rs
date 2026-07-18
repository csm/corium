//! Compiles the protocol definition with `protox` (pure Rust; no `protoc`).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/corium.proto");
    let descriptors = protox::compile(["proto/corium.proto"], ["proto"])?;
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_fds(descriptors)?;
    Ok(())
}
