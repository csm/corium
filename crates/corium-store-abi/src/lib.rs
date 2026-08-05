//! Stable Rust-to-Rust ABI for separately distributed Corium storage plugins.

#![allow(non_local_definitions)]

use abi_stable::StableAbi;
use abi_stable::library::RootModule;
use abi_stable::sabi_trait;
use abi_stable::sabi_types::VersionStrings;
use abi_stable::std_types::{RBox, ROption, RResult, RString, RVec};
use async_ffi::FfiFuture;

/// Current Corium storage ABI generation.
pub const ABI_VERSION: u32 = 1;

/// An ABI-safe operation result.
pub type AbiResult<T> = RResult<T, RString>;

/// An ABI-safe asynchronous operation result.
pub type AbiFuture<T> = FfiFuture<AbiResult<T>>;

/// An owned ABI-safe store handle.
pub type StoreBox = Store_TO<RBox<()>>;

/// An owned ABI-safe listing cursor.
pub type ListCursorBox = ListCursor_TO<RBox<()>>;

/// An owned ABI-safe backend factory.
pub type BackendBox = Backend_TO<RBox<()>>;

/// An owned host log sink.
pub type LogSinkBox = LogSink_TO<RBox<()>>;

/// Structured plugin event severity.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, StableAbi)]
pub enum AbiLogLevel {
    /// Diagnostic detail.
    Debug = 0,
    /// Normal lifecycle information.
    Info = 1,
    /// Recoverable abnormal condition.
    Warn = 2,
    /// Operation failure.
    Error = 3,
}

/// Host-owned callback for plugin events.
#[sabi_trait]
pub trait LogSink: Send + Sync + 'static {
    /// Emits a message with optional structured JSON fields.
    fn event(&self, level: AbiLogLevel, message: RString, fields_json: RString);
}

/// Transaction-log placement declared across the plugin boundary.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, StableAbi)]
pub enum AbiLogPlacement {
    /// Process-local ephemeral logs.
    Memory = 0,
    /// Files adjacent to a filesystem store root.
    Filesystem = 1,
    /// Records in the backend root store.
    RootStore = 2,
}

/// Backend capabilities declared across the plugin boundary.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, StableAbi)]
pub struct AbiBackendCapabilities {
    /// Transaction-log placement.
    pub log_placement: AbiLogPlacement,
    /// Whether another process can open an existing store.
    pub direct_access: bool,
}

/// Cursor used to list blob identifiers without moving a Rust stream across
/// the dynamic-library boundary.
#[sabi_trait]
pub trait ListCursor: Send + Sync + 'static {
    /// Returns the next batch. An empty batch marks end of stream.
    fn next(&self, max_items: u32) -> AbiFuture<RVec<RString>>;
}

/// Blob and root operations exposed by a plugin-owned store handle.
#[sabi_trait]
pub trait Store: Send + Sync + 'static {
    /// Stores immutable bytes and returns their hexadecimal digest.
    fn put(&self, bytes: RVec<u8>) -> AbiFuture<RString>;
    /// Gets immutable bytes by hexadecimal digest.
    fn get(&self, id: RString) -> AbiFuture<ROption<RVec<u8>>>;
    /// Tests for an immutable blob.
    fn contains(&self, id: RString) -> AbiFuture<bool>;
    /// Deletes an immutable blob.
    fn delete(&self, id: RString) -> AbiFuture<()>;
    /// Opens a listing cursor.
    fn list_open(&self) -> AbiFuture<ListCursorBox>;
    /// Returns a blob modification time in Unix milliseconds.
    fn modified_at(&self, id: RString) -> AbiFuture<ROption<u64>>;
    /// Gets a named root.
    fn get_root(&self, name: RString) -> AbiFuture<ROption<RVec<u8>>>;
    /// Compares and swaps a named root.
    fn cas_root(&self, name: RString, expected: ROption<RVec<u8>>, new: RVec<u8>) -> AbiFuture<()>;
    /// Deletes a named root.
    fn delete_root(&self, name: RString) -> AbiFuture<()>;
    /// Lists root names with a prefix.
    fn list_roots(&self, prefix: RString) -> AbiFuture<RVec<RString>>;
}

/// Factory for one backend kind served by a plugin.
#[sabi_trait]
pub trait Backend: Send + Sync + 'static {
    /// Stable backend kind.
    fn kind(&self) -> RString;
    /// Backend behavior needed by the engine.
    fn capabilities(&self) -> AbiBackendCapabilities;
    /// Opens or initializes a store from a JSON object.
    fn open(&self, config_json: RString, log: LogSinkBox) -> AbiFuture<StoreBox>;
    /// Opens an existing store without initializing it.
    fn open_existing(&self, config_json: RString, log: LogSinkBox) -> AbiFuture<StoreBox>;
}

/// Root module exported by every Corium storage plugin.
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = StorePluginModuleRef, prefix_fields = StorePluginModulePrefix)))]
pub struct StorePluginModule {
    /// Explicit Corium ABI generation in addition to structural layout checks.
    pub abi_version: u32,
    /// Plugin package/version for diagnostics.
    pub plugin_version: extern "C" fn() -> RString,
    /// Returns every backend factory served by this library.
    #[sabi(last_prefix_field)]
    pub backends: extern "C" fn() -> RVec<BackendBox>,
}

impl RootModule for StorePluginModuleRef {
    abi_stable::declare_root_module_statics! {StorePluginModuleRef}

    const BASE_NAME: &'static str = "corium_store_plugin";
    const NAME: &'static str = "corium storage plugin";
    const VERSION_STRINGS: VersionStrings = abi_stable::package_version_strings!();
}
