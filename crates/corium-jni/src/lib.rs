//! JNI adapter for the Corium Java client.
//!
//! The public Java API lives in `clients/java`. This crate keeps the JVM out of
//! Corium's core crates: it converts owned boundary values, drives
//! [`corium_ffi`] futures on one shared Tokio runtime, and settles the
//! `CompletableFuture` instances the Java layer hands it.
//!
//! Composite values cross the boundary as already-encoded bytes, so the Java
//! codec in `dev.corium.client.CoriumCodec` remains the only Java-side value
//! mapping for both the local and the remote peer.
//!
//! # Panics never cross the boundary
//!
//! Every native entry point runs its work inside [`EnvUnowned::with_env`],
//! which catches unwinding and lets the error policy throw a Java exception, so
//! a panic on the calling thread — including one from [`runtime`] failing to
//! start — surfaces as a Java exception instead of aborting the JVM.
//!
//! Work that runs after an entry point returns is contained the same way, for
//! the same reason: a `CompletableFuture` that never settles hangs its caller
//! forever. Engine panics come back through the task's join handle, and
//! [`settle`] both guards the delivery itself and falls back to a plain
//! exception when JNI refuses the categorized one.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::path::PathBuf;
use std::sync::OnceLock;

use corium_ffi::{
    ClientTlsOptions, CompositeValue, Datom, DbHandle, DbStats, DirectStorageOptions, ErrorKind,
    FfiError, Index, LocalConnectOptions, PeerHandle, QueryOutput, ResultShape,
    SegmentCacheOptions, TxReport, available_storage_backends, discover_storage_backend,
    load_storage_plugin,
};
use jni::errors::{Error as JniError, Result as JniResult, ThrowRuntimeExAndDefault};
use jni::objects::{
    Global, JByteArray, JClass, JLongArray, JObject, JObjectArray, JString, JThrowable,
};
use jni::strings::JNIString;
use jni::sys::{jboolean, jint, jlong};
use jni::vm::JavaVM;
use jni::{Env, EnvUnowned, JValue, jni_sig, jni_str};
use tokio::runtime::{Builder, Runtime};

/// The `dev.corium.client.Native` class, cached from the first native call so
/// that completion threads never depend on the system class loader.
static NATIVE_CLASS: OnceLock<Global<JClass<'static>>> = OnceLock::new();
static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

/// Builds the Java value a settled future completes with.
type Build<T> = for<'env> fn(&mut Env<'env>, T) -> JniResult<JObject<'env>>;

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("corium-peer")
            .build()
            .expect("the Corium native runtime could not start")
    })
}

fn initialize(env: &mut Env<'_>) -> JniResult<()> {
    if NATIVE_CLASS.get().is_some() {
        return Ok(());
    }
    // `find_class` resolves through the class loader of the class that declared
    // the running native method, which the completion threads do not have.
    let class = env.find_class(jni_str!("dev/corium/client/Native"))?;
    let class = env.new_global_ref(&class)?;
    let vm = env.get_java_vm()?;
    let _ = JAVA_VM.set(vm);
    let _ = NATIVE_CLASS.set(class);
    Ok(())
}

/// Why a Java future is completing exceptionally.
enum Failure {
    /// The facade categorized the failure.
    Ffi(FfiError),
    /// This adapter, or the engine task it drives, failed outside the facade's
    /// taxonomy.
    Native(String),
}

impl Failure {
    fn kind(&self) -> &'static str {
        match self {
            Self::Ffi(error) => error_kind(error.kind()),
            Self::Native(_) => "native",
        }
    }

    fn message(&self) -> String {
        match self {
            Self::Ffi(error) => error.message(),
            Self::Native(message) => message.clone(),
        }
    }

    fn grpc_code(&self) -> i32 {
        match self {
            Self::Ffi(error) => error.grpc_code().unwrap_or(-1),
            Self::Native(_) => -1,
        }
    }
}

fn spawn<T, F>(future: Global<JObject<'static>>, task: F, build: Build<T>)
where
    T: Send + 'static,
    F: Future<Output = Result<T, FfiError>> + Send + 'static,
{
    runtime().spawn(async move {
        // The engine future runs as its own task so that Tokio catches a panic
        // in it and reports it through the join handle. Awaiting it here means
        // the Java future still settles when the task dies, instead of leaving
        // a caller blocked in `join()` forever.
        let outcome = match tokio::spawn(task).await {
            Ok(result) => result.map_err(Failure::Ffi),
            Err(error) => Err(Failure::Native(
                if error.is_panic() {
                    "the Corium engine panicked"
                } else {
                    "the Corium engine task was cancelled"
                }
                .to_owned(),
            )),
        };
        settle(&future, outcome, build);
    });
}

fn settle<T>(future: &Global<JObject<'static>>, outcome: Result<T, Failure>, build: Build<T>) {
    let Some(vm) = JAVA_VM.get() else {
        report_unsettled("the Java VM is unavailable");
        return;
    };
    // The attachment is scoped so that a runtime worker never stays attached: a
    // permanently attached thread counts as a live non-daemon thread and would
    // keep the JVM from exiting.
    let delivered = vm.attach_current_thread_for_scope(|env| -> JniResult<()> {
        // Caught here rather than around the attachment so that a panic never
        // unwinds through the attach guard.
        match catch_unwind(AssertUnwindSafe(|| deliver(env, future, outcome, build))) {
            Ok(result) => result,
            Err(_) => Err(JniError::JavaException),
        }
    });
    let Err(error) = delivered else {
        return;
    };
    // Last resort: settle with an exception built without the adapter class or
    // the Corium taxonomy, so the caller sees a diagnosable failure instead of
    // a future that never completes.
    let fallback = vm.attach_current_thread_for_scope(|env| -> JniResult<()> {
        let message = format!("the Corium native adapter could not deliver a result: {error}");
        let message = env.new_string(&message)?;
        let throwable = env.new_object(
            jni_str!("java/lang/RuntimeException"),
            jni_sig!("(Ljava/lang/String;)V"),
            &[JValue::Object(&message)],
        )?;
        complete_exceptionally(env, future, &throwable)
    });
    if let Err(fallback) = fallback {
        report_unsettled(&fallback.to_string());
    }
}

fn deliver<T>(
    env: &mut Env<'_>,
    future: &Global<JObject<'static>>,
    outcome: Result<T, Failure>,
    build: Build<T>,
) -> JniResult<()> {
    match outcome {
        Ok(value) => {
            let value = build(env, value)?;
            env.call_method(
                future,
                jni_str!("complete"),
                jni_sig!("(Ljava/lang/Object;)Z"),
                &[JValue::Object(&value)],
            )?;
        }
        Err(failure) => {
            let throwable = java_error(env, &failure)?;
            complete_exceptionally(env, future, &throwable)?;
        }
    }
    Ok(())
}

fn complete_exceptionally(
    env: &mut Env<'_>,
    future: &Global<JObject<'static>>,
    throwable: &JObject<'_>,
) -> JniResult<()> {
    env.call_method(
        future,
        jni_str!("completeExceptionally"),
        jni_sig!("(Ljava/lang/Throwable;)Z"),
        &[JValue::Object(throwable)],
    )
    .map(|_| ())
}

/// Reports a result that could not reach Java at all. The Java side is already
/// unreachable here, so stderr is the only channel left.
fn report_unsettled(detail: &str) {
    eprintln!("corium: a native result could not be delivered to Java: {detail}");
}

fn error_kind(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Connection => "connection",
        ErrorKind::Authentication => "authentication",
        ErrorKind::PermissionDenied => "permission_denied",
        ErrorKind::Protocol => "protocol",
        ErrorKind::Query => "query",
        ErrorKind::Transaction => "transaction",
        ErrorKind::Decode => "decode",
        ErrorKind::Storage => "storage",
        ErrorKind::FuelExhausted => "fuel_exhausted",
        ErrorKind::Closed => "closed",
    }
}

fn java_error<'env>(env: &mut Env<'env>, failure: &Failure) -> JniResult<JObject<'env>> {
    let class = NATIVE_CLASS.get().ok_or(JniError::NullPtr(
        "the Corium native adapter is uninitialized",
    ))?;
    let kind = env.new_string(failure.kind())?;
    let message = env.new_string(failure.message())?;
    env.call_static_method(
        class,
        jni_str!("error"),
        jni_sig!("(Ljava/lang/String;Ljava/lang/String;I)Ldev/corium/client/CoriumException;"),
        &[
            JValue::Object(&kind),
            JValue::Object(&message),
            JValue::Int(failure.grpc_code()),
        ],
    )?
    .l()
}

/// Throws the categorized Corium exception for a synchronous failure.
fn throw_error(env: &mut Env<'_>, error: FfiError) -> JniError {
    let thrown = java_error(env, &Failure::Ffi(error))
        .and_then(|throwable| env.cast_local::<JThrowable>(throwable))
        .and_then(|throwable| env.throw(throwable));
    match thrown {
        Ok(()) | Err(JniError::JavaException) => JniError::JavaException,
        Err(error) => error,
    }
}

fn throw_argument(env: &mut Env<'_>, message: &str) -> JniError {
    let _ = env.throw_new(
        jni_str!("java/lang/IllegalArgumentException"),
        JNIString::from(message),
    );
    JniError::JavaException
}

// ---------------------------------------------------------------------------
// Boundary conversions
// ---------------------------------------------------------------------------

fn optional_string(env: &Env<'_>, value: &JString<'_>) -> JniResult<Option<String>> {
    if value.is_null() {
        Ok(None)
    } else {
        value.try_to_string(env).map(Some)
    }
}

fn optional_bytes(env: &Env<'_>, value: &JByteArray<'_>) -> JniResult<Option<Vec<u8>>> {
    if value.is_null() {
        Ok(None)
    } else {
        env.convert_byte_array(value).map(Some)
    }
}

fn string_list(env: &mut Env<'_>, array: &JObjectArray<'_, JString<'_>>) -> JniResult<Vec<String>> {
    let length = array.len(env)?;
    let mut values = Vec::with_capacity(length);
    for index in 0..length {
        let element = array.get_element(env, index)?;
        values.push(element.try_to_string(env)?);
    }
    Ok(values)
}

fn composite_list(
    env: &mut Env<'_>,
    array: &JObjectArray<'_, JByteArray<'_>>,
) -> JniResult<Result<Vec<CompositeValue>, FfiError>> {
    let length = array.len(env)?;
    let mut values = Vec::with_capacity(length);
    for index in 0..length {
        let element = array.get_element(env, index)?;
        let bytes = env.convert_byte_array(&element)?;
        match CompositeValue::from_bytes(&bytes) {
            Ok(value) => values.push(value),
            Err(error) => return Ok(Err(error)),
        }
    }
    Ok(Ok(values))
}

fn composite(env: &Env<'_>, value: &JByteArray<'_>) -> JniResult<Result<CompositeValue, FfiError>> {
    let bytes = env.convert_byte_array(value)?;
    Ok(CompositeValue::from_bytes(&bytes))
}

fn tls_options(
    tls: jboolean,
    ca_certificate: Option<Vec<u8>>,
    domain_name: Option<String>,
) -> Option<ClientTlsOptions> {
    tls.then_some(ClientTlsOptions {
        ca_certificate,
        domain_name,
    })
}

fn string_array<'env>(env: &mut Env<'env>, values: &[String]) -> JniResult<JObject<'env>> {
    let empty = env.new_string("")?;
    let array = JObjectArray::<JString>::new(env, values.len(), &empty)?;
    for (index, value) in values.iter().enumerate() {
        let element = env.new_string(value)?;
        array.set_element(env, index, &element)?;
    }
    Ok(array.into())
}

fn long_array<'env>(env: &mut Env<'env>, values: &[jlong]) -> JniResult<JLongArray<'env>> {
    let array = JLongArray::new(env, values.len())?;
    array.set_region(env, 0, values)?;
    Ok(array)
}

// ---------------------------------------------------------------------------
// Opaque handles
// ---------------------------------------------------------------------------

/// # Safety
/// `handle` must be a pointer returned by [`build_peer`] whose owning Java
/// object is still reachable, which the Java layer enforces with a cleaner and
/// explicit reachability fences.
unsafe fn peer_handle<'handle>(handle: jlong) -> &'handle PeerHandle {
    unsafe { &*(handle as *const PeerHandle) }
}

/// # Safety
/// `handle` must be a pointer returned by [`build_db`] whose owning Java object
/// is still reachable.
unsafe fn db_handle<'handle>(handle: jlong) -> &'handle DbHandle {
    unsafe { &*(handle as *const DbHandle) }
}

fn build_peer<'env>(env: &mut Env<'env>, handle: PeerHandle) -> JniResult<JObject<'env>> {
    let pointer = Box::into_raw(Box::new(handle)) as jlong;
    boxed_long(env, pointer)
}

fn build_db<'env>(env: &mut Env<'env>, handle: DbHandle) -> JniResult<JObject<'env>> {
    let pointer = Box::into_raw(Box::new(handle)) as jlong;
    boxed_long(env, pointer)
}

fn boxed_long<'env>(env: &mut Env<'env>, value: jlong) -> JniResult<JObject<'env>> {
    env.call_static_method(
        jni_str!("java/lang/Long"),
        jni_str!("valueOf"),
        jni_sig!("(J)Ljava/lang/Long;"),
        &[JValue::Long(value)],
    )?
    .l()
}

fn build_string<'env>(env: &mut Env<'env>, value: String) -> JniResult<JObject<'env>> {
    Ok(env.new_string(value)?.into())
}

fn build_bytes<'env>(env: &mut Env<'env>, value: CompositeValue) -> JniResult<JObject<'env>> {
    Ok(env.byte_array_from_slice(&value.to_bytes())?.into())
}

fn build_stats<'env>(env: &mut Env<'env>, stats: DbStats) -> JniResult<JObject<'env>> {
    let values = [
        stats.basis_t as jlong,
        stats.datoms as jlong,
        stats.entities as jlong,
        stats.attributes as jlong,
    ];
    Ok(long_array(env, &values)?.into())
}

fn shape_name(shape: ResultShape) -> &'static str {
    match shape {
        ResultShape::Relation => "relation",
        ResultShape::Collection => "collection",
        ResultShape::Tuple => "tuple",
        ResultShape::Scalar => "scalar",
    }
}

/// Builds `{String shape, byte[] value}`.
fn build_query<'env>(env: &mut Env<'env>, output: QueryOutput) -> JniResult<JObject<'env>> {
    let shape = env.new_string(shape_name(output.shape))?;
    let value = env.byte_array_from_slice(&output.value.to_bytes())?;
    let array = JObjectArray::<JObject>::new(env, 2, &JObject::null())?;
    array.set_element(env, 0, &shape)?;
    array.set_element(env, 1, &value)?;
    Ok(array.into())
}

/// Builds `{long[] {e, a, tx, added} * n, byte[][] values}`.
fn build_datoms<'env>(env: &mut Env<'env>, datoms: Vec<Datom>) -> JniResult<JObject<'env>> {
    let mut scalars = Vec::with_capacity(datoms.len() * 4);
    for datom in &datoms {
        scalars.push(datom.e as jlong);
        scalars.push(datom.a as jlong);
        scalars.push(datom.tx as jlong);
        scalars.push(jlong::from(datom.added));
    }
    let scalars = long_array(env, &scalars)?;
    let empty = env.byte_array_from_slice(&[])?;
    let values = JObjectArray::<JByteArray>::new(env, datoms.len(), &empty)?;
    for (index, datom) in datoms.iter().enumerate() {
        let value = env.byte_array_from_slice(&datom.value.to_bytes())?;
        values.set_element(env, index, &value)?;
    }
    let array = JObjectArray::<JObject>::new(env, 2, &JObject::null())?;
    array.set_element(env, 0, &scalars)?;
    array.set_element(env, 1, &values)?;
    Ok(array.into())
}

/// Builds `{long[] {basisBefore, basisT, txInstant, dbAfter}, String[] tempid
/// names, long[] tempid entities}`.
fn build_tx_report<'env>(env: &mut Env<'env>, report: TxReport) -> JniResult<JObject<'env>> {
    let db_after = Box::into_raw(Box::new(report.db_after)) as jlong;
    let scalars = long_array(
        env,
        &[
            report.basis_before as jlong,
            report.basis_t as jlong,
            report.tx_instant,
            db_after,
        ],
    )?;
    let names = report.tempids.keys().cloned().collect::<Vec<_>>();
    let entities = report
        .tempids
        .values()
        .map(|entity| *entity as jlong)
        .collect::<Vec<_>>();
    let names = string_array(env, &names)?;
    let entities = long_array(env, &entities)?;
    let array = JObjectArray::<JObject>::new(env, 3, &JObject::null())?;
    array.set_element(env, 0, &scalars)?;
    array.set_element(env, 1, &names)?;
    array.set_element(env, 2, &entities)?;
    Ok(array.into())
}

// ---------------------------------------------------------------------------
// Database views
// ---------------------------------------------------------------------------

/// Derives the view `kind` names. The ordinals are the wire contract with
/// `dev.corium.client.DbView`, whose constants must stay in step.
fn view(db: &DbHandle, kind: jint, value: jlong) -> Result<DbHandle, FfiError> {
    match kind {
        1 => db.as_of(value as u64),
        2 => db.since(value as u64),
        3 => db.history(),
        4 => db.as_of_instant(value),
        5 => db.since_instant(value),
        _ => Ok(db.clone()),
    }
}

fn check_view(env: &mut Env<'_>, kind: jint) -> JniResult<()> {
    if (0..=5).contains(&kind) {
        Ok(())
    } else {
        Err(throw_argument(env, "unknown database view"))
    }
}

fn index_for(env: &mut Env<'_>, name: &str) -> JniResult<Index> {
    match name {
        "eavt" => Ok(Index::Eavt),
        "aevt" => Ok(Index::Aevt),
        "avet" => Ok(Index::Avet),
        "vaet" => Ok(Index::Vaet),
        _ => Err(throw_argument(env, "unknown datom index")),
    }
}

// ---------------------------------------------------------------------------
// Native methods
// ---------------------------------------------------------------------------

/// Caches the JVM and the adapter class for later completion threads.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_initialize<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> { initialize(env) })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Returns the direct-storage backends registered in this process.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_storageBackends<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
) -> JObject<'local> {
    unowned
        .with_env(|env| -> JniResult<JObject<'local>> {
            initialize(env)?;
            string_array(env, &available_storage_backends())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// Loads one dynamic storage plugin and returns the backends it serves.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_loadStoragePlugin<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> JObject<'local> {
    unowned
        .with_env(|env| -> JniResult<JObject<'local>> {
            initialize(env)?;
            let path = path.try_to_string(env)?;
            match load_storage_plugin(PathBuf::from(path)) {
                Ok(backends) => string_array(env, &backends),
                Err(error) => Err(throw_error(env, error)),
            }
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// Reports the backend one transactor advertises, without opening it.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_discoverStorageBackend<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    endpoints: JObjectArray<'local, JString<'local>>,
    database: JString<'local>,
    token: JString<'local>,
    tls: jboolean,
    tls_ca: JByteArray<'local>,
    tls_domain: JString<'local>,
    future: JObject<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> {
            initialize(env)?;
            let endpoints = string_list(env, &endpoints)?;
            let database = database.try_to_string(env)?;
            let token = optional_string(env, &token)?;
            let tls = tls_options(
                tls,
                optional_bytes(env, &tls_ca)?,
                optional_string(env, &tls_domain)?,
            );
            let future = env.new_global_ref(&future)?;
            spawn(
                future,
                async move { discover_storage_backend(&endpoints, &database, token, tls).await },
                build_string,
            );
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Connects an in-process full peer.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_dev_corium_client_Native_connectLocal<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    endpoints: JObjectArray<'local, JString<'local>>,
    database: JString<'local>,
    token: JString<'local>,
    tls: jboolean,
    tls_ca: JByteArray<'local>,
    tls_domain: JString<'local>,
    direct_storage: jboolean,
    cache_directory: JString<'local>,
    cache_capacity_bytes: jlong,
    cache_memory_bytes: jlong,
    future: JObject<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> {
            initialize(env)?;
            let endpoints = string_list(env, &endpoints)?;
            let database = database.try_to_string(env)?;
            let token = optional_string(env, &token)?;
            let tls = tls_options(
                tls,
                optional_bytes(env, &tls_ca)?,
                optional_string(env, &tls_domain)?,
            );
            let segment_cache = match optional_string(env, &cache_directory)? {
                None => None,
                Some(directory) => {
                    let capacity_bytes = u64::try_from(cache_capacity_bytes)
                        .map_err(|_| throw_argument(env, "segment cache capacity is negative"))?;
                    let memory_capacity_bytes = u64::try_from(cache_memory_bytes)
                        .map_err(|_| throw_argument(env, "segment cache memory is negative"))?;
                    Some(SegmentCacheOptions {
                        directory: PathBuf::from(directory),
                        capacity_bytes,
                        memory_capacity_bytes: if memory_capacity_bytes == 0 {
                            SegmentCacheOptions::DEFAULT_MEMORY_CAPACITY_BYTES.min(capacity_bytes)
                        } else {
                            memory_capacity_bytes
                        },
                    })
                }
            };
            let options = LocalConnectOptions {
                endpoints,
                database_name: database,
                token,
                tls,
                direct_storage: direct_storage.then_some(DirectStorageOptions { segment_cache }),
            };
            let future = env.new_global_ref(&future)?;
            spawn(
                future,
                async move { PeerHandle::connect_local(options).await },
                build_peer,
            );
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Returns the connected database name.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_peerDatabaseName<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    peer: jlong,
) -> JString<'local> {
    unowned
        .with_env(|env| -> JniResult<JString<'local>> {
            // SAFETY: the caller holds the owning Java peer.
            let handle = unsafe { peer_handle(peer) };
            env.new_string(handle.database_name())
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// Returns the current immutable database value.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_peerDb<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    peer: jlong,
    future: JObject<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> {
            // SAFETY: the caller holds the owning Java peer.
            let handle = unsafe { peer_handle(peer) }.clone();
            let future = env.new_global_ref(&future)?;
            spawn(future, async move { handle.db().await }, build_db);
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Synchronizes with the transactor and returns the resulting database.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_peerSync<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    peer: jlong,
    future: JObject<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> {
            // SAFETY: the caller holds the owning Java peer.
            let handle = unsafe { peer_handle(peer) }.clone();
            let future = env.new_global_ref(&future)?;
            spawn(future, async move { handle.sync().await }, build_db);
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Submits composite-encoded transaction data.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_peerTransact<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    peer: jlong,
    tx_data: JByteArray<'local>,
    future: JObject<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> {
            // SAFETY: the caller holds the owning Java peer.
            let handle = unsafe { peer_handle(peer) }.clone();
            let forms = match composite(env, &tx_data)? {
                Ok(forms) => forms,
                Err(error) => return Err(throw_error(env, error)),
            };
            let future = env.new_global_ref(&future)?;
            spawn(
                future,
                async move { handle.transact(forms).await },
                build_tx_report,
            );
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Idempotently releases the live peer held by the facade.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_peerClose<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    peer: jlong,
) {
    unowned
        .with_env(|_env| -> JniResult<()> {
            // SAFETY: the caller holds the owning Java peer.
            unsafe { peer_handle(peer) }.close();
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Frees one peer handle once its Java owner is unreachable.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_freePeer<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    peer: jlong,
) {
    unowned
        .with_env(|_env| -> JniResult<()> {
            if peer != 0 {
                // SAFETY: the Java cleaner runs once, after the peer is
                // unreachable, so this pointer is not aliased or freed twice.
                drop(unsafe { Box::from_raw(peer as *mut PeerHandle) });
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Frees one database handle once its Java owner is unreachable.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_freeDb<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    db: jlong,
) {
    unowned
        .with_env(|_env| -> JniResult<()> {
            if db != 0 {
                // SAFETY: the Java cleaner runs once, after the database value
                // is unreachable, so this pointer is not aliased or freed twice.
                drop(unsafe { Box::from_raw(db as *mut DbHandle) });
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Executes a composite-encoded query against one database view.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_dev_corium_client_Native_dbQuery<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    db: jlong,
    view_kind: jint,
    view_value: jlong,
    query: JByteArray<'local>,
    args: JObjectArray<'local, JByteArray<'local>>,
    fuel: jlong,
    future: JObject<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> {
            check_view(env, view_kind)?;
            // SAFETY: the caller holds the owning Java database value.
            let handle = unsafe { db_handle(db) }.clone();
            let query = match composite(env, &query)? {
                Ok(query) => query,
                Err(error) => return Err(throw_error(env, error)),
            };
            let args = match composite_list(env, &args)? {
                Ok(args) => args,
                Err(error) => return Err(throw_error(env, error)),
            };
            let fuel =
                u64::try_from(fuel).map_err(|_| throw_argument(env, "query fuel is negative"))?;
            let future = env.new_global_ref(&future)?;
            spawn(
                future,
                async move {
                    let db = view(&handle, view_kind, view_value)?;
                    db.query(query, args, (fuel != 0).then_some(fuel)).await
                },
                build_query,
            );
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Executes a composite-encoded Pull pattern against one database view.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_dbPull<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    db: jlong,
    view_kind: jint,
    view_value: jlong,
    pattern: JByteArray<'local>,
    entity: JByteArray<'local>,
    future: JObject<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> {
            check_view(env, view_kind)?;
            // SAFETY: the caller holds the owning Java database value.
            let handle = unsafe { db_handle(db) }.clone();
            let pattern = match composite(env, &pattern)? {
                Ok(pattern) => pattern,
                Err(error) => return Err(throw_error(env, error)),
            };
            let entity = match composite(env, &entity)? {
                Ok(entity) => entity,
                Err(error) => return Err(throw_error(env, error)),
            };
            let future = env.new_global_ref(&future)?;
            spawn(
                future,
                async move {
                    let db = view(&handle, view_kind, view_value)?;
                    db.pull(pattern, entity).await
                },
                build_bytes,
            );
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Scans a covering index from a component prefix.
#[unsafe(no_mangle)]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_dev_corium_client_Native_dbDatoms<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    db: jlong,
    view_kind: jint,
    view_value: jlong,
    index: JString<'local>,
    components: JObjectArray<'local, JByteArray<'local>>,
    limit: jlong,
    future: JObject<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> {
            check_view(env, view_kind)?;
            // SAFETY: the caller holds the owning Java database value.
            let handle = unsafe { db_handle(db) }.clone();
            let index = index.try_to_string(env)?;
            let index = index_for(env, &index)?;
            let components = match composite_list(env, &components)? {
                Ok(components) => components,
                Err(error) => return Err(throw_error(env, error)),
            };
            let limit =
                u64::try_from(limit).map_err(|_| throw_argument(env, "datom limit is negative"))?;
            let future = env.new_global_ref(&future)?;
            spawn(
                future,
                async move {
                    let db = view(&handle, view_kind, view_value)?;
                    db.datoms(index, components, limit).await
                },
                build_datoms,
            );
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// Returns coarse statistics for one database view.
#[unsafe(no_mangle)]
pub extern "system" fn Java_dev_corium_client_Native_dbStats<'local>(
    mut unowned: EnvUnowned<'local>,
    _class: JClass<'local>,
    db: jlong,
    view_kind: jint,
    view_value: jlong,
    future: JObject<'local>,
) {
    unowned
        .with_env(|env| -> JniResult<()> {
            check_view(env, view_kind)?;
            // SAFETY: the caller holds the owning Java database value.
            let handle = unsafe { db_handle(db) }.clone();
            let future = env.new_global_ref(&future)?;
            spawn(
                future,
                async move {
                    let db = view(&handle, view_kind, view_value)?;
                    db.stats().await
                },
                build_stats,
            );
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}
