//! `PyO3` adapter for the Corium Python package.
//!
//! The public API remains in `clients/python/src/corium`. This module only
//! converts owned boundary values, adapts facade futures to `asyncio`, and
//! exposes opaque backend objects consumed by that Python layer.

use corium_core::TotalF64;
use corium_ffi::{
    ClientTlsOptions, CompositeValue, DbHandle, DirectStorageOptions, ErrorKind, FfiError, Index,
    LocalConnectOptions, PeerHandle, RemoteConnectOptions, SegmentCacheOptions,
    compiled_storage_backends,
};
use corium_protocol::codec;
use corium_query::edn::Edn;
use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyAny, PyBool, PyBytes, PyDict, PyFloat, PyFrozenSet, PyList, PySet, PyString, PyTuple,
};

const RESERVED_TAGS: [&str; 4] = ["bytes", "eid", "inst", "uuid"];

#[cfg(any(
    all(feature = "artifact-turso", any(feature = "postgres", feature = "s3")),
    all(feature = "artifact-postgres", any(feature = "turso", feature = "s3")),
    all(feature = "artifact-s3", any(feature = "turso", feature = "postgres")),
))]
compile_error!("a corium-python artifact must enable exactly one storage backend");

struct PythonRuntime {
    keyword: Py<PyAny>,
    symbol: Py<PyAny>,
    entity_id: Py<PyAny>,
    edn_list: Py<PyAny>,
    tagged: Py<PyAny>,
    datetime: Py<PyAny>,
    timezone_utc: Py<PyAny>,
    uuid: Py<PyAny>,
    unix_millis: Py<PyAny>,
    tx_report: Py<PyAny>,
    datom: Py<PyAny>,
    db_stats: Py<PyAny>,
}

static PYTHON_RUNTIME: PyOnceLock<PythonRuntime> = PyOnceLock::new();

fn python_runtime(py: Python<'_>) -> PyResult<&'static PythonRuntime> {
    PYTHON_RUNTIME.get_or_try_init(py, || {
        let values = py.import("corium.values")?;
        let datetime = py.import("datetime")?;
        let api = py.import("corium._api")?;
        Ok(PythonRuntime {
            keyword: values.getattr("Keyword")?.unbind(),
            symbol: values.getattr("Symbol")?.unbind(),
            entity_id: values.getattr("EntityId")?.unbind(),
            edn_list: values.getattr("EdnList")?.unbind(),
            tagged: values.getattr("Tagged")?.unbind(),
            datetime: datetime.getattr("datetime")?.unbind(),
            timezone_utc: datetime.getattr("timezone")?.getattr("utc")?.unbind(),
            uuid: py.import("uuid")?.getattr("UUID")?.unbind(),
            unix_millis: api.getattr("_unix_millis")?.unbind(),
            tx_report: api.getattr("_TxReportData")?.unbind(),
            datom: api.getattr("Datom")?.unbind(),
            db_stats: api.getattr("DbStats")?.unbind(),
        })
    })
}

#[pyclass(module = "corium._corium", name = "_PeerBackend")]
struct PythonPeer {
    handle: PeerHandle,
}

impl Drop for PythonPeer {
    fn drop(&mut self) {
        // Explicit close is the deterministic API; this idempotent call is only
        // a best-effort safety net when Python releases an unclosed peer.
        self.handle.close();
    }
}

#[pymethods]
impl PythonPeer {
    #[getter]
    fn database_name(&self) -> String {
        self.handle.database_name()
    }

    fn db<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let peer = self.handle.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let db = peer.db().await.map_err(ffi_error)?;
            Python::attach(|py| Py::new(py, PythonDb { handle: db }))
        })
    }

    fn sync<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let peer = self.handle.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let db = peer.sync().await.map_err(ffi_error)?;
            Python::attach(|py| Py::new(py, PythonDb { handle: db }))
        })
    }

    fn transact<'py>(
        &self,
        py: Python<'py>,
        tx_data: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let forms = python_to_composite(tx_data)?;
        let peer = self.handle.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let report = peer.transact(forms).await.map_err(ffi_error)?;
            Python::attach(|py| {
                let report_type = python_runtime(py)?.tx_report.bind(py);
                let tempids = PyDict::new(py);
                for (name, entity) in report.tempids {
                    tempids.set_item(name, entity)?;
                }
                let db = Py::new(
                    py,
                    PythonDb {
                        handle: report.db_after,
                    },
                )?;
                report_type
                    .call1((
                        report.basis_before,
                        report.basis_t,
                        datetime_from_millis(py, report.tx_instant)?,
                        tempids,
                        db,
                    ))
                    .map(Bound::unbind)
            })
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let peer = self.handle.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            peer.close();
            Ok(())
        })
    }
}

#[pyclass(module = "corium._corium", name = "_DbBackend")]
struct PythonDb {
    // DbHandle is an immutable view over Arc-backed peer state. It owns no
    // independent connection or server resource, so dropping it needs no close.
    handle: DbHandle,
}

#[pymethods]
impl PythonDb {
    #[getter]
    fn database_name(&self) -> String {
        self.handle.database_name()
    }

    #[pyo3(signature = (view, query, args, fuel=None))]
    fn query<'py>(
        &self,
        py: Python<'py>,
        view: &Bound<'py, PyAny>,
        query: &Bound<'py, PyAny>,
        args: &Bound<'py, PyTuple>,
        fuel: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = db_for_view(&self.handle, view)?;
        let query = python_to_composite(query)?;
        let args = args
            .iter()
            .map(|arg| python_to_composite(&arg))
            .collect::<PyResult<Vec<_>>>()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let output = db.query(query, args, fuel).await.map_err(ffi_error)?;
            Python::attach(|py| composite_to_python(py, &output.value).map(Bound::unbind))
        })
    }

    fn pull<'py>(
        &self,
        py: Python<'py>,
        view: &Bound<'py, PyAny>,
        pattern: &Bound<'py, PyAny>,
        entity: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = db_for_view(&self.handle, view)?;
        let pattern = python_to_composite(pattern)?;
        let entity = python_to_composite(entity)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let value = db.pull(pattern, entity).await.map_err(ffi_error)?;
            Python::attach(|py| composite_to_python(py, &value).map(Bound::unbind))
        })
    }

    fn datoms<'py>(
        &self,
        py: Python<'py>,
        view: &Bound<'py, PyAny>,
        index: &Bound<'py, PyAny>,
        components: &Bound<'py, PyTuple>,
        limit: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = db_for_view(&self.handle, view)?;
        let index = python_index(index)?;
        let components = components
            .iter()
            .map(|component| python_to_composite(&component))
            .collect::<PyResult<Vec<_>>>()?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let rows = db
                .datoms(index, components, limit)
                .await
                .map_err(ffi_error)?;
            Python::attach(|py| {
                let datom_type = python_runtime(py)?.datom.bind(py);
                let result = PyList::empty(py);
                for row in rows {
                    result.append(datom_type.call1((
                        row.e,
                        row.a,
                        composite_to_python(py, &row.value)?,
                        row.tx,
                        row.added,
                    ))?)?;
                }
                Ok(result.unbind())
            })
        })
    }

    fn stats<'py>(&self, py: Python<'py>, view: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let db = db_for_view(&self.handle, view)?;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let stats = db.stats().await.map_err(ffi_error)?;
            Python::attach(|py| {
                python_runtime(py)?
                    .db_stats
                    .bind(py)
                    .call1((
                        stats.basis_t,
                        stats.datoms,
                        stats.entities,
                        stats.attributes,
                    ))
                    .map(Bound::unbind)
            })
        })
    }
}

#[pyfunction]
#[pyo3(signature = (
    endpoint, *, database, token=None, tls=false, tls_ca=None, tls_domain=None
))]
fn connect_remote(
    py: Python<'_>,
    endpoint: String,
    database: String,
    token: Option<String>,
    tls: bool,
    tls_ca: Option<Vec<u8>>,
    tls_domain: Option<String>,
) -> PyResult<Bound<'_, PyAny>> {
    let tls = client_tls_options(tls, tls_ca, tls_domain)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let handle = PeerHandle::connect_remote(RemoteConnectOptions {
            endpoint,
            database_name: database,
            token,
            tls,
        })
        .await
        .map_err(ffi_error)?;
        Python::attach(|py| Py::new(py, PythonPeer { handle }))
    })
}

#[pyfunction]
#[pyo3(signature = (
    endpoints, *, database, token=None, tls=false, tls_ca=None, tls_domain=None,
    direct_storage=false, segment_cache_directory=None,
    segment_cache_capacity_bytes=None, segment_cache_memory_bytes=None
))]
#[allow(clippy::too_many_arguments)]
fn connect_local(
    py: Python<'_>,
    endpoints: Vec<String>,
    database: String,
    token: Option<String>,
    tls: bool,
    tls_ca: Option<Vec<u8>>,
    tls_domain: Option<String>,
    direct_storage: bool,
    segment_cache_directory: Option<String>,
    segment_cache_capacity_bytes: Option<u64>,
    segment_cache_memory_bytes: Option<u64>,
) -> PyResult<Bound<'_, PyAny>> {
    let tls = client_tls_options(tls, tls_ca, tls_domain)?;
    let segment_cache = match (
        segment_cache_directory,
        segment_cache_capacity_bytes,
        segment_cache_memory_bytes,
    ) {
        (None, None, None) => None,
        (Some(directory), Some(capacity_bytes), memory_capacity_bytes) => {
            Some(SegmentCacheOptions {
                directory: directory.into(),
                capacity_bytes,
                memory_capacity_bytes: memory_capacity_bytes.unwrap_or_else(|| {
                    SegmentCacheOptions::DEFAULT_MEMORY_CAPACITY_BYTES.min(capacity_bytes)
                }),
            })
        }
        _ => {
            return Err(PyValueError::new_err(
                "segment cache requires both a directory and capacity",
            ));
        }
    };
    if segment_cache.is_some() && !direct_storage {
        return Err(PyValueError::new_err(
            "segment cache requires direct_storage=True",
        ));
    }
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let handle = PeerHandle::connect_local(LocalConnectOptions {
            endpoints,
            database_name: database,
            token,
            tls,
            direct_storage: direct_storage.then_some(DirectStorageOptions { segment_cache }),
        })
        .await
        .map_err(ffi_error)?;
        Python::attach(|py| Py::new(py, PythonPeer { handle }))
    })
}

#[pyfunction]
fn _storage_backends() -> Vec<&'static str> {
    compiled_storage_backends()
}

fn client_tls_options(
    tls: bool,
    ca_certificate: Option<Vec<u8>>,
    domain_name: Option<String>,
) -> PyResult<Option<ClientTlsOptions>> {
    if !tls && (ca_certificate.is_some() || domain_name.is_some()) {
        return Err(PyValueError::new_err(
            "custom TLS options require an https:// endpoint",
        ));
    }
    Ok(tls.then_some(ClientTlsOptions {
        ca_certificate,
        domain_name,
    }))
}

#[pyfunction]
fn _roundtrip<'py>(py: Python<'py>, value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    composite_to_python(py, &python_to_composite(value)?)
}

fn db_for_view(db: &DbHandle, view: &Bound<'_, PyAny>) -> PyResult<DbHandle> {
    let kind: String = view.getattr("kind")?.extract()?;
    let value = view.getattr("value")?;
    match kind.as_str() {
        "current" => Ok(db.clone()),
        "as_of" => db.as_of(value.extract()?).map_err(ffi_error),
        "since" => db.since(value.extract()?).map_err(ffi_error),
        "history" => db.history().map_err(ffi_error),
        "as_of_instant" => db.as_of_instant(value.extract()?).map_err(ffi_error),
        "since_instant" => db.since_instant(value.extract()?).map_err(ffi_error),
        _ => Err(PyValueError::new_err(format!(
            "unknown database view: {kind}"
        ))),
    }
}

fn python_index(index: &Bound<'_, PyAny>) -> PyResult<Index> {
    let value: String = if index.hasattr("value")? {
        index.getattr("value")?.extract()?
    } else {
        index.extract()?
    };
    match value.as_str() {
        "eavt" => Ok(Index::Eavt),
        "aevt" => Ok(Index::Aevt),
        "avet" => Ok(Index::Avet),
        "vaet" => Ok(Index::Vaet),
        _ => Err(PyValueError::new_err(format!(
            "unknown datom index: {value}"
        ))),
    }
}

fn python_to_composite(value: &Bound<'_, PyAny>) -> PyResult<CompositeValue> {
    let bytes = codec::encode_edn(&python_to_edn(value)?);
    CompositeValue::from_bytes(&bytes).map_err(ffi_error)
}

#[allow(clippy::too_many_lines)]
fn python_to_edn(value: &Bound<'_, PyAny>) -> PyResult<Edn> {
    let py = value.py();
    if value.is_none() {
        return Ok(Edn::Nil);
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(Edn::Bool(value.extract()?));
    }
    if let Ok(integer) = value.extract::<i64>() {
        return Ok(Edn::Long(integer));
    }
    if value.is_instance_of::<PyFloat>() {
        return Ok(Edn::Double(TotalF64(value.extract()?)));
    }
    if value.is_instance_of::<PyString>() {
        return Ok(Edn::Str(value.extract()?));
    }
    if value.is_instance_of::<PyBytes>() {
        let bytes: &[u8] = value.extract()?;
        return Ok(Edn::Tagged(
            "bytes".into(),
            Box::new(Edn::Str(hex_encode(bytes))),
        ));
    }

    let runtime = python_runtime(py)?;
    if value.is_instance(runtime.keyword.bind(py))? {
        let text: String = value.getattr("value")?.extract()?;
        return Ok(Edn::keyword(&text));
    }
    if value.is_instance(runtime.symbol.bind(py))? {
        return Ok(Edn::Symbol(value.getattr("value")?.extract()?));
    }
    if value.is_instance(runtime.entity_id.bind(py))? {
        let raw: u64 = value.getattr("value")?.extract()?;
        let raw = i64::try_from(raw)
            .map_err(|_| PyOverflowError::new_err("entity id exceeds the boundary range"))?;
        return Ok(Edn::Tagged("eid".into(), Box::new(Edn::Long(raw))));
    }
    if value.is_instance(runtime.edn_list.bind(py))? {
        return value
            .getattr("items")?
            .try_iter()?
            .map(|item| python_to_edn(&item?))
            .collect::<PyResult<Vec<_>>>()
            .map(Edn::List);
    }
    if value.is_instance(runtime.tagged.bind(py))? {
        let tag: String = value.getattr("tag")?.extract()?;
        if RESERVED_TAGS.contains(&tag.as_str()) {
            return Err(PyValueError::new_err(format!(
                "tag {tag:?} is reserved for a dedicated Corium boundary type"
            )));
        }
        let inner = value.getattr("value")?;
        return Ok(Edn::Tagged(tag, Box::new(python_to_edn(&inner)?)));
    }

    if value.is_instance(runtime.datetime.bind(py))? {
        let millis: i64 = runtime.unix_millis.bind(py).call1((value,))?.extract()?;
        return Ok(Edn::Tagged("inst".into(), Box::new(Edn::Long(millis))));
    }
    if value.is_instance(runtime.uuid.bind(py))? {
        let hex: String = value.getattr("hex")?.extract()?;
        return Ok(Edn::Tagged("uuid".into(), Box::new(Edn::Str(hex))));
    }

    if let Ok(dictionary) = value.cast::<PyDict>() {
        let mut pairs = dictionary
            .iter()
            .map(|(key, value)| Ok((python_to_edn(&key)?, python_to_edn(&value)?)))
            .collect::<PyResult<Vec<_>>>()?;
        pairs.sort_by(|left, right| left.0.cmp(&right.0));
        return Ok(Edn::Map(pairs));
    }
    if let Ok(list) = value.cast::<PyList>() {
        return list
            .iter()
            .map(|item| python_to_edn(&item))
            .collect::<PyResult<Vec<_>>>()
            .map(Edn::Vector);
    }
    if let Ok(tuple) = value.cast::<PyTuple>() {
        return tuple
            .iter()
            .map(|item| python_to_edn(&item))
            .collect::<PyResult<Vec<_>>>()
            .map(Edn::Vector);
    }
    if let Ok(set) = value.cast::<PySet>() {
        return python_set_to_edn(set.iter());
    }
    if let Ok(set) = value.cast::<PyFrozenSet>() {
        return python_set_to_edn(set.iter());
    }

    Err(PyTypeError::new_err(format!(
        "unsupported Corium boundary value: {}",
        value.get_type().qualname()?
    )))
}

fn python_set_to_edn<'py>(items: impl Iterator<Item = Bound<'py, PyAny>>) -> PyResult<Edn> {
    let mut values = items
        .map(|item| python_to_edn(&item))
        .collect::<PyResult<Vec<_>>>()?;
    values.sort();
    values.dedup();
    Ok(Edn::Set(values))
}

fn composite_to_python<'py>(
    py: Python<'py>,
    value: &CompositeValue,
) -> PyResult<Bound<'py, PyAny>> {
    let form = codec::decode_edn(&value.to_bytes())
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    edn_to_python(py, &form)
}

fn edn_to_python<'py>(py: Python<'py>, value: &Edn) -> PyResult<Bound<'py, PyAny>> {
    match value {
        Edn::Nil => Ok(py.None().into_bound(py)),
        Edn::Bool(value) => Ok(value.into_pyobject(py)?.to_owned().into_any()),
        Edn::Long(value) => Ok(value.into_pyobject(py)?.into_any()),
        Edn::Double(value) => Ok(value.0.into_pyobject(py)?.into_any()),
        Edn::Str(value) => Ok(value.into_pyobject(py)?.into_any()),
        Edn::Keyword(value) => {
            let rendered = value.to_string();
            let name = rendered.strip_prefix(':').unwrap_or(&rendered);
            python_runtime(py)?.keyword.bind(py).call1((name,))
        }
        Edn::Symbol(value) => python_runtime(py)?.symbol.bind(py).call1((value,)),
        Edn::List(values) => {
            let result = PyList::empty(py);
            for value in values {
                result.append(edn_to_python(py, value)?)?;
            }
            python_runtime(py)?.edn_list.bind(py).call1((result,))
        }
        Edn::Vector(values) => {
            let result = PyList::empty(py);
            for value in values {
                result.append(edn_to_python(py, value)?)?;
            }
            Ok(result.into_any())
        }
        Edn::Set(values) => {
            let values = values
                .iter()
                .map(|value| edn_to_python(py, value))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(PyFrozenSet::new(py, &values)?.into_any())
        }
        Edn::Map(pairs) => {
            let result = PyDict::new(py);
            for (key, value) in pairs {
                result.set_item(edn_to_python(py, key)?, edn_to_python(py, value)?)?;
            }
            Ok(result.into_any())
        }
        Edn::Tagged(tag, value) => tagged_to_python(py, tag, value),
    }
}

fn tagged_to_python<'py>(py: Python<'py>, tag: &str, value: &Edn) -> PyResult<Bound<'py, PyAny>> {
    match (tag, value) {
        ("eid", Edn::Long(raw)) => {
            let raw = u64::try_from(*raw)
                .map_err(|_| PyValueError::new_err("negative entity id from native boundary"))?;
            python_runtime(py)?.entity_id.bind(py).call1((raw,))
        }
        ("inst", Edn::Long(millis)) => datetime_from_millis(py, *millis),
        ("uuid", Edn::Str(hex)) => {
            let kwargs = PyDict::new(py);
            kwargs.set_item("hex", hex)?;
            python_runtime(py)?.uuid.bind(py).call((), Some(&kwargs))
        }
        ("bytes", Edn::Str(hex)) => Ok(PyBytes::new(py, &hex_decode(hex)?).into_any()),
        _ => python_runtime(py)?
            .tagged
            .bind(py)
            .call1((tag, edn_to_python(py, value)?)),
    }
}

fn datetime_from_millis(py: Python<'_>, millis: i64) -> PyResult<Bound<'_, PyAny>> {
    let runtime = python_runtime(py)?;
    let value = runtime.datetime.bind(py).call_method(
        "fromtimestamp",
        (millis.div_euclid(1_000),),
        Some(&{
            let kwargs = PyDict::new(py);
            kwargs.set_item("tz", runtime.timezone_utc.bind(py))?;
            kwargs
        }),
    )?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("microsecond", millis.rem_euclid(1_000) * 1_000)?;
    value.call_method("replace", (), Some(&kwargs))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            use std::fmt::Write as _;
            let _ = write!(output, "{byte:02x}");
            output
        },
    )
}

fn hex_decode(value: &str) -> PyResult<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(PyValueError::new_err("byte value has odd-length hex"));
    }
    (0..value.len())
        .step_by(2)
        .map(|offset| {
            u8::from_str_radix(&value[offset..offset + 2], 16)
                .map_err(|_| PyValueError::new_err("byte value contains invalid hex"))
        })
        .collect()
}

fn ffi_error(error: FfiError) -> PyErr {
    let kind = error.kind();
    let message = error.message();
    let grpc_code = error.grpc_code();
    drop(error);
    Python::attach(|py| {
        let class = match kind {
            ErrorKind::Connection => "ConnectionFailedError",
            ErrorKind::Authentication => "AuthenticationError",
            ErrorKind::PermissionDenied => "PermissionDeniedError",
            ErrorKind::Protocol => "ProtocolError",
            ErrorKind::Query => "QueryError",
            ErrorKind::Transaction => "TransactionError",
            ErrorKind::Decode => "DecodeError",
            ErrorKind::Storage => "StorageError",
            ErrorKind::FuelExhausted => "FuelExhaustedError",
            ErrorKind::Closed => "ClosedError",
        };
        match py
            .import("corium.errors")
            .and_then(|errors| errors.getattr(class))
            .and_then(|class| {
                let kwargs = PyDict::new(py);
                kwargs.set_item("grpc_code", grpc_code)?;
                class.call((message,), Some(&kwargs))
            }) {
            Ok(instance) => PyErr::from_value(instance),
            Err(mapping_error) => mapping_error,
        }
    })
}

fn register_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(connect_remote, module)?)?;
    module.add_function(wrap_pyfunction!(connect_local, module)?)?;
    module.add_function(wrap_pyfunction!(_roundtrip, module)?)?;
    module.add_function(wrap_pyfunction!(_storage_backends, module)?)?;
    module.add_class::<PythonPeer>()?;
    module.add_class::<PythonDb>()?;
    Ok(())
}

#[cfg(not(any(
    feature = "artifact-turso",
    feature = "artifact-postgres",
    feature = "artifact-s3"
)))]
#[pymodule]
fn _corium(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_module(module)
}

#[cfg(feature = "artifact-turso")]
#[pymodule]
fn _corium_turso(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_module(module)
}

#[cfg(feature = "artifact-postgres")]
#[pymodule]
fn _corium_postgres(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_module(module)
}

#[cfg(feature = "artifact-s3")]
#[pymodule]
fn _corium_s3(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_module(module)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_value_type_errors_are_not_reinterpreted_as_string_errors() {
        Python::initialize();
        Python::attach(|py| {
            let kwargs = PyDict::new(py);
            kwargs.set_item("value", 42).unwrap();
            let index = py
                .import("types")
                .unwrap()
                .getattr("SimpleNamespace")
                .unwrap()
                .call((), Some(&kwargs))
                .unwrap();

            let error = python_index(&index).unwrap_err();
            assert!(error.is_instance_of::<PyTypeError>(py));
        });
    }
}
