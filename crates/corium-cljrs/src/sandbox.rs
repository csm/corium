//! Sandboxed Clojurust execution for database functions (ADR-0008).
//!
//! The sandbox hosts a restricted cljrs interpreter on a dedicated worker
//! thread (cljrs isolates own per-thread heaps, so the environment, compile
//! cache, and every value stay confined there). Requests and results cross
//! the thread boundary as plain boundary EDN.
//!
//! Resource discipline, resolving the M5 risk checkpoint from
//! `docs/design/clojurust-integration.md`:
//!
//! - **Fuel**: `cljrs-interp` routes every function application (user fns,
//!   builtin higher-order iteration, recursion) through the pluggable
//!   `call_cljrs_fn` hook on `GlobalEnv`; the sandbox installs a hook that
//!   charges one fuel unit per call and aborts on exhaustion.
//! - **Allocation cap**: the same hook compares the isolate heap's
//!   cumulative allocation counter against the per-invocation cap.
//! - **Watchdog deadline**: special-form-only loops (`loop`/`recur` with no
//!   function calls) never cross the hook, so the caller waits on the reply
//!   channel with a timeout; on expiry the worker is abandoned and replaced,
//!   and the invocation fails cleanly.
//!
//! Namespace restriction removes I/O, interop escape, mutable state,
//! nondeterminism, and namespace manipulation from `clojure.core`; a
//! compile-time form guard rejects definition and interop special forms.

use std::cell::Cell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cljrs_env::env::{Env, GlobalEnv};
use cljrs_env::error::{EvalError, EvalResult};
use cljrs_reader::Form;
use cljrs_reader::form::FormKind;
use cljrs_value::{CljxFn, Value};
use corium_db::Db;
use corium_query::edn::Edn;
use thiserror::Error;

use crate::convert;

/// Per-invocation resource budget (transactor configuration).
#[derive(Clone, Copy, Debug)]
pub struct SandboxBudget {
    /// Function applications the invocation may perform.
    pub fuel: u64,
    /// Bytes the invocation may allocate on the isolate heap.
    pub max_alloc_bytes: u64,
    /// Nested function applications the invocation may stack (the
    /// tree-walking interpreter consumes Rust stack per frame, so this must
    /// stay well inside [`WORKER_STACK_BYTES`]).
    pub max_call_depth: u64,
    /// Wall-clock backstop; expiry abandons the worker thread.
    pub deadline: Duration,
}

impl Default for SandboxBudget {
    fn default() -> Self {
        Self {
            fuel: 1_000_000,
            max_alloc_bytes: 64 * 1024 * 1024,
            max_call_depth: 2_000,
            deadline: Duration::from_secs(5),
        }
    }
}

/// Worker thread stack size: sized so a `max_call_depth` chain of
/// interpreter frames cannot overflow the thread.
pub const WORKER_STACK_BYTES: usize = 64 * 1024 * 1024;

/// Sandbox failure. Every variant aborts the invoking transaction cleanly.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SandboxError {
    /// The source failed the compile-time form guard.
    #[error("sandbox rejected form: {0}")]
    Rejected(String),
    /// The source did not compile to a function.
    #[error("db function did not compile: {0}")]
    Compile(String),
    /// Evaluation failed (including fuel and allocation exhaustion).
    #[error("db function failed: {0}")]
    Eval(String),
    /// The fuel budget ran out.
    #[error("db function fuel exhausted")]
    FuelExhausted,
    /// The allocation cap was exceeded.
    #[error("db function allocation budget exceeded")]
    AllocExceeded,
    /// The call-depth cap was exceeded (protects the worker stack).
    #[error("db function call depth exceeded")]
    DepthExceeded,
    /// The wall-clock deadline passed; the worker was abandoned.
    #[error("db function deadline exceeded")]
    Deadline,
    /// The result could not cross the boundary.
    #[error(transparent)]
    Convert(#[from] convert::ConvertError),
}

const FUEL_MSG: &str = "corium sandbox: fuel exhausted";
const ALLOC_MSG: &str = "corium sandbox: allocation budget exceeded";
const DEADLINE_MSG: &str = "corium sandbox: deadline exceeded";
const DEPTH_MSG: &str = "corium sandbox: call depth exceeded";

#[derive(Clone, Copy)]
struct BudgetState {
    remaining: u64,
    alloc_start: u64,
    max_alloc: u64,
    max_depth: u64,
    depth: u64,
    deadline: Instant,
}

thread_local! {
    static BUDGET: Cell<Option<BudgetState>> = const { Cell::new(None) };
}

/// The pluggable function-application hook: charges fuel, enforces the
/// allocation cap, call depth, and deadline, then delegates to the
/// tree-walking apply. (The signature — including the error size — is
/// fixed by `GlobalEnv`.)
#[allow(clippy::result_large_err)]
fn hook_call(f: &CljxFn, args: &[Value], env: &mut Env) -> EvalResult {
    let Some(mut budget) = BUDGET.with(Cell::get) else {
        return cljrs_interp::apply::call_cljrs_fn(f, args, env);
    };
    if budget.remaining == 0 {
        return Err(EvalError::Runtime(FUEL_MSG.into()));
    }
    budget.remaining -= 1;
    // Depth guards the worker's Rust stack: each interpreted call consumes
    // real stack frames, and an overflow would abort the whole process.
    if budget.depth >= budget.max_depth {
        return Err(EvalError::Runtime(DEPTH_MSG.into()));
    }
    if Instant::now() >= budget.deadline {
        return Err(EvalError::Runtime(DEADLINE_MSG.into()));
    }
    let allocated = heap_allocated().saturating_sub(budget.alloc_start);
    if allocated > budget.max_alloc {
        return Err(EvalError::Runtime(ALLOC_MSG.into()));
    }
    budget.depth += 1;
    BUDGET.with(|cell| cell.set(Some(budget)));
    let result = cljrs_interp::apply::call_cljrs_fn(f, args, env);
    BUDGET.with(|cell| {
        if let Some(mut current) = cell.get() {
            current.depth = current.depth.saturating_sub(1);
            cell.set(Some(current));
        }
    });
    result
}

fn heap_allocated() -> u64 {
    u64::try_from(cljrs_gc::HEAP.total_allocated()).unwrap_or(u64::MAX)
}

/// Vars removed from the sandbox environment: I/O, time, randomness,
/// mutable state, concurrency, namespace/var manipulation, printing, and
/// definition macros. What remains is the pure `clojure.core` subset from
/// the design document.
const DENIED_VARS: &[&str] = &[
    // I/O and the outside world.
    "slurp",
    "spit",
    "load-file",
    "load",
    "resource",
    "read-string",
    "require",
    "alias",
    // Printing (an output side channel).
    "print",
    "println",
    "prn",
    "pr",
    "printf",
    "newline",
    "flush",
    "with-out-str",
    "tap>",
    "add-tap",
    "remove-tap",
    "doc",
    // Time and randomness.
    "sleep",
    "nanotime",
    "rand",
    "rand-int",
    "random-sample",
    "random-uuid",
    "shuffle",
    // Mutable state and concurrency.
    "atom",
    "swap!",
    "reset!",
    "compare-and-set!",
    "add-watch",
    "remove-watch",
    "volatile!",
    "vreset!",
    "vswap!",
    "shared-atom",
    "agent",
    "send",
    "send-off",
    "future",
    "promise",
    "deliver",
    // Namespace and var escape hatches.
    "intern",
    "create-ns",
    "remove-ns",
    "in-ns",
    "all-ns",
    "find-ns",
    "the-ns",
    "ns-resolve",
    "ns-map",
    "ns-interns",
    "ns-publics",
    "ns-refers",
    "ns-aliases",
    "resolve",
    "var-get",
    "var-set!",
    "alter-var-root",
    "alter-meta!",
    "with-bindings*",
    "bound-fn*",
    // Definition forms (db functions are single expressions).
    "defn",
    "defmacro",
    "defonce",
    "defmulti",
    "defmethod",
    "defprotocol",
    "defrecord",
    "deftype",
    "extend-type",
    "extend-protocol",
    "prefer-method",
    "remove-method",
];

/// List heads rejected by the compile-time form guard: definitions, var
/// access, namespace manipulation, and Rust interop instantiation.
const DENIED_HEADS: &[&str] = &[
    "def",
    "set!",
    "ns",
    "in-ns",
    "require",
    "use",
    "import",
    "load",
    "load-file",
    "new",
    "var",
    "monitor-enter",
    "monitor-exit",
];

/// Walks a parsed form, rejecting constructs the sandbox forbids
/// syntactically (they are special forms, so namespace pruning cannot
/// remove them). Quoted subtrees are data — nothing in the sandbox can
/// evaluate them later (`eval`/`read-string` are absent) — so code checks
/// apply only outside quotes, flipping back on inside `~`/`~@`.
fn guard_form(form: &Form, quoted: bool) -> Result<(), SandboxError> {
    let reject = |what: &str| Err(SandboxError::Rejected(what.to_owned()));
    match &form.kind {
        FormKind::Var(_) if !quoted => reject("var access"),
        FormKind::List(items) => {
            if !quoted
                && let Some(Form {
                    kind: FormKind::Symbol(head),
                    ..
                }) = items.first()
            {
                if DENIED_HEADS.contains(&head.as_str()) {
                    return reject(head);
                }
                if head.starts_with('.') {
                    return reject("interop method call");
                }
            }
            items.iter().try_for_each(|item| guard_form(item, quoted))
        }
        FormKind::Vector(items) | FormKind::Map(items) | FormKind::Set(items) => {
            items.iter().try_for_each(|item| guard_form(item, quoted))
        }
        FormKind::Quote(inner) | FormKind::SyntaxQuote(inner) => guard_form(inner, true),
        FormKind::Unquote(inner) | FormKind::UnquoteSplice(inner) => guard_form(inner, false),
        FormKind::Var(inner) | FormKind::Deref(inner) | FormKind::TaggedLiteral(_, inner) => {
            guard_form(inner, quoted)
        }
        FormKind::Meta(meta, inner) => {
            guard_form(meta, quoted)?;
            guard_form(inner, quoted)
        }
        FormKind::AnonFn(items) => items.iter().try_for_each(|item| guard_form(item, quoted)),
        _ => Ok(()),
    }
}

/// Namespace holding compiled database functions so the GC can reach them
/// (values stored only in Rust collections would be unrooted).
const CACHE_NS: &str = "corium.sandbox-cache";

/// Builds the restricted global environment for one sandbox isolate.
fn restricted_env() -> Arc<GlobalEnv> {
    let globals = cljrs_interp::standard_env_minimal(None, Some(hook_call), None);
    for ns_name in ["clojure.core", "user", "corium.api"] {
        let ns = globals.get_or_create_ns(ns_name);
        ns.get()
            .interns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|name, _| !DENIED_VARS.contains(&name.as_ref()));
        ns.get()
            .refers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|name, _| !DENIED_VARS.contains(&name.as_ref()));
    }
    globals.get_or_create_ns(CACHE_NS);
    crate::api::register_read_api(&globals);
    globals.refer_all("user", "corium.api");
    globals
}

struct Request {
    source: Arc<str>,
    db: Option<Db>,
    args: Vec<Edn>,
    budget: SandboxBudget,
    reply: mpsc::SyncSender<Result<Edn, SandboxError>>,
}

/// A sandbox host. Owns (and replaces, after a deadline abandonment) the
/// worker thread hosting the restricted interpreter and its compile cache.
pub struct Sandbox {
    worker: Mutex<Option<mpsc::Sender<Request>>>,
    abandoned: AtomicBool,
}

impl Default for Sandbox {
    fn default() -> Self {
        Self::new()
    }
}

impl Sandbox {
    /// Creates a sandbox host; the worker thread starts lazily.
    #[must_use]
    pub fn new() -> Self {
        Self {
            worker: Mutex::new(None),
            abandoned: AtomicBool::new(false),
        }
    }

    /// Whether a previous invocation blew its deadline and abandoned a
    /// worker thread (surfaced for operator telemetry).
    #[must_use]
    pub fn abandoned_worker(&self) -> bool {
        self.abandoned.load(Ordering::Relaxed)
    }

    /// Compiles (or reuses) `source` and invokes it with `db` prepended to
    /// `args`, all inside the sandbox isolate. Blocks the calling thread up
    /// to the budget's deadline (plus scheduling grace).
    ///
    /// # Errors
    /// Returns [`SandboxError`] when the form is rejected, does not compile
    /// to a function, fails, or exceeds its budget. The sandbox stays
    /// usable after every error.
    pub fn invoke(
        &self,
        source: &str,
        db: Option<Db>,
        args: Vec<Edn>,
        budget: SandboxBudget,
    ) -> Result<Edn, SandboxError> {
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sender = if let Some(sender) = worker.as_ref() {
            sender.clone()
        } else {
            let sender = spawn_worker();
            *worker = Some(sender.clone());
            sender
        };
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        let request = Request {
            source: Arc::from(source),
            db,
            args,
            budget,
            reply: reply_tx,
        };
        if sender.send(request).is_err() {
            // The worker died (panic); replace it and report cleanly.
            *worker = None;
            return Err(SandboxError::Eval("sandbox worker stopped".into()));
        }
        // Grace on top of the in-interpreter deadline so hook-detected
        // expiry (which replies with a precise error) wins when possible.
        if let Ok(result) = reply_rx.recv_timeout(budget.deadline + Duration::from_millis(250)) {
            result
        } else {
            // Special-form-only loop: no hook crossing, no reply. The
            // worker (and its compile cache) is abandoned; the next
            // invocation gets a fresh isolate.
            *worker = None;
            self.abandoned.store(true, Ordering::Relaxed);
            Err(SandboxError::Deadline)
        }
    }
}

fn spawn_worker() -> mpsc::Sender<Request> {
    let (tx, rx) = mpsc::channel::<Request>();
    std::thread::Builder::new()
        .name("corium-sandbox".into())
        .stack_size(WORKER_STACK_BYTES)
        .spawn(move || {
            let _mutator = cljrs_gc::register_mutator();
            let globals = restricted_env();
            while let Ok(request) = rx.recv() {
                let result = serve(&globals, &request);
                let _ = request.reply.send(result);
            }
        })
        .expect("spawn sandbox worker");
    tx
}

fn map_eval_error(error: &EvalError) -> SandboxError {
    let text = format!("{error:?}");
    if text.contains(FUEL_MSG) {
        SandboxError::FuelExhausted
    } else if text.contains(ALLOC_MSG) {
        SandboxError::AllocExceeded
    } else if text.contains(DEPTH_MSG) {
        SandboxError::DepthExceeded
    } else if text.contains(DEADLINE_MSG) {
        SandboxError::Deadline
    } else {
        SandboxError::Eval(text)
    }
}

fn serve(globals: &Arc<GlobalEnv>, request: &Request) -> Result<Edn, SandboxError> {
    let _alloc_frame = cljrs_gc::push_alloc_frame();
    let function = compiled(globals, &request.source, request.budget)?;
    let mut args = Vec::with_capacity(request.args.len() + 1);
    if let Some(db) = &request.db {
        args.push(crate::api::db_value(db.clone()));
    }
    args.extend(request.args.iter().map(convert::from_edn));
    let Value::Fn(f) = &function else {
        return Err(SandboxError::Compile(
            "db function source must evaluate to a (fn ...) form".into(),
        ));
    };
    let mut env = Env::new(Arc::clone(globals), "user");
    let _roots = cljrs_env::gc_roots::root_values(&args);
    let guard = install_budget(request.budget);
    let outcome = globals.call_cljrs_fn(f.get(), &args, &mut env);
    // Convert under the same budget: realizing lazy results re-enters the
    // interpreter through the hook.
    let converted = outcome
        .map_err(|error| map_eval_error(&error))
        .and_then(|value| convert::to_edn(&value).map_err(SandboxError::from));
    drop(guard);
    converted
}

struct BudgetGuard;

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        BUDGET.with(|cell| cell.set(None));
    }
}

fn install_budget(budget: SandboxBudget) -> BudgetGuard {
    BUDGET.with(|cell| {
        cell.set(Some(BudgetState {
            remaining: budget.fuel,
            alloc_start: heap_allocated(),
            max_alloc: budget.max_alloc_bytes,
            max_depth: budget.max_call_depth,
            depth: 0,
            deadline: Instant::now() + budget.deadline,
        }));
    });
    BudgetGuard
}

/// Compiles `source` under the budget, caching the result in a
/// GC-reachable namespace keyed by source hash.
fn compiled(
    globals: &Arc<GlobalEnv>,
    source: &str,
    budget: SandboxBudget,
) -> Result<Value, SandboxError> {
    let mut hasher = DefaultHasher::new();
    source.hash(&mut hasher);
    let key = format!("fn-{:016x}", hasher.finish());
    if let Some(cached) = globals.lookup_in_ns(CACHE_NS, &key) {
        return Ok(cached);
    }
    let mut parser = cljrs_reader::Parser::new(source.to_owned(), "<db-fn>".to_owned());
    let forms = parser
        .parse_all()
        .map_err(|error| SandboxError::Compile(format!("{error:?}")))?;
    let [form] = forms.as_slice() else {
        return Err(SandboxError::Compile(
            "db function source must be a single form".into(),
        ));
    };
    guard_form(form, false)?;
    let mut env = Env::new(Arc::clone(globals), "user");
    let guard = install_budget(budget);
    let value = cljrs_interp::eval::eval(form, &mut env);
    drop(guard);
    let value = value.map_err(|error| map_eval_error(&error))?;
    if !matches!(value, Value::Fn(_)) {
        return Err(SandboxError::Compile(
            "db function source must evaluate to a (fn ...) form".into(),
        ));
    }
    globals.intern(CACHE_NS, key.into(), value.clone());
    Ok(value)
}
