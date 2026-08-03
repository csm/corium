# ADR-0021: Contextual read authorization on the peer server and pgwire

**Status:** Accepted (2026-08-03). Implements the `ViewFilter` seam of
[ADR-0012](0012-optional-authn-authz.md) and the `KeyPolicy` of
[ADR-0018](0018-attribute-protection-classes.md) on the two surfaces that serve
untrusted clients. Design in [`docs/design/auth.md`](../design/auth.md) and
[`docs/design/encryption.md`](../design/encryption.md).

## Context

A client of a hosted peer server or a pgwire server saw everything the *server
process* could see. Two independent causes:

1. **No read path applied a `ViewFilter`.** The authorizer could return
   `AllowFiltered` — the ReBAC vocabulary for views and bindings existed,
   compiled, and was tested — but nothing consumed it. The peer server refused
   a filtered decision with `UNIMPLEMENTED` precisely so that it would not
   return unfiltered data. Attribute-level policy was expressible, testable,
   and unservable.
2. **Class keys belonged to the server, not the caller.** Hydration used the
   hosted connection's keyring for every request, whoever made it, so "this
   peer server can resolve the PII class key" and "every client authorized to
   query this database reads PII" were the same statement. pgwire was further
   behind still: no `Guard`, no `Principal`, one shared cleartext password, and
   every SQL client inheriting the catalog's own peer identity.

Both were documented as known limitations, pinned by scenario tests, and both
made the honest deployment advice "do not give these servers keys."

## Decision

**A read carries the caller's view and key set, resolved per request, and the
engine applies both.**

- **One value carries "who is reading."** `corium_db::read::ReadContext` holds
  an optional attribute view and an optional hydrator, and replaces the bare
  hydrator every read path already took: `ExecOptions::read`, `pull_with`,
  `Entity::with_read`, `SqlSession::with_read`. Hydration was already a
  property of the read rather than of the `Db`; policy visibility is the same
  kind of thing, so they travel together.
- **The filter is resolved once, into attribute ids.** `AttrVisibility` is
  computed when the request arrives, so the per-datom check is a set lookup. It
  also keeps the dependency edge pointing one way: `ViewFilter` lives in
  `corium-protocol`, which depends on `corium-query`, so the engine cannot name
  it. An id is only meaningful against a schema, and an attribute missing from
  the schema a view was resolved against would be *visible* — so a request
  binding several sources resolves against all of them and hides the union.
  Every view of one database happens to share its schema today, but schema is
  itself basis-versioned data, so that is not a property to depend on.
- **Policy decides both halves in one place.** `Decision::AllowFiltered` now
  carries a `ReadGrant { view, keys }`. A view definition may name attributes,
  key ids (`:authz.view/key`), or both — `:authz.view/filter-type` became
  optional — and both halves combine across successful paths by *intersection*,
  so holding one more relation can never reveal more than holding it alone.
- **Hidden means hidden, not blank.** A datom on a hidden attribute is dropped
  in the scan, so it never binds, never joins, and never satisfies a predicate.
  A pull omits the key; SQL reports `NULL` and refuses predicate pushdown on
  the column. The direct-index paths are closed with it: `get-else` falls to
  its default, `missing?` reports missing, a lookup ref does not resolve,
  reverse-ref traversal returns nothing, `Entity::keys` omits the attribute.
  Each of those is an existence oracle over exactly the attribute the view
  withholds, and closing the scan alone would have left the filter decorative.
- **Key policy has a mode, and its default is derived from the guard.**
  `strict` grants only the key ids a decision names; `server-wide` grants the
  process's whole keyring. The default is `strict` when a `Guard` is configured
  and `server-wide` when authorization is disabled. A surface then intersects
  the granted ids with the keyring it actually holds
  (`ClassKeys::restrict_to`), so policy can never grant a key the process does
  not have.
- **A SQL client is a Corium principal.** `PostgreSQL` has no bearer-token
  field, so the password carries the caller's credential and is verified by the
  same `IdentityProvider` the gRPC surfaces use; the startup `user` is
  informational. Every statement is authorized as the resulting principal,
  `SHOW DATABASES` is filtered to what it may inspect, and `DbCatalog::hydrator`
  is the seam supplying the process's class keys.
- **Surfaces that cannot filter fail closed.** The peer server refuses
  `Transact` and `Subscribe`, and SQL refuses DML, for a principal whose view
  hides attributes: transaction data is opaque bytes by the time authorization
  runs, and `Subscribe` proxies the transactor's own stream. A grant that
  restricts only *keys* is refused nowhere — it hides no attribute, and the
  transactor holds no class key, so what it streams is sealed already.

## Consequences

- Two principals query one hosted `Db` concurrently and get different answers.
  That was the point of binding identity to the request rather than the
  connection, and it is now true of the peer server and of pgwire.
- **A key-holding peer server is deployable in front of mixed clients.** The
  old advice — give a peer server class keys only when every client it serves
  is entitled to every class — is replaced by a policy statement. This is
  enforcement by the process holding the plaintext, so it is authorization, not
  cryptography; the cryptographic floor is still a process that was never given
  the key, and `--seal-through` remains unimplemented.
- **This changes behaviour for existing guarded, key-holding deployments.**
  Under the derived `strict` default, a principal whose policy names no key id
  stops seeing protected plaintext and starts seeing redactions. That is the
  safe direction and it is loud, but it is a change, and an operator who wants
  the old behaviour sets `server-wide` deliberately.
- `:authz.binding/unfiltered` grants full *attribute* visibility and no keys,
  because keys are named by key id and that binding names none. A relation that
  must read protected values names them. This is the one genuinely surprising
  corner of the model, and the alternative — letting `unfiltered` reopen every
  class — would defeat strict mode with a single binding.
- SQL materializes only projected columns, and builds `corium_sys.datoms` when
  it is scanned rather than when a session opens. Both are performance wins,
  but the reason they are load-bearing is availability: a session is
  constructed per statement, so an eager read of a class whose missing-key
  policy is `error` would fail *every* statement for a keyless principal,
  including statements touching nothing protected. A `Refuse` must reach only
  a read that genuinely touches the datom, which is what the datalog engine
  already did.
- pgwire does not terminate TLS, and now carries each client's bearer token in
  the `PostgreSQL` password field. It rejects the TLS flags rather than
  accepting ones it cannot honour, and warns at startup; a deployment needs a
  terminating proxy or a loopback bind. Native TLS for that listener is
  follow-up work.
- A view-restricted principal cannot write on any surface. That is stricter
  than necessary for a view that touches nothing the write touches, and lifting
  it needs per-attribute write authorization in `corium-tx` rather than a check
  at the edge.
- `Subscribe` refuses a view-restricted principal rather than filtering.
  Filtered re-streaming means decoding, filtering, and re-encoding every tx
  report, and changing the proxied `tonic::Streaming` response into a boxed
  stream; it is deferred, not designed away.
- The blast radius is one field on `ExecOptions`, one on `Decision`, and a
  handful of signatures. It is deliberately small: making every read path take
  a `ReadContext` means a future entity- or value-level filter has somewhere to
  live, and a surface that forgets to build one gets the unrestricted default
  loudly rather than silently — the type is the same either way, so the escape
  is a deliberate `ReadContext::open()`, which greps.
