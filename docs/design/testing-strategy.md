# Testing Strategy

The engine's core claims — order-preserving encoding, deterministic log fold,
immutable structural sharing, crash-only recovery — are all properties, so the
testing strategy is property-first.

## Layers

1. **Property tests (proptest)** — per crate, from M0:
   - Encoding: `encode(a).cmp(encode(b)) == a.cmp(b)` and round-trip for every
     value type; datom key composition order.
   - Index trees: after any sequence of merges, iteration equals a sorted
     model `BTreeSet`; structural sharing bounds (new segments ≤ f(changed
     keys)); segment size invariants.
   - Transaction expansion: tempid unification, upsert, cardinality-one
     retraction — checked against a naive model implementation.
   - Codec: wire ↔ segment encoding cross-consistency; cljrs value round-trip.

2. **Model-based whole-db tests** — a reference implementation of the
   database semantics as a plain `Vec<Datom>` with brute-force query
   evaluation. Random schemas, transactions, and queries (including
   as-of/since/history views) run against both engines; results must match.
   This is the main defense for the query planner: any plan the optimizer
   picks must agree with brute force.

3. **Deterministic simulation (`corium-sim`)** — the pure crates take
   abstract clock/storage/transport; the simulator drives transactor + peers
   + storage on a single thread with a seeded scheduler and fault injection:
   crash the transactor at every await point, drop/reorder tx-report
   deliveries, fail blob puts after partial upload, contend the lease.
   Invariants checked continuously: acked transactions survive any crash;
   published roots are always fully dereferenceable; peer basis never
   regresses; a deposed transactor never publishes (fencing). Seeds make
   every failure replayable.

4. **Datomic-semantics conformance suite** — a corpus of EDN test vectors
   (`tests/conformance/*.edn`): schema + tx-data + query + expected result,
   covering Datomic's documented behaviors (upsert rules, component
   retraction, as-of edge cases, pull grammar, rule recursion, aggregate
   semantics). Written by hand from the Datomic docs, plus spot-verified
   against a real Datomic where licensing permits. This corpus is also the
   thin-client protocol's conformance kit.

5. **Integration tests** — real gRPC over localhost from M4: multi-peer
   consistency, reconnect/backfill, index-basis adoption, peer server limits.

6. **Benchmarks (criterion)** — from M3: encode/decode, segment merge, tx
   pipeline throughput, query suites on a generated musicbrainz-like dataset;
   tracked per-commit to catch regressions.

## Gates

- CI: fmt, clippy (deny warnings), full test suite, a fixed-seed simulation
  battery; nightly job runs long randomized simulation and fuzzing
  (cargo-fuzz on the EDN reader boundary, codec, and query parser).
- Every milestone's acceptance criteria (roadmap.md) include its test
  deliverables; a milestone isn't done when the feature works — it's done
  when the property/model/simulation coverage for it exists.
