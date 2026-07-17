# Conformance corpus

EDN test vectors for Datomic-dialect semantics, written by hand from the
Datomic documentation (testing-strategy.md layer 4). The harness lives in
`crates/corium-query/tests/conformance.rs`; every vector transacts through
the real embedded transactor and queries through the full engine. This
corpus is also intended as the thin-client protocol's conformance kit (M4).

## Vector shape

```edn
{:name     "group/short-description"
 :schema   [{:db/ident :p/name :db/valueType :db.type/string
             :db/cardinality :db.cardinality/one
             :db/unique :db.unique/identity        ; optional
             :db/isComponent true :db/index true    ; optional
             :db/noHistory true}]                   ; optional
 :tx       [[{:db/id "a" :p/name "Alice"}          ; map form
             [:db/add "a" :p/age 30]]               ; list forms
            [[:db/retractEntity #tempid "a"]]]      ; one vector per tx
 :view     :history | {:as-of 1} | {:since 1}       ; optional
 :extra-dbs [:history {:as-of 1} :current]          ; optional $2.. inputs
 :query    [:find … :in … :where …]                 ; or :pull, below
 :args     […]                                      ; inputs after the dbs
 :pull     {:eid #tempid "a" :pattern [*]}          ; instead of :query
 :expected …                                        ; or :expect-error true
}
```

Conventions:

- `#tempid "name"` resolves to the entity allocated for that tempid
  (rewritten to an entity-id input in queries/args, to a raw long in
  expected results). `#tx t` names transaction `t`'s entity.
- `#inst <millis>` and `#uuid "<32 hex digits>"` denote instant and UUID
  values (a corpus-local shorthand, not standard EDN reader syntax).
- Relation/collection expectations compare order-insensitively (write sets
  `#{…}`); tuple/scalar expectations compare exactly.
- Entity references in transaction values must resolve at submission time:
  use `#tempid` or a lookup ref to an entity from a *prior* transaction
  (value-position tempids within one transaction are not yet supported by
  the transaction layer).

Documented engine deviations exercised here: `sample` is deterministic
(first n distinct values in sort order; `rand` is not provided),
`variance`/`stddev` are population statistics, `median` returns the lower
middle element, and `/` yields a double when longs do not divide evenly.
