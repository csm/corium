# Corium Clojure client

The JVM Clojure client wraps `dev.corium/corium-client` 0.1.81. It is a
`deps.edn` project with two APIs:

- `corium.api.async` returns one-shot `clojure.core.async` channels for Java
  `CompletableFuture` operations.
- `corium.api` blocks on those channels and returns values or throws errors.

Both namespaces use top-level functions whose first argument is a `Peer` or
`Db`. Clojure boundary data is translated recursively, so query and transaction
forms can use ordinary keywords, symbols, lists, vectors, sets, and maps.

Releases use this Clojars coordinate:

```clojure
dev.corium/corium-clojure {:mvn/version "VERSION"}
```

## Remote peer

```clojure
(require '[corium.api :as corium])

(def peer
  (corium/connect {:peer :remote
                   :endpoint "http://127.0.0.1:4336"
                   :db-name "people"}))

(corium/transact peer
  [{:db/id "ada" :person/name "Ada"}])

(def database (corium/db peer))

(corium/q database
  '[:find [?name ...]
    :where [?entity :person/name ?name]])

(corium/close peer)
```

`query` retains the Java result shape and returns a map such as
`{:shape :collection :value [...]}`. The `q` convenience returns only the
value.

## Async API

Async operations deliver one value and close. Errors are delivered as
`Throwable` values. Inside a `go` block, `<?` takes a result and rethrows an
error:

```clojure
(require '[clojure.core.async :as a]
         '[corium.api.async :as corium])

(a/go
  (let [peer (corium/<? (corium/connect
                          {:peer :remote
                           :endpoint "http://127.0.0.1:4336"
                           :db-name "people"}))
        database (corium/<? (corium/db peer))]
    (println (corium/<? (corium/q database
                                  '[:find [?e ...]
                                    :where [?e :person/name]])))))
```

A successful `nil` result closes without delivering a value, which also reads
as `nil` from a take.

## Full local peer

`LocalPeer` runs the engine in the JVM process. In addition to selecting
`:peer :local`, add the native alias matching the host platform:

```sh
clojure -A:native-macos-aarch64
```

The project provides aliases for Linux and macOS on x86-64 and AArch64, and
Windows on x86-64. If an application depends on this library, add the matching
Maven classifier directly:

```clojure
dev.corium/corium-client$linux-x86_64 {:mvn/version "0.1.81"}
```

Connect to one endpoint or provide failover endpoints:

```clojure
(def peer
  (corium/connect {:peer :local
                   :endpoints ["https://tx-a:4335"
                               "https://tx-b:4335"]
                   :db-name "people"}))
```

Set `:direct-storage true` to discover and read the transactor's storage
directly. For a bounded segment cache, pass an explicit configuration:

```clojure
(def cache (corium/segment-cache "/var/cache/corium" (* 10 1024 1024 1024)))
(def storage (corium/direct-storage cache))

(corium/connect {:peer :local
                 :endpoint "https://tx:4335"
                 :db-name "people"
                 :direct-storage storage})
```

Both peer types also accept `:token`, `:allow-insecure-token?`, `:tls-ca`
(PEM bytes), and `:tls-domain`.

## Tests

```sh
clojure -M:test
```

## Publishing

The main release workflow publishes the Clojure client after it creates the
release tag. The workflow uses the version from the same Corium release.

Add these secrets to the repository or the `clojars` environment:

- `CLOJARS_USERNAME`: the Clojars account name
- `CLOJARS_PASSWORD`: a Clojars deploy token

Clojars does not accept an account password for deployment. Use a deploy token
for `CLOJARS_PASSWORD`.

You can also run the `Publish Clojure client` workflow manually. Enter the Git
ref and the version without a `v` prefix.
