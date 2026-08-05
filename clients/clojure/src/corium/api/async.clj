(ns corium.api.async
  "Asynchronous JVM Clojure API for Corium.

  Operations backed by CompletableFuture return a one-shot core.async channel.
  A successful channel yields its value; a failed channel yields the Throwable.
  Successful nil results close without yielding a value. Use `<?` in a go block
  to rethrow failures.

  Boundary values are recursively translated between ordinary Clojure data and
  the Java client's Keyword, Symbol, EdnList, and Tagged representations."
  (:refer-clojure :exclude [sync])
  (:require [clojure.core.async :as async])
  (:import (clojure.lang TaggedLiteral)
           (dev.corium.client Datom Db DbStats DirectStorage EdnList
                              Index Keyword LocalPeer LocalPeer$Builder Peer
                              QueryResult RemotePeer RemotePeer$Builder
                              SegmentCache Symbol Tagged TxReport)
           (java.nio.file Path Paths)
           (java.time Instant)
           (java.util ArrayList LinkedHashMap LinkedHashSet List Locale Map Set)
           (java.util.concurrent CompletableFuture CompletionException
                                 ExecutionException)
           (java.util.function BiConsumer)))

(defn error?
  "Returns true when `value` is an error delivered by a result channel."
  [value]
  (instance? Throwable value))

(defmacro <?
  "Takes from `channel` in a go block and throws a delivered error."
  [channel]
  `(let [value# (async/<! ~channel)]
     (if (error? value#)
       (throw value#)
       value#)))

(defn- keyword-name
  [value]
  (if-let [ns (namespace value)]
    (str ns "/" (name value))
    (name value)))

(declare ->java ->clj)

(defn- map-values
  [f values]
  (let [result (LinkedHashMap.)]
    (doseq [[key value] values]
      (.put result (f key) (f value)))
    result))

(defn- list-values
  [f values]
  (let [result (ArrayList.)]
    (doseq [value values]
      (.add result (f value)))
    result))

(defn- set-values
  [f values]
  (let [result (LinkedHashSet.)]
    (doseq [value values]
      (.add result (f value)))
    result))

(defn- ->java
  [value]
  (cond
    (keyword? value) (Keyword. (keyword-name value))
    (symbol? value) (Symbol. (str value))
    (instance? TaggedLiteral value)
    (let [^TaggedLiteral tagged value]
      (Tagged. (str (.-tag tagged)) (->java (.-form tagged))))
    (list? value) (EdnList. (list-values ->java value))
    (map? value) (map-values ->java value)
    (set? value) (set-values ->java value)
    (sequential? value) (list-values ->java value)
    :else value))

(defn- query-result->map
  [^QueryResult result]
  {:shape (keyword (.toLowerCase (.name (.shape result)) Locale/ROOT))
   :value (->clj (.value result))})

(defn- tx-report->map
  [^TxReport report]
  {:basis-before (.basisBefore report)
   :basis-t (.basisT report)
   :tx-instant (.transactionInstant report)
   :tempids (into {} (.tempids report))
   :db-after (.dbAfter report)})

(defn- stats->map
  [^DbStats stats]
  {:basis-t (.basisT stats)
   :datoms (.datoms stats)
   :entities (.entities stats)
   :attributes (.attributes stats)})

(defn- datom->map
  [^Datom datom]
  {:e (.entity datom)
   :a (.attribute datom)
   :v (->clj (.value datom))
   :tx (.transaction datom)
   :added? (.added datom)})

(defn- ->clj
  [value]
  (cond
    (instance? Keyword value) (keyword (.name ^Keyword value))
    (instance? Symbol value) (symbol (.name ^Symbol value))
    (instance? EdnList value) (apply list (map ->clj (.items ^EdnList value)))
    (instance? Tagged value)
    (TaggedLiteral/create (symbol (.tag ^Tagged value)) (->clj (.value ^Tagged value)))
    (instance? QueryResult value) (query-result->map value)
    (instance? TxReport value) (tx-report->map value)
    (instance? DbStats value) (stats->map value)
    (instance? Datom value) (datom->map value)
    (instance? Map value)
    (persistent!
     (reduce (fn [result [key item]]
               (assoc! result (->clj key) (->clj item)))
             (transient {}) value))
    (instance? List value) (mapv ->clj value)
    (instance? Set value) (into #{} (map ->clj) value)
    :else value))

(defn- unwrap-completion-error
  [error]
  (if (and (or (instance? CompletionException error)
               (instance? ExecutionException error))
           (.getCause ^Throwable error))
    (.getCause ^Throwable error)
    error))

(defn- result-chan
  ([] (async/promise-chan))
  ([value]
   (let [channel (result-chan)]
     (if (nil? value)
       (async/close! channel)
       (async/put! channel value (fn [_] (async/close! channel))))
     channel)))

(defn- failed-chan
  [error]
  (result-chan (unwrap-completion-error error)))

(defn- future->chan
  ([future]
   (future->chan future ->clj))
  ([^CompletableFuture future transform]
   (let [channel (result-chan)]
     (.whenComplete
      future
      (reify BiConsumer
        (accept [_ value error]
          (if error
            (async/put! channel (unwrap-completion-error error)
                        (fn [_] (async/close! channel)))
            (try
              (let [value (transform value)]
                (if (nil? value)
                  (async/close! channel)
                  (async/put! channel value (fn [_] (async/close! channel)))))
              (catch Throwable transform-error
                (async/put! channel transform-error
                            (fn [_] (async/close! channel)))))))))
     channel)))

(defn- required
  [config key]
  (or (get config key)
      (throw (IllegalArgumentException.
              (str "connect requires " key)))))

(defn- config-database-name
  [config]
  (or (:db-name config)
      (:database-name config)
      (:database config)
      (throw (IllegalArgumentException.
              "connect requires :db-name"))))

(defn- configure-remote
  [^RemotePeer$Builder builder config]
  (when-let [token (:token config)]
    (.token builder token))
  (when (contains? config :allow-insecure-token?)
    (.allowInsecureToken builder (boolean (:allow-insecure-token? config))))
  (when-let [tls-ca (:tls-ca config)]
    (.tlsCa builder tls-ca))
  (when-let [tls-domain (:tls-domain config)]
    (.tlsDomain builder tls-domain))
  builder)

(defn- configure-local
  [^LocalPeer$Builder builder config]
  (when-let [token (:token config)]
    (.token builder token))
  (when (contains? config :allow-insecure-token?)
    (.allowInsecureToken builder (boolean (:allow-insecure-token? config))))
  (when-let [tls-ca (:tls-ca config)]
    (.tlsCa builder tls-ca))
  (when-let [tls-domain (:tls-domain config)]
    (.tlsDomain builder tls-domain))
  (when-let [storage (:direct-storage config)]
    (.directStorage builder
                    (if (true? storage)
                      (DirectStorage/discover)
                      storage)))
  builder)

(defn connect
  "Connects to a Corium peer and returns a result channel.

  Required keys are `:peer` (`:remote` or `:local`) and `:db-name`.
  A remote peer also requires `:endpoint`. A local peer accepts either
  `:endpoint` or failover-ordered `:endpoints`.

  Both peer types accept `:token`, `:allow-insecure-token?`, `:tls-ca`, and
  `:tls-domain`. Local peers additionally accept `:direct-storage`, either
  true or a DirectStorage value."
  [config]
  (try
    (let [peer-type (or (:peer config) (:peer-type config) (:type config)
                        (throw (IllegalArgumentException.
                                "connect requires :peer")))]
      (case peer-type
        :remote
        (let [^String endpoint (required config :endpoint)
              ^String db-name (config-database-name config)
              ^RemotePeer$Builder builder (RemotePeer/builder endpoint db-name)]
          (configure-remote builder config)
          (result-chan (.build builder)))

        :local
        (let [endpoints (:endpoints config)
              ^String db-name (config-database-name config)
              ^LocalPeer$Builder builder
              (if endpoints
                (LocalPeer/builder ^List (list-values identity endpoints) db-name)
                (LocalPeer/builder ^String (required config :endpoint) db-name))]
          (configure-local builder config)
          (future->chan (.connect builder) identity))

        (failed-chan
         (IllegalArgumentException.
          (str ":peer must be :remote or :local, got " (pr-str peer-type))))))
    (catch Throwable error
      (failed-chan error))))

(defn close
  "Idempotently closes `peer`."
  [^Peer peer]
  (.close peer))

(defn database-name
  "Returns the name of the database connected to `peer` or represented by `db`."
  [peer-or-db]
  (cond
    (instance? Peer peer-or-db) (.databaseName ^Peer peer-or-db)
    (instance? Db peer-or-db) (.databaseName ^Db peer-or-db)
    :else (throw (IllegalArgumentException. "database-name requires a Peer or Db"))))

(defn database
  "Alias for `database-name`."
  [peer-or-db]
  (database-name peer-or-db))

(defn db
  "Returns a channel yielding the current immutable database value for `peer`."
  [^Peer peer]
  (future->chan (.db peer) identity))

(defn sync
  "Synchronizes `peer` and returns a channel yielding the resulting database."
  [^Peer peer]
  (future->chan (.sync peer) identity))

(defn transact
  "Submits Clojure transaction data through `peer` and yields a transaction map."
  [^Peer peer transaction-data]
  (future->chan (.transact peer (->java transaction-data))))

(defn as-of
  "Derives an as-of database view at transaction `t` or a java.time.Instant."
  [^Db db t]
  (if (instance? Instant t)
    (.asOf db ^Instant t)
    (.asOf db (long t))))

(defn since
  "Derives a since database view at transaction `t` or a java.time.Instant."
  [^Db db t]
  (if (instance? Instant t)
    (.since db ^Instant t)
    (.since db (long t))))

(defn history
  "Derives a history database view."
  [^Db db]
  (.history db))

(defn query
  "Executes a query and yields `{:shape keyword :value clojure-value}`.

  `arguments` and `additional-databases` are collections. Fuel 0 requests the
  peer default. Only RemotePeer supports joins across databases."
  ([^Db db query]
   (query db query [] 0))
  ([^Db db query arguments]
   (query db query arguments 0))
  ([^Db db query arguments fuel]
   (future->chan
    (.query db (->java query) (list-values ->java arguments) (long fuel))))
  ([^Db db query additional-databases arguments fuel]
   (future->chan
    (.query db (->java query)
            (list-values identity additional-databases)
            (list-values ->java arguments)
            (long fuel)))))

(defn q
  "Executes `query` with positional `arguments` and yields only its value."
  [^Db db query & arguments]
  (future->chan
   (.query db (->java query) (list-values ->java arguments) 0)
   (fn [^QueryResult result] (->clj (.value result)))))

(defn pull
  "Pulls `entity` using `pattern` from `db`."
  [^Db db pattern entity]
  (future->chan (.pull db (->java pattern) (->java entity))))

(defn- ->index
  [index]
  (cond
    (instance? Index index) index
    (keyword? index) (Index/valueOf (.toUpperCase ^String (name index) Locale/ROOT))
    (string? index) (Index/valueOf (.toUpperCase ^String index Locale/ROOT))
    :else (throw (IllegalArgumentException.
                  "index must be :eavt, :aevt, :avet, :vaet, a string, or Index"))))

(defn datoms
  "Scans `index`, optionally beginning with `components` and capped by `limit`.
  Limit 0 requests an unbounded scan."
  ([^Db db index]
   (datoms db index [] 0))
  ([^Db db index components]
   (datoms db index components 0))
  ([^Db db index components limit]
   (future->chan
    (.datoms db (->index index) (list-values ->java components) (long limit)))))

(defn stats
  "Returns a channel yielding database statistics as a Clojure map."
  [^Db db]
  (future->chan (.stats db)))

(defn basis-t
  "Returns a channel yielding the basis transaction of `db`."
  [^Db db]
  (future->chan (.basisT db)))

(defn segment-cache
  "Creates a SegmentCache. The optional memory capacity 0 uses the engine default."
  ([directory capacity-bytes]
   (SegmentCache. (if (instance? Path directory)
                    directory
                    (Paths/get (str directory) (make-array String 0)))
                  (long capacity-bytes)))
  ([directory capacity-bytes memory-capacity-bytes]
   (SegmentCache. (if (instance? Path directory)
                    directory
                    (Paths/get (str directory) (make-array String 0)))
                  (long capacity-bytes)
                  (long memory-capacity-bytes))))

(defn direct-storage
  "Creates direct-storage configuration, optionally using `cache`."
  ([] (DirectStorage/discover))
  ([^SegmentCache cache] (DirectStorage/discover cache)))

(defn available-storage-backends
  "Returns the storage backends installed for LocalPeer in this JVM."
  []
  (set (LocalPeer/availableStorageBackends)))
