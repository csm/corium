(ns corium.api
  "Synchronous JVM Clojure API for Corium.

  This namespace has the same operations as corium.api.async. It blocks on
  one-shot result channels and throws errors from the Java client."
  (:refer-clojure :exclude [await sync])
  (:require [clojure.core.async :as async]
            [corium.api.async :as api.async]))

(defn- await
  [channel]
  (let [value (async/<!! channel)]
    (if (api.async/error? value)
      (throw value)
      value)))

(defn connect [config] (await (api.async/connect config)))
(defn close [peer] (api.async/close peer))
(defn database-name [peer-or-db] (api.async/database-name peer-or-db))
(defn database [peer-or-db] (api.async/database peer-or-db))
(defn db [peer] (await (api.async/db peer)))
(defn sync [peer] (await (api.async/sync peer)))
(defn transact [peer transaction-data]
  (await (api.async/transact peer transaction-data)))

(defn as-of [db t] (api.async/as-of db t))
(defn since [db t] (api.async/since db t))
(defn history [db] (api.async/history db))

(defn query
  ([db query] (await (api.async/query db query)))
  ([db query arguments] (await (api.async/query db query arguments)))
  ([db query arguments fuel]
   (await (api.async/query db query arguments fuel)))
  ([db query additional-databases arguments fuel]
   (await (api.async/query db query additional-databases arguments fuel))))

(defn q [db query & arguments]
  (await (apply api.async/q db query arguments)))

(defn pull [db pattern entity]
  (await (api.async/pull db pattern entity)))

(defn datoms
  ([db index] (await (api.async/datoms db index)))
  ([db index components] (await (api.async/datoms db index components)))
  ([db index components limit]
   (await (api.async/datoms db index components limit))))

(defn stats [db] (await (api.async/stats db)))
(defn basis-t [db] (await (api.async/basis-t db)))

(defn segment-cache
  ([directory capacity-bytes]
   (api.async/segment-cache directory capacity-bytes))
  ([directory capacity-bytes memory-capacity-bytes]
   (api.async/segment-cache directory capacity-bytes memory-capacity-bytes)))

(defn direct-storage
  ([] (api.async/direct-storage))
  ([cache] (api.async/direct-storage cache)))

(defn available-storage-backends []
  (api.async/available-storage-backends))
