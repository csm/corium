(ns corium.api-test
  (:refer-clojure :exclude [sync])
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [corium.api :as api]
            [corium.api.async :as api.async])
  (:import (clojure.lang TaggedLiteral)
           (dev.corium.client DbStats EdnList Keyword QueryResult
                              QueryResult$Shape RemotePeer Symbol Tagged)
           (java.util.concurrent CompletableFuture CompletionException)))

(deftest remote-connect-and-database-views
  (let [peer (api/connect {:peer :remote
                           :endpoint "http://127.0.0.1:1"
                           :db-name "people"})]
    (try
      (is (instance? RemotePeer peer))
      (is (= "people" (api/database-name peer)))
      (is (= "people" (api/database peer)))
      (let [db (api/db peer)]
        (is (= "people" (api/database db)))
        (is (= "people" (api/database (api/as-of db 10))))
        (is (= "people" (api/database (api/since db 10))))
        (is (= "people" (api/database (api/history db)))))
      (finally
        (api/close peer)))))

(deftest connect-validation-is-delivered-and-rethrown
  (let [error (async/<!! (api.async/connect {:peer :sideways
                                              :db-name "people"}))]
    (is (api.async/error? error))
    (is (thrown-with-msg? IllegalArgumentException
                          #":peer must be :remote or :local"
                          (api/connect {:peer :sideways
                                        :db-name "people"})))))

(deftest boundary-values-round-trip
  (let [to-java @#'api.async/->java
        to-clj @#'api.async/->clj
        value {:person/name 'person/name
               :query [(list '> 'x 4)]
               :tags #{:a :b}
               :tagged (TaggedLiteral/create 'corium/example {:x :y})}
        encoded (to-java value)]
    (is (instance? java.util.Map encoded))
    (is (instance? Keyword (first (.keySet encoded))))
    (is (= value (to-clj encoded)))
    (is (instance? EdnList
                   (first (get (into {} encoded)
                               (Keyword. "query")))))))

(deftest java-results-become-clojure-data
  (let [to-clj @#'api.async/->clj]
    (is (= {:shape :collection :value [:ada :grace]}
           (to-clj (QueryResult.
                    QueryResult$Shape/COLLECTION
                    [(Keyword. "ada") (Keyword. "grace")]))))
    (is (= {:basis-t 12 :datoms 20 :entities 4 :attributes 3}
           (to-clj (DbStats. 12 20 4 3))))
    (is (= '(hello :world)
           (to-clj (EdnList. [(Symbol. "hello")
                              (Keyword. "world")]))))
    (is (= (TaggedLiteral/create 'example/value [:ok])
           (to-clj (Tagged. "example/value"
                            [(Keyword. "ok")]))))))

(deftest future-errors-are-unwrapped
  (let [future->chan @#'api.async/future->chan
        future (CompletableFuture.)
        cause (IllegalStateException. "broken")]
    (.completeExceptionally future (CompletionException. cause))
    (is (identical? cause (async/<!! (future->chan future))))))
