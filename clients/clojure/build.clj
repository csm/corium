(ns build
  (:require [clojure.edn :as edn]
            [clojure.tools.build.api :as b]))

(def lib 'dev.corium/corium-clojure)
(def class-dir "target/classes")
(def jar-file "target/corium-clojure.jar")
(def basis (delay (b/create-basis {:project "deps.edn"})))

(def pom-data
  [[:description "Synchronous and core.async JVM Clojure APIs for Corium"]
   [:url "https://github.com/csm/corium"]
   [:licenses
    [:license
     [:name "Apache License, Version 2.0"]
     [:url "https://www.apache.org/licenses/LICENSE-2.0.txt"]]
    [:license
     [:name "MIT License"]
     [:url "https://opensource.org/license/mit/"]]]
   [:scm
    [:url "https://github.com/csm/corium"]
    [:connection "scm:git:https://github.com/csm/corium.git"]
    [:developerConnection "scm:git:ssh://git@github.com/csm/corium.git"]]])

(defn- version
  []
  (:version (edn/read-string (slurp "version.edn"))))

(defn clean
  [_]
  (b/delete {:path "target"}))

(defn jar
  [_]
  (let [version (version)]
    (clean nil)
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis @basis
                  :src-dirs ["src"]
                  :pom-data pom-data})
    (b/copy-dir {:src-dirs ["src"]
                 :target-dir class-dir})
    (b/jar {:class-dir class-dir
            :jar-file jar-file})
    {:jar-file jar-file
     :version version}))
