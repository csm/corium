(ns corium.test-runner
  (:require [clojure.test :as test]
            [corium.api-test]))

(defn -main
  [& _]
  (let [{:keys [fail error]} (test/run-tests 'corium.api-test)]
    (when (pos? (+ fail error))
      (System/exit 1))))
