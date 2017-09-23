(ns phoenix.avatica-test
  (:require [clojure.test :refer :all]
            [phoenix.db :refer [defdb avatica] :as db]))

(defdb avatica-db
  (avatica {:url "http://127.0.0.1:8765"
            :serialization :protobuf}))

(deftest test-avatica-db
  (is (= {:result 43}
         (first (db/exec-raw ["SELECT 42 + 1 AS result"])))))
