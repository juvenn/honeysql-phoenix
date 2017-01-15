(ns honeysql-phoenix.core-test
  (:require [clojure.test :refer :all]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all]
            [honeysql-phoenix.core :as ph]))

(deftest test-format-upsert
  (is (= (sql/format {:upsert-into :table
                      :values [{:a 1 :b 2} {:a 3 :b 4}]})
         ["UPSERT INTO table (a, b) VALUES (?, ?), (?, ?)" 1 2 3 4]))
  (is (= (sql/format {:upsert-into :table
                      :columns [:a [:b :varchar] [:c "char(10)"] [:d "char[5]"]
                                [:e :time]]
                      :values [[1 "hello" "hello" "hello" :%current_time]]})
         ["UPSERT INTO table (a, b varchar, c char(10), d char[5], e time) VALUES (?, ?, ?, ?, current_time())" 1 "hello" "hello" "hello"])))

(deftest test-format-select
  (is (= (sql/format {:select [:id :a :b]
                      :from [:table]
                      :columns [[:a :float]
                                [:b "binary(8)"]]})
         ["SELECT id, a, b FROM table (a float, b binary(8))"])))

(deftest test-build-upsert
  (is (= (sql/build :upsert-into :table
                    :values [{:a 1 :b 2} {:a 3 :b 4}])
         {:upsert-into :table
          :values [{:a 1 :b 2} {:a 3 :b 4}]}))
  (is (= (sql/build :upsert-into :table
                    :columns [:a [:b :varchar] [:c "char(10)"] [:d "char[5]"]
                              [:e :time]]
                    :values [[1 "hello" "hello" "hello" :%current_time]])
         {:upsert-into :table
          :columns [:a [:b :varchar] [:c "char(10)"] [:d "char[5]"] [:e :time]]
          :values [[1 "hello" "hello" "hello" :%current_time]]})))

(deftest test-upsert-helper
  (is (= (-> (upsert-into :table)
             (values [{:a 1 :b 2} {:a 3 :b 4}]))
         {:upsert-into :table
          :values [{:a 1 :b 2} {:a 3 :b 4}]}))
  (is (= (-> (upsert-into :table)
             (columns :a [:b :float] [:c :time])
             (values [[1 0.25 :%current_time]]))
         {:upsert-into :table
          :columns [:a [:b :float] [:c :time]]
          :values [[1 0.25 :%current_time]]})))

