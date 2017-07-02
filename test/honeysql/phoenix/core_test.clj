(ns honeysql.phoenix.core-test
  (:require [clojure.test :refer :all]
            [honeysql.core :as sql]
            [honeysql.helpers :refer :all]
            [clojure.java.jdbc :as j]
            [honeysql.phoenix.core :refer :all]))

(deftable test-table
  {:table :test_table
   :columns [:a :b :c]
   :dynamic {:x :integer :y "decimal(10,2)" :z "ARRAY[5]"}})

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
         ["SELECT id, a, b FROM table (a float, b binary(8))"]))
  (testing "select from table with dynamic columns"
    (is (= (sql/format {:select [:a :b :y :z]
                        :from [test-table]
                        :limit 5})
           ["SELECT a, b, y, z FROM test_table (z ARRAY[5], y decimal(10,2)) LIMIT ?" 5]))))

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

(deftest test-format-on-duplicate-key
  (is (= (sql/format {:upsert-into :table
                      :on-duplicate-key :ignore})
         ["UPSERT INTO table ON DUPLICATE KEY IGNORE"]))
  (is (= (sql/format {:upsert-into :table
                      :on-duplicate-key {:a 1 :b 2}})
         ["UPSERT INTO table ON DUPLICATE KEY UPDATE a = ?, b = ?" 1 2])))

(deftest test-build-on-duplicate-key
  (is (= (sql/build :upsert-into :table
                    :on-duplicate-key :ignore)
         {:upsert-into :table
          :on-duplicate-key :ignore}))
  (is (= (sql/build :upsert-into :table
                    :on-duplicate-key {:a 1 :b "hello"})
         {:upsert-into :table
          :on-duplicate-key {:a 1 :b "hello"}})))

(deftest test-on-duplicate-key-helper
  (is (= (-> (upsert-into :table)
             (on-duplicate-key :ignore))
         {:upsert-into :table
          :on-duplicate-key :ignore}))
  (is (= (-> (upsert-into :table)
             (on-duplicate-key {:a 1 :b "hello"}))
         {:upsert-into :table
          :on-duplicate-key {:a 1 :b "hello"}})))

(comment
  (-> (upsert-into :table)
      (columns :a [:b :varchar] [:c "char(10)"] [:d "char[5]"] [:e :time])
      (values [[1 "b" "c" "d" :%current_time]])
      (on-duplicate-key {:a 2 :b "b+" :e :%current_time})
      sql/format))

(defdb phoenix-db {:quorum "127.0.0.1:2181"
                   :zk-path "/hbase"})

(deftable web-stat
  {:table :web_stat ;; table name in hbase, case-insensitive
   :columns [:a :b :c] ;; predefined typed columns
   :dynamic {:x :int :y "char(8)" :ts :time}})

(select! :a :b :x :y (from web-stat) (where {:a 1}))

