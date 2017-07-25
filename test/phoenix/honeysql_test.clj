(ns phoenix.honeysql-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as j]
            [honeysql.core :as sql]
            [phoenix.db :refer [defdb deftable]]
            [honeysql.helpers :refer :all]
            [phoenix.honeysql :refer :all]))

(defdb test-db
  {:quorum "127.0.0.1:2181"
   :zk-path "/hbase"})

(deftable test-table
  {:table :test_table
   :columns [:a :b :c]
   :dynamic {:x :integer :y "decimal(10,2)" :z "ARRAY[5]"}})

(deftest test-format-on-duplicate-key
  (is (= ["UPSERT INTO table ON DUPLICATE KEY IGNORE"]
         (sql/format {:upsert-into :table
                      :on-duplicate-key :ignore})))
  (is (= ["UPSERT INTO table ON DUPLICATE KEY UPDATE a = ?, b = ?" 1 2]
         (sql/format {:upsert-into :table
                      :on-duplicate-key {:a 1 :b 2}}))))

(deftest test-build-on-duplicate-key
  (is (= {:upsert-into :table
          :on-duplicate-key :ignore}
         (sql/build :upsert-into :table
                    :on-duplicate-key :ignore)))
  (is (= {:upsert-into :table
          :on-duplicate-key {:a 1 :b "hello"}}
         (sql/build :upsert-into :table
                    :on-duplicate-key {:a 1 :b "hello"}))))

(deftest test-on-duplicate-key-helper
  (is (= {:upsert-into :table
          :on-duplicate-key :ignore}
         (-> (upsert-into :table)
             (on-duplicate-key :ignore))))
  (is (= {:upsert-into :table
          :on-duplicate-key {:a 1 :b "hello"}}
         (-> (upsert-into :table)
             (on-duplicate-key {:a 1 :b "hello"})))))

(deftest test-format-upsert
  (is (= ["UPSERT INTO table (a, b) VALUES (?, ?), (?, ?)"
          1 2 3 4]
         (sql/format {:upsert-into :table
                      :values [{:a 1 :b 2} {:a 3 :b 4}]})))
  (is (= [(str "UPSERT INTO table (a, b varchar, c char(10), d char[5], e time)"
               "  VALUES (?, ?, ?, ?, current_time())")
          1 "hello" "hello" "hello"]
         (sql/format {:upsert-into :table
                      :columns [:a [:b :varchar] [:c "char(10)"] [:d "char[5]"]
                                [:e :time]]
                      :values [[1 "hello" "hello" "hello" :%current_time]]})))
  (testing "upsert-into table with dynamic columns"
    (is (= [(str "UPSERT INTO test_table (a, b, c, x integer, y decimal(10,2), ts time)"
                 "  VALUES (?, ?, ?, ?, ?, current_time())")
            1 "hello" "12" 42 42.10]
           (sql/format {:upsert-into test-table
                        :columns [:a :b :c :x :y [:ts :time]]
                        :values [[1 "hello" "12" 42 42.10 :%current_time]]})))))

(deftest test-format-select
  (testing "Explicitly specify column and types"
    (is (= ["SELECT id, a, b  FROM table (a float, b binary(8))"]
           (sql/format {:select [:id :a :b]
                        :from [:table]
                        :columns [[:a :float]
                                  [:b "binary(8)"]]}))))
  (testing "selected fields are type-inferred"
    (is (= [(str "SELECT a, b, y, z"
                 " FROM test_table (y integer, z ARRAY[5])"
                 " LIMIT ?")
            5]
           (sql/format {:select [:a :b :y :z]
                        :from [test-table]
                        :columns [[:y :integer]]
                        :limit 5}))))
  (testing "aliased fields are type-inferred too"
    (is (= [(str "SELECT tt.a, tt.b, y, tt.z"
                 " FROM test_table tt (y decimal(10,2), z ARRAY[5]) LIMIT ?")
            5]
           (sql/format {:select [:tt.a :tt.b :y :tt.z]
                        :from [[test-table :tt]]
                        :limit 5}))))
  (testing "sub query are type-inferred"
    (is (= [(str "SELECT a, b, y, z FROM ("
                   "SELECT a, b, y, z"
                   " FROM test_table (y decimal(10,2), z ARRAY[5]) WHERE a > ?"
                 ") LIMIT ?")
            100 5]
           (sql/format {:select [:a :b :y :z]
                        :from [{:select [:a :b :y :z]
                                :from [test-table]
                                :where [:> :a 100]}]
                        :limit 5})))))

(deftest test-build-upsert
  (is (= {:upsert-into :table
          :values [{:a 1 :b 2} {:a 3 :b 4}]}
         (sql/build :upsert-into :table
                    :values [{:a 1 :b 2} {:a 3 :b 4}])))
  (is (= {:upsert-into :table
          :columns [:a [:b :varchar] [:c "char(10)"] [:d "char[5]"] [:e :time]]
          :values [[1 "hello" "hello" "hello" :%current_time]]}
         (sql/build :upsert-into :table
                    :columns [:a [:b :varchar] [:c "char(10)"] [:d "char[5]"]
                              [:e :time]]
                    :values [[1 "hello" "hello" "hello" :%current_time]]))))

(deftest test-upsert-helper
  (is (= {:upsert-into :table
          :values [{:a 1 :b 2} {:a 3 :b 4}]}
         (-> (upsert-into :table)
             (values [{:a 1 :b 2} {:a 3 :b 4}]))))
  (is (= {:upsert-into :table
          :columns [:a [:b :float] [:c :time]]
          :values [[1 0.25 :%current_time]]}
         (-> (upsert-into :table)
             (columns :a [:b :float] [:c :time])
             (values [[1 0.25 :%current_time]])))))

(deftest test-delete-from!
  (binding [*no-op* true]
    (is (= [test-db "DELETE FROM test_table WHERE a = ?" 1]
           (delete-from! (-> (delete-from test-table)
                             (where [:= :a 1])))))

    (is (= [test-db "DELETE FROM test_table WHERE a = ?" 1]
           (delete-from! test-table
                         (where [:= :a 1]))))))

(deftest test-upsert-into!
  (binding [*no-op* true]
    (is (= [test-db
            "UPSERT INTO test_table (a, b) VALUES (?, ?), (?, ?)"
            1 2 3 4]
           (upsert-into! (-> (upsert-into test-table)
                             (values [{:a 1 :b 2} {:a 3 :b 4}])))))
    (is (= [test-db
            "UPSERT INTO test_table (a, b) VALUES (?, ?), (?, ?)"
            1 2 3 4]
           (upsert-into! test-table
                         (values [{:a 1 :b 2} {:a 3 :b 4}]))))))

(deftest test-select!
  (binding [*no-op* true]
    (is (= [test-db
            (str "SELECT a, b, y, z"
                 " FROM test_table (y decimal(10,2), z ARRAY[5])"
                 " LIMIT ?")
            5]
           (select! (-> (select :a :b :y :z)
                        (from test-table)
                        (limit 5)))))
    (is (= [test-db
            (str "SELECT a, b, y, z"
                 " FROM test_table (y decimal(10,2), z ARRAY[5])"
                 " LIMIT ?")
            5]
           (select! :a :b :y :z
                    (from test-table)
                    (limit 5))))))

(deftest test-examples
  (binding [*no-op* true]
    (is (= [test-db
            (str "UPSERT INTO test_table (a, b, c, x integer, y decimal(10,2))"
                 " VALUES (?, ?, ?, ?, ?)"
                 " ON DUPLICATE KEY UPDATE x = ?")
            1 "b1" "c1" 42 3.14 43]
           (upsert-into! test-table
                         (values [{:a 1 :b "b1" :c "c1" :x 42 :y 3.14}])
                         (on-duplicate-key {:x 43}))))
    (is (= [test-db
            (str "SELECT tt.a, tt.b, x, tt.y"
                 " FROM test_table tt (x integer, y decimal(10,2))"
                 " WHERE tt.a > ? LIMIT ?")
            42 5]
           (select! :tt.a :tt.b :x :tt.y
                    (from [test-table :tt])
                    (where [:> :tt.a 42])
                    (limit 5))))
    (is (= [test-db "DELETE FROM test_table WHERE a > ?" 42]
           (delete-from! test-table
                         (where [:> :a 42]))))))

