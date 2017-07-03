(ns phoenix.honeysql-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as j]
            [honeysql.core :as sql]
            [phoenix.db :refer [defdb deftable]]
            [honeysql.helpers :refer :all]
            [phoenix.honeysql :refer :all]))

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
  (testing "Explicitly specify table columns"
    (is (= ["SELECT id, a, b  FROM table (a float, b binary(8))"]
           (sql/format {:select [:id :a :b]
                        :from [:table]
                        :columns [[:a :float]
                                  [:b "binary(8)"]]}))))
  (testing "selected dynamic columns are type-inferred"
    (is (= [(str "SELECT a, b, y, z"
                 " FROM test_table (y decimal(10,2), z ARRAY[5])"
                 " LIMIT ?")
            5]
           (sql/format {:select [:a :b :y :z]
                        :from [test-table]
                        :limit 5}))))
  (testing "aliased columns are type-inferred too"
    (is (= [(str "SELECT tt.a, tt.b, y, tt.z"
                 " FROM test_table (y decimal(10,2), z ARRAY[5]) tt LIMIT ?")
            5]
           (sql/format {:select [:tt.a :tt.b :y :tt.z]
                        :from [[test-table :tt]]
                        :limit 5}))))
  (testing "columns in sub query are type-inferred"
    (is (= [(str "SELECT a, b, y, z FROM"
                 " (SELECT a, b, y, z FROM test_table (y decimal(10,2), z ARRAY[5]) WHERE a > ?)"
                 " LIMIT ?")
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

