(ns phoenix.db-test
  (:require [clojure.test :refer :all]
            [phoenix.honeysql :refer :all]
            [phoenix.db :refer [defdb deftable phoenix]
             :as db])
  (:import [phoenix.db Table]))

(defdb default-db
  (phoenix {:zk-quorum "127.0.0.1:2181"}))

(deftest test-deftable
  (deftable my_table)
  (is (= (Table. default-db :my_table)
         my_table))
  (deftable my-table
    (db/table* :my_table))
  (is (= (Table. default-db :my_table)
         my-table))

  (deftable my-table
    (db/table* :my_table)
    (db/types* :field_a "CHAR(64)"
               :field_b "VARCHAR(64)"))
  (is (= {:field_a "CHAR(64)"
          :field_b "VARCHAR(64)"}
         (db/types* my-table))))

(deftable user
  (db/types* :twitter_id "VARCHAR(64)"
             :github_id  "VARCHAR(64)"
             :referrer   "VARCHAR(64)"
             :landing_url "VARCHAR(64)"))

(deftable address
  (db/types* :line2 "VARCHAR(128)"))

(deftest test-exec-raw
  (is (= {:result 43}
         (first (db/exec-raw ["SELECT 42+1 AS result"]))))
  (is (= {:result 43}
         (first (db/exec-raw ["SELECT ? + ? AS result" 42 1]))))
  (is (= {:result -43}
         (first (db/exec-raw ["SELECT ? + ? AS result" 42 1]
                          :row-fn #(update-in % [:result] -)))))
  (testing "DML queries"
    ;; phoenix does not yet support stmt.getGeneratedKeys for upsert statement,
    ;; it always returns number of rows affected.
    (is (= [1]
         (db/exec-raw [(str "UPSERT INTO user (id, username, email, phonenumber) VALUES"
                         " (NEXT VALUE FOR user_id_seq, ?, ?, ?)")
                    "cc5767f0" "cc5767f0@example.net" "25600001234"])))
    (is (= [1]
         (db/exec-raw [(str "DELETE FROM user WHERE email = ?") "cc5767f0@example.net"])))))

(deftest test-exec
  (is (= [1]
         (-> (upsert-into user)
             (columns :id :username :email :referrer)
             (values [[(keyword "NEXT VALUE FOR user_id_seq"), "6e4c580b", "6e4c580b@example.net", "google.com"]])
             (on-duplicate-key :ignore)
             db/exec)))
  (is (= {:username "6e4c580b"
          :email "6e4c580b@example.net"
          :phonenumber nil
          :referrer "google.com"}
         (-> (select :username :email :phonenumber :referrer)
             (from user)
             (where [:= :username "6e4c580b"])
             (limit 1)
             db/exec
             first)))
  (is (= [1]
         (-> (delete-from user)
             (where [:= :username "6e4c580b"])
             db/exec)))
  (is (= nil
         (-> (select :username :email :phonenumber :referrer)
             (from user)
             (where [:= :username "6e4c580b"])
             (limit 1)
             db/exec
             first))))
