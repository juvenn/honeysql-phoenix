(ns phoenix.db-test
  (:require [clojure.test :refer :all]
            [phoenix.honeysql :refer :all]
            [phoenix.db :refer [defdb deftable exec exec-raw]]))

(defdb default-db
  {:quorum "127.0.0.1:2181"
   :zk-path "/hbase"})

(deftable user
  {:table :user
   :columns [:username :email :phonenumber
             :id :address_id]
   :dynamic {:twitter_id "VARCHAR(64)"
             :github_id  "VARCHAR(64)"
             :referrer   "VARCHAR(64)"
             :landing_url "VARCHAR(64)"}})

(deftable address
  {:table :address
   :columns [:country :state :city :zipcode :line
             :id :user_id]
   :dynamic {:line2 "VARCHAR(128)"}})

(deftest test-exec-raw
  (is (= {:result 43}
         (first (exec-raw ["SELECT 42+1 AS result"]))))
  (is (= {:result 43}
         (first (exec-raw ["SELECT ? + ? AS result" 42 1]))))
  (is (= {:result -43}
         (first (exec-raw ["SELECT ? + ? AS result" 42 1]
                          :row-fn #(update-in % [:result] -)))))
  (testing "DML queries"
    ;; phoenix does not yet support stmt.getGeneratedKeys for upsert statement,
    ;; it always returns number of rows affected.
    (is (= [1]
         (exec-raw [(str "UPSERT INTO user (id, username, email, phonenumber) VALUES"
                         " (NEXT VALUE FOR user_id_seq, ?, ?, ?)")
                    "cc5767f0" "cc5767f0@example.net" "25600001234"])))
    (is (= [1]
         (exec-raw [(str "DELETE FROM user WHERE email = ?") "cc5767f0@example.net"])))))

(deftest test-exec
  (is (= [1]
         (-> (upsert-into user)
             (columns :id :username :email :referrer)
             (values [[(keyword "NEXT VALUE FOR user_id_seq"), "6e4c580b", "6e4c580b@example.net", "google.com"]])
             (on-duplicate-key :ignore)
             exec)))
  (is (= {:username "6e4c580b"
          :email "6e4c580b@example.net"
          :phonenumber nil
          :referrer "google.com"}
         (-> (select :username :email :phonenumber :referrer)
             (from user)
             (where [:= :username "6e4c580b"])
             (limit 1)
             exec
             first)))
  (is (= [1]
         (-> (delete-from user)
             (where [:= :username "6e4c580b"])
             exec)))
  (is (= nil
         (-> (select :username :email :phonenumber :referrer)
             (from user)
             (where [:= :username "6e4c580b"])
             (limit 1)
             exec
             first))))
