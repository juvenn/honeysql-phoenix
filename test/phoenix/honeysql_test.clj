(ns phoenix.honeysql-test
  (:require [clojure.test :refer :all]
            [phoenix.honeysql :refer :all]
            [phoenix.db-test :refer [user address]]))

(deftest test-upsert-into
  (testing "Upsert map form rows should annotate type"
    (is (= [(str "UPSERT INTO user (username, email, referrer VARCHAR(64)) VALUES"
                 " (?, ?, ?),"
                 " (?, ?, ?)")
            "jack" "jack@example.net" "google.com"
            "jill" "jill@example.net" "google.com"]
           (-> (upsert-into user)
               (values [{:username "jack" :email "jack@example.net" :referrer "google.com"}
                        {:username "jill" :email "jill@example.net" :referrer "google.com"}])
               as-sql))))
  (testing "Upsert vector form rows specifying columns"
    (is (= [(str "UPSERT INTO user (username, email, referrer VARCHAR(64))  VALUES"
                 " (?, ?, ?),"
                 " (?, ?, ?)")
            "jack" "jack@example.net" "google.com"
            "jill" "jill@example.net" "google.com"]
           (-> (upsert-into user)
               (columns :username :email :referrer)
               (values [["jack" "jack@example.net" "google.com"]
                        ["jill" "jill@example.net" "google.com"]])
               as-sql))))
  (testing "User specified type is preferred over inferred"
    (is (= [(str "UPSERT INTO user (username, email, referrer CHAR(64))  VALUES"
                 " (?, ?, ?),"
                 " (?, ?, ?)")
            "jack" "jack@example.net" "google.com"
            "jill" "jill@example.net" "google.com"]
           (-> (upsert-into user)
               (columns :username :email [:referrer "CHAR(64)"])
               (values [["jack" "jack@example.net" "google.com"]
                        ["jill" "jill@example.net" "google.com"]])
               as-sql)))))

(deftest test-upsert-on-duplicate-key
  (testing "On duplicate key ignore"
    (is (= [(str "UPSERT INTO user (username, email, phonenumber)  VALUES"
                 " (?, ?, ?)"
                 " ON DUPLICATE KEY IGNORE")
            "jack" "jack@example.net" "15600001234"]
           (-> (upsert-into user)
               (columns :username :email :phonenumber)
               (values [["jack" "jack@example.net" "15600001234"]])
               (on-duplicate-key :ignore)
               as-sql))))
  (testing "On duplicate key update"
    (is (= [(str "UPSERT INTO user (username, email, phonenumber)  VALUES"
                 " (?, ?, ?)"
                 " ON DUPLICATE KEY UPDATE"
                 " username = ?, email = ?, phonenumber = ?")
            "jack"  "jack@example.net"  "15600001234"
            "jack2" "jack2@example.net" "15600001235"]
           (-> (upsert-into user)
               (columns :username :email :phonenumber)
               (values [["jack" "jack@example.net" "15600001234"]])
               (on-duplicate-key {:username "jack2"
                                  :email "jack2@example.net"
                                  :phonenumber "15600001235"})
               as-sql)))))

(deftest test-select
  (testing "No type is annotated"
    (is (= [(str "SELECT username, email FROM user")]
           (-> (select :username :email)
               (from user)
               as-sql))))
  (testing "select fields are auto-annotated"
    (is (= [(str "SELECT username, email, twitter_id AS twitter, github_id AS github"
                 " FROM user (github_id VARCHAR(64), twitter_id VARCHAR(64))")]
           (-> (select :username :email
                       [:twitter_id :twitter]
                       [:github_id  :github])
               (from user)
               as-sql))))
  (testing "Explicitly annotate types"
    (is (= [(str "SELECT username, email, twitter_id AS twitter, github_id AS github"
                 " FROM user (github_id VARCHAR(64), twitter_id CHAR(64))")]
           (-> (select :username :email
                       [:twitter_id :twitter]
                       [:github_id  :github])
               (from [user {:twitter_id "CHAR(64)"}])
               as-sql))))
  (testing "Alias table with explicit annotated types"
    (is (= [(str "SELECT username, email, twitter_id AS twitter, github_id AS github"
                 " FROM user u (github_id VARCHAR(64), twitter_id CHAR(64))")]
           (-> (select :username :email
                       [:twitter_id :twitter]
                       [:github_id  :github])
               (from [user :u {:twitter_id "CHAR(64)"}])
               as-sql))))
  (testing "Select aliased fields are annnotated"
    (is (= [(str "SELECT first_name AS name, addr.line2, addr.zipcode"
                 " FROM user, address addr (line2 CHAR(64))")]
           (-> (select [:first_name :name]
                       :addr.line2
                       :addr.zipcode)
               (from user [address :addr {:line2 "CHAR(64)"}])
               as-sql))))
  (testing "Subquery are annotated"
    (is (= [(str "SELECT referrer, user_cnt FROM ("
                 "SELECT referrer, count(user_id) AS user_cnt FROM"
                 " user (referrer VARCHAR(64))"
                 " GROUP BY referrer"
                 ") WHERE user_cnt > ?") 5]
           (-> (select :referrer :user_cnt)
               (from (-> (select :referrer [:%count.user_id :user_cnt])
                         (from user)
                         (group :referrer)))
               (where [:> :user_cnt 5])
               as-sql)))))

