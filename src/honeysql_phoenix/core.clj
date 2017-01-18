(ns honeysql-phoenix.core
  (:require [honeysql.format :as fmt]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [defhelper]]))

(defmethod fmt/format-clause :upsert-into [[op table] sqlmap]
  (str "UPSERT INTO " (fmt/to-sql table)))

(fmt/register-clause! :upsert-into 45)

;; Change :columns priority so it always follows table definition
(fmt/register-clause! :columns 115)

(defhelper upsert-into [m args]
  (assoc m :upsert-into (if (sequential? args) (first args) args)))

(defmethod fmt/format-clause :on-duplicate-key [[op values] sqlmap]
  (if (= :ignore values)
    "ON DUPLICATE KEY IGNORE"
    (str "ON DUPLICATE KEY UPDATE "
         (fmt/comma-join (for [[k v] values]
                           (str (fmt/to-sql k) " = " (fmt/to-sql v)))))))

(defhelper on-duplicate-key [m args]
  (assoc m :on-duplicate-key (if (sequential? args)
                               (first args)
                               args)))
