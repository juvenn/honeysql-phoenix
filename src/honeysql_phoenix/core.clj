(ns honeysql-phoenix.core
  (:require [honeysql.format :as fmt]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [defhelper]]))

(defmethod fmt/format-clause :upsert-into [[op table] sqlmap]
  (str "UPSERT INTO " (fmt/to-sql table)))

(fmt/register-clause! :upsert-into 45)

;; Change :columns priority so it always follows table definition
(fmt/register-clause! :columns 115)

(defhelper upsert-into [sqlmap args]
  (assoc sqlmap :upsert-into (if (sequential? args) (first args) args)))
