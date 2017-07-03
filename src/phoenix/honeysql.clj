(ns phoenix.honeysql
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]
            [phoenix.db :as db]
            [honeysql.format :as fmt]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [defhelper]
             :as h])
  (:import [phoenix.db Table]))

(defhelper on-duplicate-key [m args]
  (assoc m :on-duplicate-key (if (sequential? args)
                               (first args)
                               args)))

(defhelper upsert-into [m table]
  (assoc m :upsert-into (if (sequential? table) (first table) table)))

(defmethod fmt/format-clause :on-duplicate-key [[op values] sqlmap]
  (if (= :ignore values)
    "ON DUPLICATE KEY IGNORE"
    (str "ON DUPLICATE KEY UPDATE "
         (fmt/comma-join (for [[k v] values]
                           (str (fmt/to-sql k) " = " (fmt/to-sql v)))))))

(fmt/register-clause! :on-duplicate-key 225) ;; after :values

;; Reset :columns to be no-op, which should be handled at
;; :from, :upsert-into, :insert-into.
(defmethod fmt/format-clause :columns [[_ fields] sqlmap]
  "")

(defn- annotate-type
  "Annotate type to column if it's present."
  [type col]
  (if type
    [col type]
    col))

(defn- attach-types
  "Attach type definitions to columns."
  [type-defs columns]
  (map
   #(annotate-type (get type-defs %) %)
   columns))

(defn- refn-alias
  "Split a reference into ref and its alias (nil if not present)."
  [refn-or-tuple]
  (if (sequential? refn-or-tuple)
    [(first refn-or-tuple) (second refn-or-tuple)]
    [refn-or-tuple nil]))

(defn format-table
  "Format table with optional typed columns: TEST_TABLE(a, x INTEGER ...)"
  [table columns]
  (let [[table aliaz] (refn-alias table)
        cols (if (instance? Table table)
               (attach-types (:dynamic table) columns)
               columns)]
    (str (fmt/to-sql table)
         (when-not (empty? cols)
           (str " (" (fmt/comma-join (map fmt/to-sql cols)) ")"))
         (when aliaz
           (str " " (fmt/to-sql aliaz))))))

(defmethod fmt/format-clause :upsert-into [[op table] sqlmap]
  (str "UPSERT INTO " (format-table table (:columns sqlmap))))

;; Phoenix does not support insert-into, define it anyway.
(defmethod fmt/format-clause :insert-into [[op table] sqlmap]
  (str "INSERT INTO " (format-table table (:columns sqlmap))))

(fmt/register-clause! :upsert-into 45)       ;; before :select

(defn split-qualifier
  "Split terms (e.g. columns) into tuple of [qual name]. Qualifier will 
   be `*` if its not qualified."
  [^String term]
  (let [xs (str/split term #"\.")]
    (if (= 1 (count xs))
      ["*" (first xs)]
      xs)))

(defn group-qualified-terms
  "Group (qualified) terms into qualifier and its names. E.g.:

    => (group-qualified-terms [:a \"test.b\" :test.c])
    {:* (:a) :test (:c :b)}

  Note both qualifier and name are keywordized, and unqualified terms are
  grouped under :*.
  "
  [names]
  (->> names
       (filter #(or (keyword? %) (string? %)))
       (map (comp split-qualifier name))
       (reduce (fn [m [k v]]
                 (update m (keyword k) conj (keyword v)))
               {})))

(defmethod fmt/format-clause :from [[_ tables] sqlmap]
  (let [cols (:columns sqlmap) ;; user specified columns
        table-cols (->> cols
                        (into (:select sqlmap))
                        (map (comp first refn-alias))
                        group-qualified-terms)]
    (str "FROM "
         (fmt/comma-join
          (for [table tables]
            (let [[t alias] (refn-alias table)]
              (condp instance? t
                Table
                (format-table table
                              (->> (get table-cols :*)
                                   ;; fully qualified columns
                                   (concat (get table-cols (db/table-name t)))
                                   ;; alias qualified columns
                                   (concat (get table-cols (keyword alias)))
                                   set
                                   ;; only handle dynamic columns
                                   (filter #(contains? (:dynamic t) %))))
                String
                (format-table table cols)
                clojure.lang.Keyword
                (format-table table cols)
                ;; subquery etc.
                (fmt/to-sql table))))))))

;; TODO: support join table type-inference


(def ^:dynamic *no-op* false)

(defmacro delete-from!
  ([query]
   `(let [db# (db/table-db ~(:delete-from query))
          q# (sql/format ~query)]
      (if *no-op*
        (cons db# q#)
        (jdbc/execute! db# q#))))
  ([table & clauses]
   `(delete-from! (-> (h/delete-from ~table)
                      ~@clauses))))

(defmacro upsert-into!
  "Exectue upsert-into query, E.g.:

  (upsert-into! web-stat
         (columns :a :b [:dynamic_col :int] ...)
         (values ...)
         (on-duplicate-key :ignore))
  "
  ([query]
   `(let [db# (db/table-db ~(:upsert-into query))
          q# (sql/format ~query)]
      (if *no-op*
        (cons db# q#)
        (jdbc/execute! db# q#))))
  ([table & clauses]
   `(upsert-into! (-> (upsert-into ~table)
                      ~@clauses))))

(defmacro select!
  "Execute select query. E.g.:

  (select! :a :b
         (from web-stat)
         (where ...))
  "
  ([query]
   `(let [db# (db/table-db ~(-> (:from query)
                                first
                                refn-alias
                                first))
          q# (sql/format ~query)]
      (if *no-op*
        (cons db# q#)
        (jdbc/query db# q#))))
  ([col & forms]
   (let [[cols# clauses#] (split-with (comp not list?) forms)
         cols# (cons col cols#)]
     `(select! (-> (h/select ~@cols#)
                   ~@clauses#)))))

