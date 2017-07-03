(ns phoenix.honeysql
  (:require [clojure.string :as str]
            [phoenix.db :as db]
            [honeysql.format :as fmt]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [defhelper]
             :as h])
  (:import [phoenix.db Table]))

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
  [table]
  (if (sequential? table)
    [(first table) (second table)]
    [table nil]))

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
  "Split qualified columns into qualifier and name, return tuple.
  Non-qualified will have qualifier `*`."
  [s]
  (let [xs (str/split s #"\.")]
    (if (= 1 (count xs))
      ["*" (first xs)]
      xs)))

(defn group-qualified-names
  "Group qualified names into qualifier and its names. E.g.:

    => (group-qualified-names [:a \"test.b\" :test.c])
    {:* (:a) :test (:c :b)}

  Note non-qualified names are grouped under :*.
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
                        group-qualified-names)]
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

;; TODO: support join

;; ---- Define helpers

(defhelper on-duplicate-key [m args]
  (assoc m :on-duplicate-key (if (sequential? args)
                               (first args)
                               args)))

(defhelper upsert-into [m table]
  (assoc m :upsert-into (if (sequential? table) (first table) table)))

(defmacro upsert-into!
  "Exectue and upsert records. E.g.:

  (upsert! web-stat
         (columns :a :b [:dynamic_col :int] ...)
         (values ...)
         (on-duplicate-key :ignore))
  "
  ([table & forms]
   (let [db# (db/table-db ~table)]
     `(-> (upsert-into (db/table-name ~table))
          ~@forms
          (update :columns
                  #(attach-types (:dynamic ~table) %))))))

(defn from-clause
  "Return clause if it's form of `(from ...)`, false otherwise."
  [clause]
  (and (list? clause)
       (= #'h/from (resolve (first clause)))
       clause))

(defmacro select!
  "Select, execute, and return result. E.g.:

  (select! :a :b
         (from web-stat)
         (where ...))
  "
  ([col & forms]
   ;; TODO: support alias and multiple tables
   (let [[cols# clauses#] (split-with (comp not list?) forms)
         from-clause# (some from-clause clauses#)
         clauses# (doall (remove from-clause clauses#))
         table# (second from-clause#)
         db# (db/table-db table#)
         cols# (cons col cols#)
         dyna-cols# (->> cols#
                         (select-keys (:dynamic (var-get (resolve table#)) {}))
                         vec)]
     `(-> (h/select ~@cols#)
          (h/from (db/table-name ~table#))
          ~@clauses#
          (update :columns
                  #(if (empty? %)
                     ~dyna-cols#
                     %))))))


(defmacro delete! [])

; :columns (:a :b :c [:d :int] :e)
; :columns ([:a :b :c [:d :int] :e])
; (-> (upsert-into web-stat)
;     (columns ...)
;     (values ...)
;     upsert!)

; for upsert!:
; 1. columns not present, do nothing
; 2. columns present, parse and add dynamic type

; (-> (select [...])
;     (from web-stat)
;     (where ...)
;     select!)
;
; for select!:
; 1. :* present, add all dynamic columns
; 2. columns present, parse and add dynamic type
