(ns phoenix.honeysql
  (:require [clojure.string :as str]
            [potemkin :refer [import-vars]]
            [phoenix.db :as db]
            [honeysql.format :as fmt]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [defhelper]
             :as h]))

(import-vars
 [honeysql.helpers
  columns
  values
  modifiers
  select
  from
  where
  join
  left-join
  right-join
  full-join
  group
  having
  order-by
  limit
  offset
  delete-from])

(def as-sql
  "Format sqlmap as sql string, return vector of sql string and params.
  See also honeysql.core/format."
  #'sql/format)

(fmt/register-clause! :on-duplicate-key 225) ;; after :values

(defhelper on-duplicate-key [m args]
  (assoc m :on-duplicate-key (if (sequential? args)
                               (first args)
                               args)))

(defmethod fmt/format-clause :on-duplicate-key [[op values] sqlmap]
  (if (= :ignore values)
    "ON DUPLICATE KEY IGNORE"
    (str "ON DUPLICATE KEY UPDATE "
         (fmt/comma-join (for [[k v] values]
                           (str (fmt/to-sql k) " = " (fmt/to-sql v)))))))

;; Reset :columns to be no-op
(defmethod fmt/format-clause :columns [[_ fields] sqlmap]
  "")

(defn- ref-alias
  "Partition a form into ref and alias, assuming [ref alias] form."
  [form]
  (if (sequential? form)
    [(first form) (second form)]
    [form nil]))

(defn- format-columns [cols]
  (fmt/paren-wrap (fmt/comma-join (map fmt/to-sql cols))))

(defmethod fmt/format-clause :values [[_ values] _]
  (if (sequential? (first values))
    ;; value in vector form
    (str "VALUES "
         (fmt/comma-join
          (for [xs values] (format-columns xs))))
    ;; value in map form
    (let [ks (keys (first values))]
      (str "VALUES "
           (fmt/comma-join
            (for [m values]
              (format-columns (map (partial get m) ks))))))))

(defn- format-table-cols
  "Format table with (optionally-typed) columns."
  [table cols]
  (let [[table alias] (if (sequential? table)
                        table
                        [table])]
    (str (fmt/to-sql table)
         (when alias
           (str " " (fmt/to-sql alias)))
         (when-let [cols (seq cols)]
           (str " " (format-columns cols))))))

(defn- annotate-types
  "Annotate cols with types, return seq of cols with optional types.
   E.g.:

  ```
  => (annotate-types user [:username :email :referrer])
  (:username :email [:referrer \"VARCHAR(64)\"])
  ```
  "
  [table cols]
  (if-not (db/table? table)
    cols
    (let [get-type (partial get (db/types* table))]
      (map #(if-let [type* (get-type %)]
              [% type*]
              %)
           (seq cols)))))

(fmt/register-clause! :upsert-into 45)       ;; before :select

(defmethod fmt/format-clause :upsert-into [[op table] sqlmap]
  (let [cols (or (:columns sqlmap)
                 (when-let [row (first (:values sqlmap))]
                   (when (map? row)
                     (keys row))))]
    (->> (annotate-types table cols)
         (format-table-cols table)
         (str "UPSERT INTO "))))

(defhelper upsert-into [m table]
  (assoc m :upsert-into (if (sequential? table) (first table) table)))

(defn- split-qual-col
  "Split column name into tuple of [qual name], keywordized. Unqualified columns are
   grouped under :_."
  [term]
  (let [[qual col] (str/split (name term) #"\.")]
    (if col
      [(keyword qual) (keyword col)]
      [:_ (keyword qual)])))

(defn- infer-qual-col
  "Infer qual(ifier) and column from a column related expr, return
   qual col pair."
  [expr]
  (cond
    (string? expr) (split-qual-col expr)
    (keyword? expr) (split-qual-col expr)
    (and (vector? expr)
         (= 2 (count expr)))
    (infer-qual-col (first expr))
    :else nil))

(defn- group-qual-col
  "Group qual-col pair by qualifier, keywordized."
  [pairs]
  (reduce
   (fn [m [k v]]
     (if v
       (update m (keyword k) (fnil conj []) v)
       m))
   {}
   pairs))

(defmethod fmt/format-clause :from [[_ tables] sqlmap]
  (let [select-cols (->> (:select sqlmap)
                         (map infer-qual-col)
                         (group-qual-col))]
    (str "FROM "
         (fmt/comma-join
          (for [table-ref tables]
            (let [[table alias types] (if (sequential? table-ref)
                                         table-ref
                                         [table-ref])
                  [alias types] (if (map? alias)
                                  [nil alias]
                                  [(keyword alias) types])]
              (cond
                ;; no type to infer about
                (keyword? table)
                (format-table-cols [table alias] types)

                (db/table? table)
                (->> select-cols
                     ((juxt :_ (or alias :_null) (db/table* table)))
                     (reduce into #{})
                     (select-keys (db/types* table))
                     ;; user specified type is preferred over inferred
                     (#(merge %2 %1) types)
                     (format-table-cols [table alias]))

                ;; subquery etc
                :else
                (fmt/to-sql table-ref))))))))

