(ns honeysql-phoenix.core
  (:require [clojure.string :as str]
            [honeysql.format :as fmt]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [defhelper]
             :as h]))

(def ^:dynamic *default-db* nil)

(defn add-connection-uri
  "Build and add connection-uri to spec if not present."
  [{:keys [quorum zk-path] :as spec}]
  (if (:connection-uri spec)
    spec
    (assoc spec :connection-uri
           (str "jdbc:phoenix:"
                (or quorum "127.0.0.1:2181")
                ":"
                (or zk-path "/hbase")))))

(defmacro defdb
  "
  Define a connection spec in the form of jdbc spec. The first def
  will be used by default for queries where no db were specified. The
  following options can be specified in the spec:

  :connection-uri  If present will be used to connect to jdbc regardless
                   of :quorum and :zk-path.
  :quorum          zookeeper quorum, it should contain port. Default to
                   be `127.0.0.1:2181`.
  :zk-path         hbase's zookeeper znode parent, default to be
                   `/hbase`.

  See java.jdbc.
  "
  [db-name spec]
  `(let [spec# (add-connection-uri ~spec)]
     (defonce ~db-name spec#)))

(defrecord Table [db table columns dynamic])

(defmacro deftable
  "Defind a table with dynamic columns."
  [table spec]
  `(let [spec# (-> ~spec
                   (update-in [:db] #(or % *default-db*))
                   (update-in [:table] #(or % (keyword '~table))))]
     (def ~table (map->Table spec#))))

(defn table-name
  [table]
  (if (instance? Table table)
    (:table table)
    table))

(defn table-db [table]
  (if (instance? Table table)
    (:db table *default-db*)
    *default-db*))

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

(defn- annotate-type
  "Annotate type if it's present."
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

(defmacro upsert-into!
  "Exectue and upsert records. E.g.:

  (upsert! web-stat
         (columns :a :b [:dynamic_col :int] ...)
         (values ...)
         (on-duplicate-key :ignore))
  "
  ([table & forms]
   (let [db# (table-db ~table)]
     `(-> (upsert-into (table-name ~table))
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
         db# (table-db table#)
         cols# (cons col cols#)
         dyna-cols# (->> cols#
                         (select-keys (:dynamic (var-get (resolve table#)) {}))
                         vec)]
     `(-> (h/select ~@cols#)
          (h/from (table-name ~table#))
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
