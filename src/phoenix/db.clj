(ns phoenix.db
  (:require [honeysql.format :as fmt]
            [honeysql.core :as sql]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]))

(def ^:dynamic *default-db* (atom nil))

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
  Define a connection spec in the form of jdbc spec. The last def
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
     (defonce ~db-name spec#)
     (reset! *default-db* ~db-name)))

(defrecord Table [db table]
  fmt/ToSql
  (to-sql [_]
    (fmt/to-sql (keyword table))))

(defmacro deftable
  "Define a table with dynamic columns."
  [table & forms]
  `(let [spec# {:db @*default-db*
                :table (keyword '~table)}]
     (def ~table
       (-> (map->Table spec#)
           ~@forms))))

(defn table?
  "Test if it is a table instance."
  [table]
  (instance? Table table))

(defn db*
  "Return db spec, or set db sepc of a table."
  ([table]
   (when (table? table)
     (:db table)))
  ([^Table table db]
   (assoc table :db db)))

(defn table*
  "Return table name, or set table name."
  ([table]
   (if (table? table)
     (:table table)
     table))
  ([^Table table name]
   (assoc table :table name)))

(defn types*
  "Return column type map, or set column types of a table."
  ([table]
   (when (table? table)
     (:types table)))
  ([^Table table types]
   (update table :types merge types))
  ([^Table table k v & types]
   (-> table
       (assoc-in [:types k] v)
       (update :types into (map vec (partition 2 types))))))

(defn- select-query? [^String query]
  (or (str/starts-with? query "SELECT")
      (str/starts-with? query "select")))

(defn exec-raw
  "Execute raw sql, either in query (select) or update mode. In the query mode,
   options such as :row-fn, :result-fn could be supplied to transform queried
   result, return seq of rows. E.g.:

  (exec-raw [\"SELECT * FROM user WHERE username = ? LIMIT ?\" \"jack\" 1]
            :db my-db
            :row-fn identity)

  While in update mode, return seq of number of rows affected. Phoenix does
  not yet support generated keys in update mode.

  See also jdbc/query, jdbc/execute!.
  "
  [query & {:keys [db] :as opts}]
  (if (select-query? (first query))
    (jdbc/query    (or db @*default-db*) query opts)
    (jdbc/execute! (or db @*default-db*) query opts)))

(defn exec
  "Render sqlmap as sql string, then execute it.
  See also: exec-raw."
  [sqlmap & {:as opts}]
  (let [table (or (:upsert-into sqlmap)
                  (:delete-from sqlmap))]
    (exec-raw (sql/format sqlmap)
              :db (db* (or table
                           (#(if (sequential? %) (first %) %)
                            (first (:from sqlmap))) )))))
