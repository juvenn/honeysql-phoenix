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

(defrecord Table [db table columns dynamic]
  fmt/ToSql
  (to-sql [_]
    (fmt/to-sql (keyword table))))

(defmacro deftable
  "Defind a table with dynamic columns."
  [table spec]
  `(let [spec# (-> ~spec
                   (update-in [:db] #(or % @*default-db*))
                   (update-in [:table] #(or % (keyword '~table))))]
     (def ~table (map->Table spec#))))

(defn table-name
  [table]
  (if (instance? Table table)
    (:table table)
    table))

(defn table-db [table]
  (if (instance? Table table)
    (:db table @*default-db*)
    @*default-db*))

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
              :db (table-db (or table
                                (#(if (sequential? %) (first %) %)
                                 (first (:from sqlmap))) )))))
