(ns phoenix.db
  (:require [honeysql.format :as fmt]
            [honeysql.core :as sql]
            [clojure.string :as str]
            [clojure.java.jdbc :as jdbc]))

(def ^:dynamic *default-db* (atom nil))

(defn phoenix
  "Create an phoenix jdbc driver spec.

  :zk-quorum  Zookeeper quorum url, including port, default `127.0.0.1:2181`
  :zk-parent  Zookeeper parent, default `/hbase`
  :principal  Kerberos principal, optional
  :keytab     Kerberos keytab, optional

  See Phoenix [FAQ](http://phoenix.apache.org/faq.html#What_is_the_Phoenix_JDBC_URL_syntax)."
  [spec]
  (-> spec
      (update :dbtype #(or % :phoenix))
      (update :connection-uri
              (fn [uri]
                (or uri
                    (->> (map spec [:zk-parent :principal :keytab])
                         (remove nil?)
                         (str/join ":")
                         (str "jdbc:phoenix:"
                              (or (:zk-quorum spec) "127.0.0.1:2181")
                              ":")))))))

(defn avatica
  "Create an avatica jdbc driver spec.

  :url            Full url of avatica server (or phoenix query server), e.g.:
                  http://127.0.0.1:8765
  :serialization  Serialization protocol to communicate with server,
                  `json` or `protobuf`.

  For more options, please see:
  https://calcite.apache.org/avatica/docs/client_reference.html"
  [spec]
  (-> spec
      (update :dbtype #(or % :avatica))
      (update :connection-uri
              (fn [uri]
                (or uri
                    (reduce-kv (fn [uri k v]
                                 (if v
                                   (str uri (name k) "=" (name v) ";")
                                   uri))
                               "jdbc:avatica:remote:"
                               spec))))))

(defmacro defdb
  "
  Define a db spec and set it as default.
  "
  [db-name spec]
  `(let [db# ~spec]
     (defonce ~db-name db#)
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
