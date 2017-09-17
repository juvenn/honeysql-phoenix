(defproject phoenix-sql "0.2.0-SNAPSHOT"
  :description "Clojure SQL for HBase Phoenix"
  :url "https://github.com/juvenn/phoenix-sql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.7.0"]
                 [honeysql "0.9.0"]]
  :profiles {:dev {:dependencies [[org.apache.hbase/hbase-client "1.2.2"]
                                  [org.apache.phoenix/phoenix-core
                                   "4.10.0-HBase-1.2"]]}})
