(defproject walkingcloud/honeysql-phoenix "0.1.2-SNAPSHOT"
  :description "Honeysql extension for HBase Phoenix"
  :url "https://github.com/juvenn/honeysql-phoenix"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [honeysql "0.8.2"]]
  :profiles {:dev {:dependencies [[org.apache.hbase/hbase-client "1.2.2"]
                                  [org.apache.phoenix/phoenix-core
                                   "4.10.0-HBase-1.2"]]}})
