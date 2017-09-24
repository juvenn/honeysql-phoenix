(defproject honeysql-phoenix "0.2.1"
  :description "Clojure SQL for HBase Phoenix"
  :url "https://github.com/juvenn/honeysql-phoenix"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.7.0"]
                 [potemkin "0.4.4"]
                 [honeysql "0.9.0"]]
  :profiles {:dev {:dependencies [[org.apache.hbase/hbase-client "1.2.2"]
                                  [org.apache.phoenix/phoenix-core
                                   "4.10.0-HBase-1.2"]]}
             :avatica {:dependencies [[org.apache.calcite.avatica/avatica-core
                                       "1.10.0"]]}}
  :deploy-repositories [["releases" :clojars]])
