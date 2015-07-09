(defproject org.onyxplatform/onyx-sql "0.6.0"
  :description "Onyx plugin for JDBC-backed SQL databases"
  :url "https://github.com/MichaelDrogalis/onyx-sql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [java-jdbc/dsl "0.1.3"]
                 [com.mchange/c3p0 "0.9.2.1"]
                 [org.onyxplatform/onyx "0.6.0"]
                 [com.taoensso/timbre "3.0.1"]
                 [honeysql "0.5.1"]]
  :profiles {:dev {:dependencies [[midje "1.6.2"]
                                  [environ "1.0.0"]
                                  [mysql/mysql-connector-java "5.1.25"]]
                   :plugins [[lein-midje "3.1.3"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
