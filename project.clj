(defproject org.onyxplatform/onyx-sql "0.10.0-beta14"
  :description "Onyx plugin for JDBC-backed SQL databases"
  :url "https://github.com/onyx-platform/onyx-sql"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/java.jdbc "0.7.0-alpha3"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.10.0-beta14"]
                 [java-jdbc/dsl "0.1.3"]
                 [com.mchange/c3p0 "0.9.2.1"]
                 [aero "0.2.0"]
                 [honeysql "0.5.1"]]
  :profiles {:dev {:dependencies [[mysql/mysql-connector-java "5.1.25"]]
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]
                   :resource-paths ["test-resources/"]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
