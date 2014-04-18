(defproject onyx-sql "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.3"]
                 [com.mdrogalis/onyx "0.2.0-SNAPSHOT"]
                 [honeysql "0.4.3"]]
  :profiles {:dev {:dependencies [[midje "1.6.2"]
                                  [org.hornetq/hornetq-core-client "2.4.0.Final"]
                                  [org.clojure/data.fressian "0.2.0"]]
                   :plugins [[lein-midje "3.1.3"]]}})
