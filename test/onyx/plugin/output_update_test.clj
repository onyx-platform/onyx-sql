(ns onyx.plugin.output-update-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.plugin.core-async]
            [onyx.plugin.sql]
            [onyx.api]
            [environ.core :refer [env]]
            [honeysql.core :as sql]
            [midje.sweet :refer :all])
  (:import [com.mchange.v2.c3p0 ComboPooledDataSource]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx/id id})

(def peer-config
  {:zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :netty
   :onyx.messaging/peer-port-range [40200 40400]
   :onyx.messaging/peer-ports [40199]
   :onyx.messaging/bind-addr "localhost"
   :onyx/id id})

(def dev-env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(defn transform [{:keys [id word] :as segment}]
  {:rows [{:word (str word "!")}] :where {:id id}})

(def db-user (or (env :test-db-user) "root"))

(def db-name (or (env :test-db-name) "onyx_output_update_test"))

(def db-spec
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname "//127.0.0.1:3306"
   :user db-user
   :password ""})

(defn pool [spec]
  {:datasource
   (doto (ComboPooledDataSource.)
     (.setDriverClass (:classname spec))
     (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
     (.setUser (:user spec))
     (.setPassword (:password spec))
     (.setMaxIdleTimeExcessConnections (* 30 60))
     (.setMaxIdleTime (* 3 60 60)))})

(def conn-pool (pool db-spec))

(try
  (jdbc/execute! conn-pool [(str "drop database " db-name)])
  (catch Exception e
    (.printStackTrace e)))

(jdbc/execute! conn-pool [(str "create database " db-name)])
(jdbc/execute! conn-pool [(str "use " db-name)])

(def db-spec
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname (str "//127.0.0.1:3306/" db-name)
   :user db-user
   :password ""})

(def conn-pool (pool db-spec))

(jdbc/execute!
  conn-pool
  (vector (jdbc/create-table-ddl
            :words
            [:id :int]
            [:word "VARCHAR(32)"])))

(def words
  [{:id 1 :word "Cat"}
   {:id 2 :word "Orange"}
   {:id 3 :word "Pan"}
   {:id 4 :word "Door"}
   {:id 5 :word "Surf board"}])

(doseq [word words]
  (jdbc/insert! conn-pool :words word))


(def update-in-chan (chan 1000))

(doseq [word words]
  (>!! update-in-chan word))

(>!! update-in-chan :done)

(def workflow
  [[:in :transform]
   [:transform :out]])

(def catalog
  [{:onyx/name :in
    :onyx/plugin: :onyx.plugin.core-async/output
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size 1000
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :transform
    :onyx/fn :onyx.plugin.output-update-test/transform
    :onyx/type :function
    :onyx/batch-size 1000
    :onyx/doc "Transforms a segment to prepare for SQL persistence"}

   {:onyx/name :out
    :onyx/plugin ::onyx.plugin.sql/upsert-rows
    :onyx/type :output
    :onyx/medium :sql
    :sql/classname "com.mysql.jdbc.Driver"
    :sql/subprotocol "mysql"
    :sql/subname (str "//127.0.0.1:3306/" db-name)
    :sql/user db-user
    :sql/password ""
    :sql/table :words
    :onyx/batch-size 1000
    :onyx/doc "Writes segments from the :rows keys to the SQL database based on :where key"}])

(defn inject-in-ch [event lifecycle]
  {:core.async/chan update-in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.output-update-test/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.sql/upsert-rows-calls}])

(def v-peers (onyx.api/start-peers 3 peer-group))

(def job-id
  (:job-id (onyx.api/submit-job
             peer-config
             {:catalog catalog :workflow workflow :lifecycles lifecycles
              :task-scheduler :onyx.task-scheduler/balanced})))

(onyx.api/await-job-completion peer-config job-id)

(def sql-map {:select [:*] :from [:words]})

(def results (jdbc/query conn-pool (sql/format sql-map)))

(defn transform-word
  [word]
  {:id (:id word) :word (str (:word word) "!")})

(fact results => (map transform-word words))


(close! update-in-chan)

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env dev-env)