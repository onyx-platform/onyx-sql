(ns onyx.plugin.input-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :refer [chan >!! <!!]]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.sql]
            [onyx.api]
            [environ.core :refer [env]]
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
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port-range [40200 40400]
   :onyx.messaging/peer-ports [40199]
   :onyx.messaging/bind-addr "localhost"
   :onyx.messaging/backpressure-strategy :high-restart-latency
   :onyx/id id})

(def dev-env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(defn capitalize [segment]
  (update-in segment [:name] clojure.string/upper-case))

(def db-user (or (env :test-db-user) "root"))

(def db-name (or (env :test-db-name) "onyx_input_test"))

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
          :people
          [:id :int "PRIMARY KEY AUTO_INCREMENT"]
          [:name "VARCHAR(32)"])))

(def people
  ["Mike"
   "Dorrene"
   "Benti"
   "Kristen"
   "Derek"])

(doseq [person people]
  (jdbc/insert! conn-pool :people {:name person}))

(def workflow
  [[:partition-keys :read-rows]
   [:read-rows :capitalize]
   [:capitalize :persist]])

(def out-chan (chan 1000))

(def catalog
  [{:onyx/name :partition-keys
    :onyx/ident :sql/partition-keys
    :onyx/type :input
    :onyx/medium :sql
    :sql/classname "com.mysql.jdbc.Driver"
    :sql/subprotocol "mysql"
    :sql/subname (str "//127.0.0.1:3306/" db-name)
    :sql/user db-user
    :sql/password ""
    :sql/table :people
    :sql/id :id
    :sql/rows-per-segment 1000
    :onyx/batch-size 1000
    :onyx/max-peers 1
    :onyx/doc "Partitions a range of primary keys into subranges"}

   {:onyx/name :read-rows
    :onyx/ident :sql/read-rows
    :onyx/fn :onyx.plugin.sql/read-rows
    :onyx/type :function
    :onyx/batch-size 1000
    :sql/classname "com.mysql.jdbc.Driver"
    :sql/subprotocol "mysql"
    :sql/subname (str "//127.0.0.1:3306/" db-name)
    :sql/user db-user
    :sql/password ""
    :sql/table :people
    :sql/id :id
    :onyx/doc "Reads rows of a SQL table bounded by a key range"}

   {:onyx/name :capitalize
    :onyx/fn :onyx.plugin.input-test/capitalize
    :onyx/type :function
    :onyx/batch-size 1000
    :onyx/doc "Capitilizes the :name key"}

   {:onyx/name :persist
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size 1000
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(defmethod l-ext/inject-lifecycle-resources :persist
  [_ _] {:core.async/chan out-chan})

(def v-peers (onyx.api/start-peers 4 peer-group))

(onyx.api/submit-job
 peer-config
 {:catalog catalog :workflow workflow
  :task-scheduler :onyx.task-scheduler/round-robin})

(def results (take-segments! out-chan))

(fact results
      => [{:id 1 :name "MIKE"}
          {:id 2 :name "DORRENE"}
          {:id 3 :name "BENTI"}
          {:id 4 :name "KRISTEN"}
          {:id 5 :name "DEREK"}
          :done])

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env dev-env)

