(ns onyx.plugin.input-test
  (:require [clojure.java.jdbc :as jdbc]
            [com.stuartsierra.component :as component]
            [onyx.system :refer [onyx-development-env]]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.plugin.sql]
            [onyx.api]
            [midje.sweet :refer :all])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]
           [com.mchange.v2.c3p0 ComboPooledDataSource]))

(def id (java.util.UUID/randomUUID))

(def env-config
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2185"
   :zookeeper/server? true
   :zookeeper.server/port 2185
   :onyx/id id})

(def peer-config
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id
   :onyx.peer/inbox-capacity 100
   :onyx.peer/outbox-capacity 100
   :onyx.peer/job-scheduler :onyx.job-scheduler/round-robin})

(def dev (onyx-development-env env-config))

(def env (component/start dev))

(defn capitalize [segment]
  (update-in segment [:name] clojure.string/upper-case))

(def db-spec
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname "//127.0.0.1:3306"
   :user "root"
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
  (jdbc/execute! conn-pool ["drop database onyx_input_test"])
  (catch Exception e
    (.printStackTrace e)))

(jdbc/execute! conn-pool ["create database onyx_input_test"])
(jdbc/execute! conn-pool ["use onyx_input_test"])

(def db-spec
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname "//127.0.0.1:3306/onyx_input_test"
   :user "root"
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

(def hornetq-host "localhost")

(def hornetq-port 5465)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def out-queue (str (java.util.UUID/randomUUID)))

(hq-util/create-queue! hq-config out-queue)

(def workflow
  [[:partition-keys :read-rows]
   [:read-rows :capitalize]
   [:capitalize :persist]])

(def catalog
  [{:onyx/name :partition-keys
    :onyx/ident :sql/partition-keys
    :onyx/type :input
    :onyx/medium :sql
    :onyx/consumption :concurrent
    :onyx/bootstrap? true
    :sql/classname "com.mysql.jdbc.Driver"
    :sql/subprotocol "mysql"
    :sql/subname "//127.0.0.1:3306/onyx_input_test"
    :sql/user "root"
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
    :onyx/consumption :concurrent
    :onyx/batch-size 1000
    :sql/classname "com.mysql.jdbc.Driver"
    :sql/subprotocol "mysql"
    :sql/subname "//127.0.0.1:3306/onyx_input_test"
    :sql/user "root"
    :sql/password ""
    :sql/table :people
    :sql/id :id
    :onyx/doc "Reads rows of a SQL table bounded by a key range"}

   {:onyx/name :capitalize
    :onyx/fn :onyx.plugin.input-test/capitalize
    :onyx/type :function
    :onyx/consumption :concurrent
    :onyx/batch-size 1000
    :onyx/doc "Capitilizes the :name key"}

   {:onyx/name :persist
    :onyx/ident :hornetq/write-segments
    :onyx/type :output
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size 1000
    :onyx/doc "Output source for intermediate query results"}])

(def v-peers (onyx.api/start-peers! 1 peer-config))

(onyx.api/submit-job peer-config
                     {:catalog catalog :workflow workflow
                      :task-scheduler :onyx.task-scheduler/round-robin})

(def results (hq-util/consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(fact results
      => [{:id 1 :name "MIKE"}
          {:id 2 :name "DORRENE"}
          {:id 3 :name "BENTI"}
          {:id 4 :name "KRISTEN"}
          {:id 5 :name "DEREK"}
          :done])

(component/stop env)

