(ns onyx.plugin.output-test
  (:require [clojure.java.jdbc :as jdbc]
            [midje.sweet :refer :all]
            [honeysql.core :as sql]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.plugin.sql]
            [onyx.api])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]
           [com.mchange.v2.c3p0 ComboPooledDataSource]))

(defn transform [{:keys [word] :as segment}]
  {:rows [{:word word}]})

(def db-spec
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname "//127.0.0.1:3306"
   :user "root"
   :password "password"})

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
  (jdbc/execute! conn-pool ["drop database onyx_output_test"])
  (catch Exception e
    (.printStackTrace e)))

(jdbc/execute! conn-pool ["create database onyx_output_test"])
(jdbc/execute! conn-pool ["use onyx_output_test"])

(def db-spec
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname "//127.0.0.1:3306/onyx_output_test"
   :user "root"
   :password "password"})

(def conn-pool (pool db-spec))

(jdbc/execute!
 conn-pool
 (vector (jdbc/create-table-ddl
          :words
          [:id :int "PRIMARY KEY AUTO_INCREMENT"]
          [:word "VARCHAR(32)"])))

(def words
  [{:word "Cat"}
   {:word "Orange"}
   {:word "Pan"}
   {:word "Door"}
   {:word "Surf board"}])

(def hornetq-host "localhost")

(def hornetq-port 5465)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def in-queue (str (java.util.UUID/randomUUID)))

(hq-util/create-queue! hq-config in-queue)
(hq-util/write-and-cap! hq-config in-queue words 1)

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts
  {:hornetq/mode :vm
   :hornetq/server? true
   :hornetq.server/type :vm
   :zookeeper/address "127.0.0.1:2185"
   :zookeeper/server? true
   :zookeeper.server/port 2185
   :onyx/id id
   :onyx.coordinator/revoke-delay 5000})

(def peer-opts
  {:hornetq/mode :vm
   :zookeeper/address "127.0.0.1:2185"
   :onyx/id id})

(def workflow {:input {:transform :output}})

(def catalog
  [{:onyx/name :input
    :onyx/ident :hornetq/read-segments
    :onyx/type :input
    :onyx/medium :hornetq
    :onyx/consumption :concurrent
    :hornetq/queue-name in-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size 1000}

   {:onyx/name :transform
    :onyx/fn :onyx.plugin.output-test/transform
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 1000
    :onyx/doc "Transforms a segment to prepare for SQL persistence"}

   {:onyx/name :output
    :onyx/ident :sql/write-rows
    :onyx/type :output
    :onyx/medium :sql
    :onyx/consumption :concurrent
    :sql/classname "com.mysql.jdbc.Driver"
    :sql/subprotocol "mysql"
    :sql/subname "//127.0.0.1:3306/onyx_output_test"
    :sql/user "root"
    :sql/password "password"
    :sql/table :words
    :onyx/batch-size 1000
    :onyx/doc "Writes segments from the :rows keys to the SQL database"}])

(def conn (onyx.api/connect :memory coord-opts))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(def job-id (onyx.api/submit-job conn {:catalog catalog :workflow workflow}))

@(onyx.api/await-job-completion conn (str job-id))

(def sql-map {:select [:*] :from [:words]})

(def results (jdbc/query conn-pool (sql/format sql-map)))

(doseq [v-peer v-peers]
  ((:shutdown-fn v-peer)))

(onyx.api/shutdown conn)

(fact results => (map-indexed (fn [k x] (assoc x :id (inc k))) words))

