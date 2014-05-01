(ns onyx.plugin.sql-test
  (:require [clojure.java.jdbc :as jdbc]
            [midje.sweet :refer :all]
            [onyx.queue.hornetq-utils :as hq-util]
            [onyx.plugin.sql]
            [onyx.api])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]
           [com.mchange.v2.c3p0 ComboPooledDataSource]))

(defn capitalize [{:keys [rows] :as segment}]
  (map (fn [{:keys [name] :as row}] (assoc row :name (clojure.string/upper-case name))) rows))

(def db-spec
  {:classname "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :subname "//127.0.0.1:3306/onyx_test"
   :user "root"
   :password "root"})

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

(jdbc/execute! conn-pool ["drop database onyx_test"])
(jdbc/execute! conn-pool ["create database onyx_test"])
(jdbc/execute! conn-pool ["use onyx_test"])

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

(def hornetq-port 5445)

(def hq-config {"host" hornetq-host "port" hornetq-port})

(def out-queue (str (java.util.UUID/randomUUID)))

(def id (str (java.util.UUID/randomUUID)))

(def coord-opts {:datomic-uri (str "datomic:mem://" id)
                 :hornetq-host hornetq-host
                 :hornetq-port hornetq-port
                 :zk-addr "127.0.0.1:2181"
                 :onyx-id id
                 :revoke-delay 5000})

(def peer-opts {:hornetq-host hornetq-host
                :hornetq-port hornetq-port
                :zk-addr "127.0.0.1:2181"
                :onyx-id id})

(def workflow {:partition-keys {:load-rows {:capitalize :persist}}})

(def catalog
  [{:onyx/name :partition-keys
    :onyx/ident :sql/partition-keys
    :onyx/direction :input
    :onyx/type :database
    :onyx/medium :sql
    :onyx/consumption :sequential
    :onyx/bootstrap? true
    :sql/classname "com.mysql.jdbc.Driver"
    :sql/subprotocol "mysql"
    :sql/subname "//127.0.0.1:3306/onyx_test"
    :sql/user "root"
    :sql/password "root"
    :sql/table :people
    :sql/id :id
    :sql/partition-size 1000
    :onyx/doc "Partitions a range of primary keys into subranges"}

   {:onyx/name :load-rows
    :onyx/ident :sql/load-rows
    :onyx/fn :onyx.plugin.sql/load-rows
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 1000
    :sql/classname "com.mysql.jdbc.Driver"
    :sql/subprotocol "mysql"
    :sql/subname "//127.0.0.1:3306/onyx_test"
    :sql/user "root"
    :sql/password "root"
    :sql/table :people
    :sql/id :id
    :onyx/doc "Reads rows of a SQL table bounded by a key range"}

   {:onyx/name :capitalize
    :onyx/fn :onyx.plugin.sql-test/capitalize
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 1000
    :onyx/doc "Capitilizes the :name key"}

   {:onyx/name :persist
    :onyx/ident :hornetq/write-segments
    :onyx/direction :output
    :onyx/consumption :concurrent
    :onyx/type :queue
    :onyx/medium :hornetq
    :hornetq/queue-name out-queue
    :hornetq/host hornetq-host
    :hornetq/port hornetq-port
    :onyx/batch-size 1000
    :onyx/doc "Output source for intermediate query results"}])

(def conn (onyx.api/connect (str "onyx:memory//localhost/" id) coord-opts))

(def v-peers (onyx.api/start-peers conn 1 peer-opts))

(onyx.api/submit-job conn {:catalog catalog :workflow workflow})

(def results (hq-util/consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

(fact results
      => [[{:id 1 :name "MIKE"}
           {:id 2 :name "DORRENE"}
           {:id 3 :name "BENTI"}
           {:id 4 :name "KRISTEN"}
           {:id 5 :name "DEREK"}]
          :done])

