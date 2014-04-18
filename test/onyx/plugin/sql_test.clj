(ns onyx.plugin.sql-test
  (:require [clojure.data.fressian :as fressian]
            [midje.sweet :refer :all]
            [onyx.plugin.sql]
            [onyx.api])
  (:import [org.hornetq.api.core.client HornetQClient]
           [org.hornetq.api.core TransportConfiguration HornetQQueueExistsException]
           [org.hornetq.core.remoting.impl.netty NettyConnectorFactory]))

(defn create-queue [session queue-name]
  (try
    (.createQueue session queue-name queue-name true)
    (catch Exception e)))

(defn consume-queue! [config queue-name echo]
  (let [tc (TransportConfiguration. (.getName NettyConnectorFactory) config)
        locator (HornetQClient/createServerLocatorWithoutHA (into-array [tc]))
        session-factory (.createSessionFactory locator)
        session (.createTransactedSession session-factory)]

    (create-queue session queue-name)

    (let [consumer (.createConsumer session queue-name)
          results (atom [])]
      (.start session)
      (while (not= (last @results) :done)
        (when (zero? (mod (count @results) echo))
          (prn (format "[HQ Util] Read %s segments" (count @results))))
        (let [message (.receive consumer)]
          (when message
            (.acknowledge message)
            (swap! results conj (fressian/read (.toByteBuffer (.getBodyBuffer message)))))))

      (prn "[HQ Util] Done reading")
      (.commit session)
      (.close consumer)
      (.close session)

      @results)))

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
    :sql/host "localhost"
    :sql/port 1000
    :sql/protocol "mysql"
    :sql/table "people"
    :sql/column "id"
    :sql/partition-size 1000
    :onyx/doc "Partitions a range of primary keys into subranges"}

   {:onyx/name :load-rows
    :onyx/ident :sql/load-rows
    :onyx/fn :onyx.plugin.sql/load-rows
    :onyx/type :transformer
    :onyx/consumption :concurrent
    :onyx/batch-size 1000
    :sql/host "localhost"
    :sql/port 1000
    :sql/username "root"
    :sql/password "root"
    :sql/protocol "mysql"
    :sql/table "people"
    :sql/column "id"
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

(def results (consume-queue! hq-config out-queue 1))

(doseq [v-peer v-peers]
  (try
    ((:shutdown-fn v-peer))
    (catch Exception e (prn e))))

(try
  (onyx.api/shutdown conn)
  (catch Exception e (prn e)))

(fact results
      => [{:id 1 :name "MIKE"}
          {:id 2 :name "DORRENE"}
          {:id 3 :name "BENTI"}
          {:id 4 :name "DEREK"}
          {:id 5 :name "KRISTEN"}])

