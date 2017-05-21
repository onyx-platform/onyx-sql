(ns onyx.plugin.sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.pprint :as pp]
            [clojure.core.async :refer [chan >! >!! <!! close! go timeout alts!! go-loop]]
            [onyx.types :as t]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.peer.function :as function]
            [onyx.extensions :as extensions]
            [onyx.plugin.util :as util]
            [onyx.plugin.protocols :as p]
            [taoensso.timbre :refer [info error debug fatal]]
            [honeysql.core :as sql]
            [java-jdbc.sql :as sql-dsl])
  (:import [com.mchange.v2.c3p0 ComboPooledDataSource]))

(defn create-pool [spec]
  {:datasource
   (doto (ComboPooledDataSource.)
     (.setDriverClass (:classname spec))
     (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
     (.setUser (:user spec))
     (.setPassword (:password spec))
     (.setMaxIdleTimeExcessConnections (* 30 60))
     (.setMaxIdleTime (* 3 60 60)))})

(defn task->pool [task-map]
  (let [db-spec {:classname (:sql/classname task-map)
                 :subprotocol (:sql/subprotocol task-map)
                 :subname (:sql/subname task-map)
                 :user (:sql/user task-map)
                 :password (:sql/password task-map)}]
    (create-pool db-spec)))

; (defn partition-table-by-uuid [{:keys [onyx.core/task-map sql/pool] :as event}]
;   (let [table (name (:sql/table task-map))
;         id-col (name (:sql/id task-map))
;         n-min (:sql/lower-bound task-map)
;         n-min (util/bytes-to-bigint n-min)
;         n-max (:sql/upper-bound task-map)
;         n-max (util/bytes-to-bigint n-max)
;         count (:count (first (jdbc/query pool (vector (format "select count(*) as count from %s" table)))))
;         steps-num (/ count (:sql/rows-per-segment task-map))
;         step (bigint (/ (- n-max n-min) steps-num))
;         ranges (partition-all 2 1 (range n-min n-max step))
;         columns (or (:sql/columns task-map) [:*])]
;     (doall (map (fn [[l h]]
;                   {:low (util/bigint-to-bytes l)
;                    :high (util/bigint-to-bytes (dec (or h (inc n-max))))
;                    :table (:sql/table task-map)
;                    :id (:sql/id task-map)
;                    :columns columns})
;                 ranges))))

(defn partition-table [{:keys [onyx.core/task-map onyx.core/slot-id] :as event} table id colums pool]
  (let [table (name (:sql/table task-map))
        id-col (name (:sql/id task-map))
        n-min (:sql/lower-bound task-map) 
        n-max (:sql/upper-bound task-map)
        ranges (partition-all 2 1 (range n-min n-max (:sql/rows-per-segment task-map)))]
    ;; Partition up the partitions over all n-peers.
    (take-nth (:onyx/n-peers task-map)
              (drop slot-id 
                    (map (fn [[l h]]
                           [l (dec (or h (inc n-max)))])
                         ranges)))))

(defn read-rows [pool table id columns [low high]]
  (let [sql-map {:select columns
                 :from [table]
                 :where [:and
                         [:>= id low]
                         [:<= id high]]}]
    (jdbc/query pool (sql/format sql-map))))

(defrecord SqlPartitioner [pool table id columns event rst completed? offset]
  p/Plugin
  (start [this event]
    this)

  (stop [this event]
    (.close (:datasource pool))
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    @completed?)

  p/Checkpointed
  (checkpoint [this]
    @rst)

  (recover! [this replica-version checkpoint]
    (vreset! completed? false)
    (if (nil? checkpoint)
      (vreset! rst (partition-table event table id columns pool))
      (vreset! rst checkpoint)))

  (checkpointed! [this epoch])

  p/Input
  (poll! [this segment]
    (if-let [part (first @rst)]
      (do (vswap! rst rest)
          (read-rows pool table id columns part))
      (do (vreset! completed? true)
          nil))))

(defn partition-keys [{:keys [onyx.core/task-map] :as event}]
  (let [table (:sql/table task-map)
        id (:sql/id task-map)] 
    (when-not (:sql/lower-bound task-map)
      (throw (Exception. "As of Onyx 0.10.0, :sql/lower-bound must be set on onyx-sql input tasks."))) 
    (when-not (:sql/upper-bound task-map)
      (throw (Exception. "As of Onyx 0.10.0, :sql/upper-bound must be set on onyx-sql input tasks.")))
    (map->SqlPartitioner {:pool (task->pool task-map)
                          :table table
                          :id id
                          :columns (or (:sql/columns task-map) [:*])
                          :event event
                          :rst (volatile! nil)
                          :completed? (volatile! false)
                          :offset (volatile! nil)})))

(defrecord SqlWriter [pool table]
  p/Plugin
  (start [this event]
    this)

  (stop [this event]
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    true)

  p/Checkpointed
  (recover! [this replica-version checkpoint]
    this)

  (checkpoint [this])

  (checkpointed! [this epoch])

  p/Output
  (prepare-batch [this event replica messenger]
    true)

  (write-batch [this {:keys [onyx.core/results]} replica messenger]
    (doseq [msg (mapcat :leaves (:tree results))]
      (jdbc/with-db-transaction
        [conn pool]
        (doseq [row (:rows msg)]
          (jdbc/insert! conn table row))))
    true))

(defn write-rows [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        table (:sql/table task-map)
        pool (task->pool task-map)]
    (->SqlWriter pool table)))

(defrecord SqlUpserter [pool table]
    p/Plugin
  (start [this event]
    this)

  (stop [this event]
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    true)

  p/Checkpointed
  (recover! [this _ _]
    this)

  (checkpoint [this])

  (checkpointed! [this epoch])

  p/Output
  (prepare-batch [this event replica _]
    true)

  (write-batch
    [_ {:keys [onyx.core/results]} replica _]
    (doseq [msg (mapcat :leaves (:tree results))]
      (jdbc/with-db-transaction
        [conn pool]
        (doseq [row (:rows msg)]
          (jdbc/update! conn table row (sql-dsl/where (:where msg))))))
    true))

(defn upsert-rows [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        table (:sql/table task-map)
        pool (task->pool task-map)]
    (->SqlUpserter pool table)))

(defn inject-write-rows
  [event lifecycle]
  {})

(defn close-write-rows
  [event lifecycle]
  {})

(defn inject-upsert-rows
  [event lifecycle]
  {})

(defn close-update-rows
  [event lifecycle]
  {})

(def partition-keys-calls
  {})

(def partition-uuid-calls
  {})

(def write-rows-calls
  {:lifecycle/before-task-start inject-write-rows
   :lifecycle/after-task-stop close-write-rows})

(def upsert-rows-calls
  {:lifecycle/before-task-start inject-upsert-rows
   :lifecycle/after-task-stop close-update-rows})
