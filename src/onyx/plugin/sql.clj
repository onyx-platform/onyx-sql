(ns onyx.plugin.sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :refer [chan >! >!! <!! close! go timeout alts!!]]
            [taoensso.timbre :refer [fatal]]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.extensions :as extensions]
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

(defn partition-table [{:keys [onyx.core/task-map sql/pool] :as event}]
  (let [table (name (:sql/table task-map))
        id-col (name (:sql/id task-map))
        n-min (or (:sql/lower-bound task-map)
                  (:min (first (jdbc/query pool (vector (format "select min(%s) as min from %s" id-col table))))))
        n-max (or (:sql/upper-bound task-map)
                  (:max (first (jdbc/query pool (vector (format "select max(%s) as max from %s" id-col table))))))
        ranges (partition-all 2 1 (range n-min n-max (:sql/rows-per-segment task-map)))]
    (map (fn [[l h]]
           {:low l
            :high (dec (or h (inc n-max)))
            :table (:sql/table task-map)
            :id (:sql/id task-map)})
         ranges)))

(defn inject-partition-keys
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id] :as event} lifecycle]
  (let [pool (task->pool task-map)
        ch (chan (or (:sql/read-buffer task-map) 1000))]
    (go
     (try
       (let [partitions (partition-table (assoc event :sql/pool pool))
             chunk (map (fn [p] {:partition p :status :incomplete}) partitions)
             ;; Attempt to write. It will fail if it's already been written. Read it back
             ;; in either case.
             _ (extensions/write-chunk log :chunk chunk task-id)
             content (extensions/read-chunk log :chunk task-id)]
         ;; Remove messages that are already acknowledged.
         (doseq [part (remove #(= (:status %) :acked) content)]
           (>! ch {:content part}))
         (>! ch :done))
       (catch Exception e
         (fatal e))))
    {:sql/pool pool
     :sql/read-ch ch
     :sql/pending-messages (atom {})}))

(defn close-partition-keys
  [{:keys [sql/pool] :as event} lifecycle]
  (.close (:datasource pool))
  {})

(defmethod p-ext/read-batch :sql/partition-keys
  [{:keys [sql/read-ch sql/pending-messages onyx.core/task-map]}]
  (let [pending (count (keys @pending-messages))
        max-pending (or (:onyx/max-pending task-map) 10000)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (or (:onyx/batch-timeout task-map) 50)
        timeout-ch (timeout ms)
        batch (->> (range max-segments)
                   (map (fn [_]
                          (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
                            (if (= :done result)
                              {:id (java.util.UUID/randomUUID)
                               :input :sql
                               :message :done}
                              {:id (java.util.UUID/randomUUID)
                               :input :sql
                               :message (:partition (:content result))}))))
                   (remove (comp nil? :message)))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (select-keys m [:message])))
    {:onyx.core/batch batch}))

(defn update-partition [content new-content part]
  (map
   (fn [c]
     (if (= (:partition c) part)
       new-content
       c))
   content))

(defmethod p-ext/ack-message :sql/partition-keys
  [{:keys [sql/pending-messages onyx.core/log onyx.core/task-id]} message-id]
  (let [part (get @pending-messages message-id)
        content {:status :acked :partition (:message part)}
        read-content (extensions/read-chunk log :chunk task-id)
        updated-content (update-partition read-content content message-id)]
    (extensions/force-write-chunk log :chunk updated-content task-id)
    (swap! pending-messages dissoc message-id)))

(defmethod p-ext/retry-message :sql/partition-keys
  [{:keys [sql/pending-messages sql/read-ch onyx.core/log onyx.core/task-id]} message-id]
  (let [snapshot @pending-messages
        message (get snapshot message-id)]
    (swap! pending-messages dissoc message-id)
    (if (:partition message)
      (>!! read-ch {:partition (:partition message)})
      (>!! read-ch :done))))

(defmethod p-ext/pending? :sql/partition-keys
  [{:keys [sql/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? :sql/partition-keys
  [{:keys [sql/pending-messages]}]
  (let [x @pending-messages]
    (and (= (count (keys x)) 1)
         (= (first (map :message (vals x))) :done))))

(defn inject-read-rows
  [{:keys [onyx.core/task-map] :as event} lifecycle]
  (let [pool (task->pool task-map)]
    {:sql/pool pool
     :onyx.core/params [pool]}))

(defn close-read-rows
  [{:keys [sql/pool] :as event} lifecycle]
  (.close (:datasource pool))
  {})

(defn read-rows [pool {:keys [table id low high] :as segment}]
  (let [sql-map {:select [:*]
                 :from [table]
                 :where [:and
                         [:>= id low]
                         [:<= id high]]}]
    (jdbc/query pool (sql/format sql-map))))

(defn inject-write-rows
  [{:keys [onyx.core/task-map] :as event} lifecycle]
  {:sql/pool (task->pool task-map)})

(defn close-write-rows
  [{:keys [sql/pool] :as event} lifecycle]
  (.close (:datasource pool))
  {})

(defmethod p-ext/write-batch :sql/write-rows
  [{:keys [onyx.core/results onyx.core/task-map sql/pool] :as event}]
  (doseq [msg (mapcat :leaves results)]
    (jdbc/with-db-transaction
      [conn pool]
      (if (:sql/update task-map)
        (doseq [row (:rows (:message msg))]
          (jdbc/update! conn (:sql/table task-map) row (sql-dsl/where (:where (:message msg)))))
        (doseq [row (:rows (:message msg))]
          (jdbc/insert! conn (:sql/table task-map) row)))
      ))
  {:onyx.core/written? true})

(defmethod p-ext/seal-resource :sql/write-rows
  [event]
  {})

(def partition-keys-calls
  {:lifecycle/before-task-start inject-partition-keys
   :lifecycle/after-task-stop close-partition-keys})

(def read-rows-calls
  {:lifecycle/before-task-start inject-read-rows
   :lifecycle/after-task-stop close-read-rows})

(def write-rows-calls
  {:lifecycle/before-task-start inject-write-rows
   :lifecycle/after-task-stop close-write-rows})