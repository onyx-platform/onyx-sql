(ns onyx.plugin.sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :refer [chan >! >!! <!! close! go timeout alts!!]]
            [honeysql.core :as sql]
            [onyx.peer.task-lifecycle-extensions :as l-ext]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.extensions :as extensions])
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
  (let [sql-map {:select [:%count.*] :from [(:sql/table task-map)]}
        n ((keyword "count(*)") (first (jdbc/query pool (sql/format sql-map))))
        ranges (partition-all 2 1 (range (or (:sql/lower-bound task-map) 1)
                                         (or (:sql/upper-bound task-map) n)
                                         (:sql/rows-per-segment task-map)))]
    (map (fn [[l h]]
           {:low l
            :high (dec (or h (inc (or (:sql/upper-bound task-map) n))))
            :table (:sql/table task-map)
            :id (:sql/id task-map)})
         ranges)))

(defmethod l-ext/inject-lifecycle-resources :sql/partition-keys
  [_ {:keys [onyx.core/task-map onyx.core/log onyx.core/task-id] :as event}]
  (let [pool (task->pool task-map)
        ch (chan (or (:sql/read-buffer task-map) 1000))]
    (go
     (let [partitions (partition-table event)
           content {:chunks (count partitions)}]
       (extensions/write-chunk log :bootstrapped-index content task-id)

       (doseq [partition partitions]
         (let [content {:partition partition :status :incomplete}
               path (extensions/write-chunk log :bootstrapped-segment content task-id)]
           (>! ch {:partition partition :path path})))
       (>! ch :done)))
    {:sql/pool pool
     :sql/read-ch ch
     :sql/pending-messages (atom {})}))

(defmethod p-ext/read-batch :sql/partition-keys
  [_ {:keys [sql/read-ch sql/pending-messages onyx.core/task-map]}]
  (let [pending (count (keys @pending-messages))
        max-pending (or (:onyx/max-pending task-map) 10000)
        batch-size (:onyx/batch-size task-map)
        max-segments (min (- max-pending pending) batch-size)
        ms (or (:onyx/batch-timeout task-map) 50)
        timeout-ch (timeout ms)
        batch (->> (range max-segments)
                   (map (fn [_]
                          (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
                            {:id (java.util.UUID/randomUUID)
                             :input :sql
                             :message (:partition result)
                             :path (:path result)})))
                   (remove (comp nil? :message)))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m)
             {:message (:message m) :path (:path m)}))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message [:input :sql]
  [{:keys [sql/pending-messages onyx.core/log onyx.core/task-id]} message-id]
  (let [content {:status :acked}
        path (:path (get @pending-messages message-id))]
    (extensions/force-write-chunk log :bootstrapped-segment content path)
    (swap! pending-messages dissoc message-id)))

(defmethod p-ext/retry-message [:input :sql]
  [{:keys [sql/pending-messages core.async/read-ch onyx.core/log]} message-id]
  (let [snapshot @pending-messages
        message (get snapshot message-id)
        content {:status :incomplete :partition (:partition message)}]
    (extensions/force-write-chunk log :bootstrapped-segment content (:path message))
    (swap! pending-messages dissoc message-id)
    (>!! read-ch {:partition (:partition message) :path (:path message)})))

(defmethod p-ext/pending? [:input :sql]
  [{:keys [sql/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? [:input :sql]
  [{:keys [sql/pending-messages]}]
  (let [x @pending-messages]
    (and (= (count (keys x)) 1)
         (= (first (vals x)) :done))))

(defmethod l-ext/inject-lifecycle-resources
  :sql/read-rows
  [_ {:keys [onyx.core/task-map] :as event}]
  (let [pool (task->pool task-map)]
    {:sql/pool pool
     :onyx.core/params [pool]}))

(defmethod l-ext/inject-lifecycle-resources
  :sql/write-rows
  [_ {:keys [onyx.core/task-map] :as event}]
  {:sql/pool (task->pool task-map)})

(defmethod l-ext/close-lifecycle-resources
  :sql/partition-keys
  [_ {:keys [sql/pool] :as event}]
  (.close (:datasource pool))
  {})

(defmethod l-ext/close-lifecycle-resources
  :sql/read-rows
  [_ {:keys [sql/pool] :as event}]
  (.close (:datasource pool))
  {})

(defmethod l-ext/close-lifecycle-resources
  :sql/write-rows
  [_ {:keys [sql/pool] :as event}]
  (.close (:datasource pool))
  {})

(defn read-rows [pool {:keys [table id low high] :as segment}]
  (let [sql-map {:select [:*]
                 :from [table]
                 :where [:and
                         [:>= id low]
                         [:<= id high]]}]
    (jdbc/query pool (sql/format sql-map))))

(defmethod p-ext/write-batch [:output :sql]
  [{:keys [onyx.core/compressed onyx.core/task-map sql/pool] :as event}]
  (jdbc/with-db-transaction
    [conn pool]
    (doseq [segment compressed]
      (doseq [row (:rows segment)]
        (jdbc/insert! conn (:sql/table task-map) row))))
  {:onyx.core/written? true})

