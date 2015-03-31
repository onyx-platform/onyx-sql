(ns onyx.plugin.sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :refer [chan >! >!! <!! close! go timeout alts!!]]
            [honeysql.core :as sql]
            [taoensso.timbre :refer [fatal]]
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
        table (name (:sql/table task-map))
        id-col (name (:sql/id task-map))
        n-min (or (:sql/lower-bound task-map)
                  (:min (first (jdbc/query pool (vector (format "select min(%s) as min from %s" id-col table))))))
        n-max (or (:sql/upper-bound task-map)
                  (:max (first (jdbc/query pool (vector (format "select max(%s) as max from %s" id-col table))))))
        ranges (partition-all 2 1 (range n-min n-max (:sql/rows-per-segment task-map)))]
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
     (try
       (let [partitions (partition-table (assoc event :sql/pool pool))]
         (extensions/write-chunk log :chunk-index (count partitions) task-id)

         (doseq [partition partitions]
           (let [content {:partition partition :status :incomplete}
                 chunk-id (extensions/write-chunk log :chunk content task-id)]
             (>! ch {:content content :chunk-id chunk-id})))
         (>! ch :done))
       (catch Exception e
         (fatal e))))
    {:sql/pool pool
     :sql/read-ch ch
     :sql/pending-messages (atom {})}))

(defmethod p-ext/read-batch [:input :sql]
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
                               :message :done}
                              {:id (java.util.UUID/randomUUID)
                               :input :sql
                               :message (:partition (:content result))
                               :chunk-id (:chunk-id result)}))))
                   (remove (comp nil? :message)))]
    (doseq [m batch]
      (swap! pending-messages assoc (:id m) (select-keys m [:message :chunk-id])))
    {:onyx.core/batch batch}))

(defmethod p-ext/ack-message [:input :sql]
  [{:keys [sql/pending-messages onyx.core/log onyx.core/task-id]} message-id]
  (try
    (if-let [chunk-id (:chunk-id (get @pending-messages message-id))]
      (let [content {:status :acked :id chunk-id}]
        (extensions/force-write-chunk log :chunk content task-id)
        (swap! pending-messages dissoc message-id))
      (swap! pending-messages dissoc message-id))
    (catch Exception e
      (fatal e))))

(defmethod p-ext/retry-message [:input :sql]
  [{:keys [sql/pending-messages sql/read-ch onyx.core/log]} message-id]
  (let [snapshot @pending-messages
        message (get snapshot message-id)
        content {:status :incomplete :partition (:partition message)}]
    (swap! pending-messages dissoc message-id)
    (if (:partition message)
      (do
        (extensions/force-write-chunk log :chunk content (:path message))
        (>!! read-ch {:partition (:partition message) :status :incomplete :path (:path message)}))
      (>!! read-ch :done))))

(defmethod p-ext/pending? [:input :sql]
  [{:keys [sql/pending-messages]} message-id]
  (get @pending-messages message-id))

(defmethod p-ext/drained? [:input :sql]
  [{:keys [sql/pending-messages]}]
  (let [y @pending-messages]
    (let [x @pending-messages]
      (and (= (count (keys x)) 1)
           (= (first (map :message (vals x))) :done)))))

(defmethod l-ext/inject-lifecycle-resources :sql/read-rows
  [_ {:keys [onyx.core/task-map] :as event}]
  (let [pool (task->pool task-map)]
    {:sql/pool pool
     :onyx.core/params [pool]}))

(defn read-rows [pool {:keys [table id low high] :as segment}]
  (let [sql-map {:select [:*]
                 :from [table]
                 :where [:and
                         [:>= id low]
                         [:<= id high]]}]
    (jdbc/query pool (sql/format sql-map))))

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

(defmethod p-ext/write-batch [:output :sql]
  [{:keys [onyx.core/compressed onyx.core/task-map sql/pool] :as event}]
  (jdbc/with-db-transaction
    [conn pool]
    (doseq [segment compressed]
      (doseq [row (:rows segment)]
        (jdbc/insert! conn (:sql/table task-map) row))))
  {:onyx.core/written? true})

