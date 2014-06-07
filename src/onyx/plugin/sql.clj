(ns onyx.plugin.sql
  (:require [honeysql.core :as sql]
            [clojure.java.jdbc :as jdbc]
            [onyx.peer.task-lifecycle-extensions :as l-ext])
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

(defmethod l-ext/inject-lifecycle-resources
  :sql/read-rows
  [_ {:keys [onyx.core/task-map] :as pipeline}]
  (let [pool (task->pool task-map)]
    {:sql/pool pool
     :onyx.core/params [pool]}))

(defmethod l-ext/inject-lifecycle-resources
  :sql/write-rows
  [_ {:keys [onyx.core/task-map] :as pipeline}]
  {:sql/pool (task->pool task-map)})

(defmethod l-ext/close-lifecycle-resources
  :sql/read-rows
  [_ {:keys [sql/pool] :as pipeline}]
  (.close (:datasource pool))
  {})

(defmethod l-ext/close-lifecycle-resources
  :sql/write-rows
  [_ {:keys [sql/pool] :as pipeline}]
  (.close (:datasource pool))
  {})

(defmethod l-ext/apply-fn [:input :sql]
  [{:keys [onyx.core/task-map] :as pipeline}]
  (let [pool (task->pool task-map)
        sql-map {:select [:%count.*] :from [(:sql/table task-map)]}
        n ((keyword "count(*)") (first (jdbc/query pool (sql/format sql-map))))
        ranges (partition-all 2 1 (range (or (:sql/lower-bound task-map) 1)
                                         (or (:sql/upper-bound task-map) n)
                                         (:sql/rows-per-segment task-map)))]
    {:onyx.core/results
     (map (fn [[l h]]
            {:low l
             :high (dec (or h (inc (or (:sql/upper-bound task-map) n))))
             :table (:sql/table task-map)
             :id (:sql/id task-map)})
          ranges)}))

(defn read-rows [pool {:keys [table id low high] :as segment}]
  (let [sql-map {:select [:*]
                 :from [table]
                 :where [:and
                         [:>= id low]
                         [:<= id high]]}]
    {:rows (jdbc/query pool (sql/format sql-map))}))

(defmethod l-ext/apply-fn [:output :sql]
  [_]
  {})

(defmethod l-ext/compress-batch [:output :sql]
  [{:keys [onyx.core/decompressed] :as pipeline}]
  {:onyx.core/compressed decompressed})

(defmethod l-ext/write-batch [:output :sql]
  [{:keys [onyx.core/compressed onyx.core/task-map sql/pool] :as pipeline}]
  (doseq [segment compressed]
    (doseq [row (:rows segment)]
      (jdbc/insert! pool (:sql/table task-map) row)))
  {:onyx.core/written? true})

