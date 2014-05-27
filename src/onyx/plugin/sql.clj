(ns onyx.plugin.sql
  (:require [honeysql.core :as sql]
            [clojure.java.jdbc :as jdbc]
            [onyx.peer.pipeline-extensions :as p-ext])
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

(defmethod p-ext/inject-pipeline-resources
  :sql/load-rows
  [{:keys [task-map] :as pipeline}]
  {:params [(task->pool task-map)]})

(defmethod p-ext/apply-fn [:input :sql]
  [{:keys [task-map] :as pipeline}]
  (let [pool (task->pool task-map)
        sql-map {:select [:%count.*] :from [(:sql/table task-map)]}
        n ((keyword "count(*)") (first (jdbc/query pool (sql/format sql-map))))
        ranges (partition-all 2 1 (range (or (:sql/lower-bound task-map) 1)
                                         (or (:sql/upper-bound task-map) n)
                                         (:onyx/batch-size task-map)))]
    {:results
     (map (fn [[l h]]
            {:low l
             :high (dec (or h (inc (or (:sql/upper-bound task-map) n))))
             :table (:sql/table task-map)
             :id (:sql/id task-map)})
          ranges)}))

(defn load-rows [pool {:keys [table id low high] :as segment}]
  (let [sql-map {:select [:*]
                 :from [table]
                 :where [:and
                         [:>= id low]
                         [:<= id high]]}]
    {:rows (jdbc/query pool (sql/format sql-map))}))

