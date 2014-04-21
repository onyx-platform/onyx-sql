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

(defmethod p-ext/inject-pipeline-resources
  :sql/load-rows
  [{:keys [task-map] :as pipeline}]
  (let [db-spec {:classname (:sql/classname task-map)
                 :subprotocol (:sql/subprotocol task-map)
                 :subname (:sql/subname task-map)
                 :user (:sql/user task-map)
                 :password (:sql/password task-map)}]
    {:params [(create-pool db-spec)]}))

(defmethod p-ext/apply-fn
  {:onyx/type :database
   :onyx/direction :input
   :onyx/medium :sql}
  [{:keys [task-map] :as pipeline}]
  (let [db-spec {:classname (:sql/classname task-map)
                 :subprotocol (:sql/subprotocol task-map)
                 :subname (:sql/subname task-map)
                 :user (:sql/user task-map)
                 :password (:sql/password task-map)}
        pool (create-pool db-spec)
        sql-map {:select [:%count.*] :from [(:sql/table task-map)]}
        n (:count (jdbc/execute! (sql/format sql-map)))
        ranges (partition-all 2 1 (range (:sql/lower-bound task-map)
                                         (:sql/upper-bound task-map)
                                         (:sql/partition-size task-map)))]
    (map (fn [[l h]] {:low l :high (dec (or h (inc (:sql/upper-bound task-map))))}) ranges)))

