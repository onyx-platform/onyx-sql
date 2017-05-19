(ns onyx.plugin.input-test
  (:require [aero.core :refer [read-config]]
            [clojure.java
             [io :as io]
             [jdbc :as jdbc]]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.tasks
             [sql :as sql]
             [core-async :as ca]]
            [onyx.plugin
             [sql]
             [core-async :refer [take-segments! get-core-async-channels]]])
  (:import com.mchange.v2.c3p0.ComboPooledDataSource))

(defn build-job [db-user db-pass db-sub-base db-name batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        sql-settings {:sql/classname "com.mysql.jdbc.Driver"
                      :sql/subprotocol "mysql"
                      :sql/subname (str db-sub-base "/" db-name)
                      :sql/user db-user
                      :sql/password db-pass
                      :sql/table :people}
        base-job {:workflow [[:partition-keys :read-rows]
                             [:read-rows :capitalize]
                             [:capitalize :persist]]
                  :catalog [{:onyx/name :capitalize
                             :onyx/fn :onyx.plugin.input-test/capitalize
                             :onyx/type :function
                             :onyx/batch-size 10
                             :onyx/doc "Capitilizes the :name key"}]
                  :lifecycles [{:lifecycle/task :partition-keys
                                :lifecycle/calls ::read-crash}]
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (sql/partition-keys :partition-keys (merge {:sql/id :id
                                                              :sql/columns [:name]
                                                              :sql/rows-per-segment 2}
                                                             sql-settings
                                                             batch-settings)))
        (add-task (sql/read-rows :read-rows (merge {:sql/id :id}
                                                   sql-settings
                                                   batch-settings)))
        (add-task (ca/output :persist batch-settings)))))

(def batch-num (atom 0))
(def read-crash
  {:lifecycle/before-batch
   (fn [event lifecycle]
     (Thread/sleep 7000)
     (when (= (swap! batch-num inc) 2)
       (throw (ex-info "Restartable" {:restartable? true})))
     {})
   :lifecycle/handle-exception (constantly :restart)})

(defn capitalize [segment]
  (update-in segment [:name] clojure.string/upper-case))

(defn pool [spec]
  {:datasource
   (doto (ComboPooledDataSource.)
     (.setDriverClass (:classname spec))
     (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
     (.setUser (:user spec))
     (.setPassword (:password spec))
     (.setMaxIdleTimeExcessConnections (* 30 60))
     (.setMaxIdleTime (* 3 60 60)))})

(defn ensure-database! [db-user db-pass db-sub-base db-name]
  (let [db-spec {:classname "com.mysql.jdbc.Driver"
                 :subprotocol "mysql"
                 :subname db-sub-base
                 :user db-user
                 :password db-pass}
        cpool (pool db-spec)]
    (try
      (jdbc/execute! cpool [(str "drop database " db-name)])
      (catch Exception e
        (.printStackTrace e)))
    (jdbc/execute! cpool [(str "create database " db-name)])
    (jdbc/execute! cpool [(str "use " db-name)]))
  (let [db-spec {:classname "com.mysql.jdbc.Driver"
                 :subprotocol "mysql"
                 :subname (str db-sub-base "/" db-name)
                 :user db-user
                 :password db-pass}
        cpool (pool db-spec)
        values (mapv str (range 50))]
    (jdbc/execute!
     cpool
     (vector (jdbc/create-table-ddl
              :people
              [:id :int "PRIMARY KEY AUTO_INCREMENT"]
              [:name "VARCHAR(32)"])))
    (doseq [person values]
      (jdbc/insert! cpool :people {:name person}))))

(deftest sql-input-test
  (let [{:keys [env-config peer-config sql-config]} (read-config
                                                     (io/resource "config.edn")
                                                     {:profile :test})
        {:keys [sql/username sql/password sql/subname sql/db-name]} sql-config
        job (build-job username password subname db-name 10 1000)
        {:keys [persist]} (get-core-async-channels job)]
    (with-test-env [test-env [4 env-config peer-config]]
      (ensure-database! username password subname db-name)
      (onyx.test-helper/validate-enough-peers! test-env job)
      (onyx.api/submit-job peer-config job)
      (is (= (sort (map :name (butlast (take-segments! persist 10000))))
             (sort (mapv str (range 50))))))))
