(ns onyx.plugin.output-test
  (:require [aero.core :refer [read-config]]
            [honeysql.core :as honey]
            [clojure.java
             [io :as io]
             [jdbc :as jdbc]]
            [clojure.test :refer [deftest is]]
            [clojure.core.async :refer [pipe] :as async]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.tasks
             [sql :as sql]
             [core-async :as ca]]
            [onyx.plugin
             [sql]
             [core-async :refer [get-core-async-channels]]])
  (:import [com.mchange.v2.c3p0 ComboPooledDataSource]))

(defn spool
  ([s c]
     (async/go
      (loop [[f & r] s]
        (if f
          (do
            (async/>! c f)
            (recur r))
          (async/close! c))))
     c)
  ([s]
     (spool s (async/chan))))

(defn build-job [db-user db-pass db-sub-base db-name batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        sql-settings {:sql/classname "com.mysql.jdbc.Driver"
                      :sql/subprotocol "mysql"
                      :sql/subname (str db-sub-base "/" db-name)
                      :sql/user db-user
                      :sql/password db-pass
                      :sql/table :words}
        base-job {:workflow [[:in :transform]
                             [:transform :out]]
                  :catalog [{:onyx/name :transform
                             :onyx/fn :onyx.plugin.output-test/transform
                             :onyx/type :function
                             :onyx/batch-size batch-size
                             :onyx/doc "Transforms a segment to prepare for SQL persistence"}]
                  :lifecycles [{:lifecycle/task :out
                                :lifecycle/calls :onyx.plugin.sql/write-rows-calls}]
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (ca/input :in batch-settings))
        (add-task (sql/write-rows :out (merge sql-settings batch-settings))))))

(defn transform [{:keys [word] :as segment}]
  {:rows [{:word word}]})

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
        values (mapv str (range 5000))]
    (jdbc/execute!
     cpool
     (vector (jdbc/create-table-ddl
              :words
              [:id :int "PRIMARY KEY AUTO_INCREMENT"]
              [:word "VARCHAR(32)"])))))

(def words
  [{:word "Cat"}
   {:word "Orange"}
   {:word "Pan"}
   {:word "Door"}
   {:word "Surf board"}])

(deftest sql-output-test
  (let [{:keys [env-config peer-config sql-config]} (read-config
                                                     (io/resource "config.edn")
                                                     {:profile :test})
        {:keys [sql/username sql/password sql/subname sql/db-name]} sql-config
        job (build-job username password subname db-name 10 1000)
        {:keys [in]} (get-core-async-channels job)
        cpool (pool {:classname "com.mysql.jdbc.Driver"
                     :subprotocol "mysql"
                     :subname (str subname "/" db-name)
                     :user username
                     :password password})]
    (with-test-env [test-env [4 env-config peer-config]]
      (ensure-database! username password subname db-name)
      (pipe (spool words) in true)
      (onyx.test-helper/validate-enough-peers! test-env job)
      (->> (:job-id (onyx.api/submit-job peer-config job))
           (onyx.api/await-job-completion peer-config))
      (is (= (jdbc/query cpool (honey/format {:select [:*] :from [:words]}))
             (map-indexed (fn [k x] (assoc x :id (inc k))) words))))))
