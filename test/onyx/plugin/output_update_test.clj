(ns onyx.plugin.output-update-test
  (:require [aero.core :refer [read-config]]
            [honeysql.core :as honey]
            [clojure.core.async :refer [pipe]]
            [clojure.core.async.lab :refer [spool]]
            [clojure.java
             [io :as io]
             [jdbc :as jdbc]]
            [clojure.test :refer [deftest is]]
            [clojure.core.async :refer [pipe]]
            [clojure.core.async.lab :refer [spool]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.tasks
             [sql :as sql]
             [core-async :as ca]]
            [onyx.plugin
             [sql]
             [core-async :refer [take-segments! get-core-async-channels]]])
  (:import [com.mchange.v2.c3p0 ComboPooledDataSource]))

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
                             :onyx/fn :onyx.plugin.output-update-test/transform
                             :onyx/type :function
                             :onyx/batch-size 1000
                             :onyx/doc "Transforms a segment to prepare for SQL persistence"}]
                  :lifecycles [{:lifecycle/task :out
                                :lifecycle/calls :onyx.plugin.sql/upsert-rows-calls}]
                  :windows []
                  :triggers []
                  :flow-conditions []
                  :task-scheduler :onyx.task-scheduler/balanced}]
    (-> base-job
        (add-task (ca/input :in batch-settings))
        (add-task (sql/upsert-rows :out (merge sql-settings batch-settings))))))

(defn transform [{:keys [id word] :as segment}]
  {:rows [{:word (str word "!")}] :where {:id id}})

#_(def db-name (or (env :test-db-name) "onyx_output_update_test"))

(defn pool [spec]
  {:datasource
   (doto (ComboPooledDataSource.)
     (.setDriverClass (:classname spec))
     (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
     (.setUser (:user spec))
     (.setPassword (:password spec))
     (.setMaxIdleTimeExcessConnections (* 30 60))
     (.setMaxIdleTime (* 3 60 60)))})

(def words
  [{:id 1 :word "Cat"}
   {:id 2 :word "Orange"}
   {:id 3 :word "Pan"}
   {:id 4 :word "Door"}
   {:id 5 :word "Surf board"}
   :done])

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
        values (mapv str (range 5000))
        truncated-words (butlast words)]
    (jdbc/execute!
     cpool
     (vector (jdbc/create-table-ddl
              :words
              [:id :int]
              [:word "VARCHAR(32)"])))
    (doseq [word truncated-words]
      (jdbc/insert! cpool :words word))))


(defn transform-word
  [word]
  {:id (:id word) :word (str (:word word) "!")})

(deftest sql-update-output-test
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
             (map transform-word (butlast words)))))))
