(ns onyx.plugin.sql
  (:require [honeysql.core :as sql]
            [onyx.peer.pipeline-extensions :as p-ext]))

(defmethod p-ext/inject-pipeline-resources
  :sql/load-rows
  [{:keys [] :as pipeline}]
  {:params []})

(defmethod p-ext/apply-fn
  {:onyx/type :database
   :onyx/direction :input
   :onyx/medium :sql}
  [{:keys [] :as pipeline}])

