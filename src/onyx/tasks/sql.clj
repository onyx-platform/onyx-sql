(ns onyx.tasks.sql
  (:require [schema.core :as s]
            [onyx.schema :as os]))

(def UserTaskMapKey
  (os/build-allowed-key-ns :sql))

(def SqlConnectionSettings
  {:sql/classname s/Str
   :sql/subprotocol s/Str
   :sql/subname s/Str
   :sql/user s/Str
   :sql/password s/Str
   :sql/table s/Keyword})

(def SqlPartitionKeysTaskMap
  (s/->Both [os/TaskMap
             (merge
              {:sql/id s/Keyword
               :sql/columns [s/Keyword]
               :sql/rows-per-segment s/Num
               UserTaskMapKey s/Any}
              SqlConnectionSettings)]))

(s/defn ^:always-validate partition-keys
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.sql/partition-keys
                             :onyx/type :input
                             :onyx/medium :sql
                             :sql/columns [:*]
                             :sql/rows-per-segment 500
                             :onyx/doc "Partitions a range of primary keys into subranges"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.sql/partition-keys-calls}]}
    :schema {:task-map SqlPartitionKeysTaskMap
             :lifecycles [os/Lifecycle]}})
  ([task-name :- s/Keyword
    classname :- s/Str
    subprotocol :- s/Str
    subname :- s/Str
    user :- s/Str
    password :- s/Str
    table :- s/Keyword
    id :- s/Keyword
    columns :- [s/Keyword]
    rows-per-segment :- s/Num
    task-opts :- {s/Any s/Any}]
   (partition-keys task-name (merge {:sql/classname classname
                                     :sql/subprotocol subprotocol
                                     :sql/subname subname
                                     :sql/user user
                                     :sql/password password
                                     :sql/table table
                                     :sql/id id
                                     :sql/columns columns
                                     :sql/rows-per-segment rows-per-segment}
                                    task-opts))))

(def SqlReadRowsTaskMap
  (s/->Both [os/TaskMap
             (merge
              {:sql/id s/Keyword
               UserTaskMapKey s/Any}
              SqlConnectionSettings)]))

(s/defn ^:always-validate read-rows
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/fn :onyx.plugin.sql/read-rows
                             :onyx/type :function
                             :onyx/doc "Reads rows of a SQL table bounded by a key range"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.sql/read-rows-calls}]}
    :schema {:task-map SqlReadRowsTaskMap
             :lifecycles [os/Lifecycle]}})
  ([task-name :- s/Keyword
    classname :- s/Str
    subprotocol :- s/Str
    subname :- s/Str
    user :- s/Str
    password :- s/Str
    table :- s/Keyword
    id :- s/Keyword
    task-opts :- {s/Any s/Any}]
   (read-rows task-name (merge {:sql/classname classname
                                :sql/subprotocol subprotocol
                                :sql/subname subname
                                :sql/user user
                                :sql/password password
                                :sql/table table
                                :sql/id id}
                               task-opts))))
