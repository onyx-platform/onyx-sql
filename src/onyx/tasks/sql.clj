(ns onyx.tasks.sql
  (:require [schema.core :as s]
            [onyx.schema :as os]))

(def SqlPartitionKeysTaskMap
  {:sql/id s/Keyword
   (s/optional-key :sql/columns) [s/Keyword]
   (s/optional-key :sql/rows-per-segment) s/Num
   (s/optional-key :sql/lower-bound) (s/cond-pre s/Uuid s/Num)
   (s/optional-key :sql/upper-bound) (s/cond-pre s/Uuid s/Num)
   (s/optional-key :sql/read-buffer) s/Num
   (s/optional-key :sql/db-name) s/Str
   :sql/classname s/Str
   :sql/subprotocol s/Str
   :sql/subname s/Str
   :sql/user s/Str
   :sql/password s/Str
   :sql/table s/Keyword
   (os/restricted-ns :sql) s/Any})

(s/defn ^:always-validate partition-keys
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.sql/partition-keys
                             :onyx/type :input
                             :onyx/medium :sql
                             :sql/columns [:*]
                             :sql/rows-per-segment 500
                             :sql/read-buffer 1000
                             :onyx/doc "Partitions a range of primary keys into subranges"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.sql/partition-keys-calls}]}
    :schema {:task-map SqlPartitionKeysTaskMap}})
  ([task-name :- s/Keyword
    classname :- s/Str
    subprotocol :- s/Str
    subname :- s/Str
    user :- s/Str
    password :- s/Str
    db-name :- s/Str
    table :- s/Keyword
    id :- s/Keyword
    task-opts :- {s/Any s/Any}]
   (partition-keys task-name (merge {:sql/classname classname
                                     :sql/subprotocol subprotocol
                                     :sql/subname subname
                                     :sql/user user
                                     :sql/password password
                                     :sql/db-name db-name
                                     :sql/table table
                                     :sql/id id}
                                    task-opts))))

(s/defn ^:always-validate partition-keys-by-uuid
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
                         :lifecycle/calls :onyx.plugin.sql/partition-uuid-calls}]}
    :schema {:task-map SqlPartitionKeysTaskMap}})
  ([task-name :- s/Keyword
    classname :- s/Str
    subprotocol :- s/Str
    subname :- s/Str
    user :- s/Str
    password :- s/Str
    db-name :- s/Str
    table :- s/Keyword
    id :- s/Keyword
    columns :- [s/Keyword]
    rows-per-segment :- s/Num
    task-opts :- {s/Any s/Any}]
   (partition-keys-by-uuid task-name (merge {:sql/classname classname
                                             :sql/subprotocol subprotocol
                                             :sql/subname subname
                                             :sql/user user
                                             :sql/password password
                                             :sql/db-name db-name
                                             :sql/table table
                                             :sql/id id
                                             :sql/columns columns
                                             :sql/rows-per-segment rows-per-segment}
                                            task-opts))))

(def SqlReaderTaskMap
  {:sql/id s/Keyword
   :sql/classname s/Str
   :sql/subprotocol s/Str
   :sql/subname s/Str
   :sql/user s/Str
   :sql/password s/Str
   :sql/table s/Keyword
   (s/optional-key :sql/db-name) s/Str
   (os/restricted-ns :sql) s/Any})

(s/defn ^:always-validate read-rows
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/fn :onyx.plugin.sql/read-rows
                             :onyx/type :function
                             :onyx/doc "Reads rows of a SQL table bounded by a key range"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.sql/read-rows-calls}]}
    :schema {:task-map SqlReaderTaskMap}})
  ([task-name :- s/Keyword
    classname :- s/Str
    subprotocol :- s/Str
    subname :- s/Str
    user :- s/Str
    password :- s/Str
    db-name :- s/Str
    table :- s/Keyword
    id :- s/Keyword
    task-opts :- {s/Any s/Any}]
   (read-rows task-name (merge {:sql/classname classname
                                :sql/subprotocol subprotocol
                                :sql/subname subname
                                :sql/user user
                                :sql/password password
                                :sql/db-name db-name
                                :sql/table table
                                :sql/id id}
                               task-opts))))

(def SqlWriterTaskMap
  {:sql/classname s/Str
   :sql/subprotocol s/Str
   :sql/subname s/Str
   :sql/user s/Str
   :sql/password s/Str
   :sql/table s/Keyword
   (s/optional-key :sql/db-name) s/Str
   (os/restricted-ns :sql) s/Any})

(s/defn ^:always-validate write-rows
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.sql/write-rows
                             :onyx/type :output
                             :onyx/medium :sql
                             :onyx/doc "Writes segments from the :rows keys to the SQL database"}
                            opts)}
    :schema {:task-map SqlWriterTaskMap}})
  ([task-name :- s/Keyword
    classname :- s/Str
    subprotocol :- s/Str
    subname :- s/Str
    user :- s/Str
    password :- s/Str
    db-name :- s/Str
    table :- s/Keyword
    task-opts :- {s/Any s/Any}]
   (write-rows task-name (merge {:sql/classname classname
                                 :sql/subprotocol subprotocol
                                 :sql/subname subname
                                 :sql/user user
                                 :sql/password password
                                 :sql/db-name db-name
                                 :sql/table table}
                                task-opts))))

(def SqlUpserterTaskMap
  {:sql/classname s/Str
   :sql/subprotocol s/Str
   :sql/subname s/Str
   :sql/user s/Str
   :sql/password s/Str
   :sql/db-name s/Str
   :sql/table s/Keyword
   (os/restricted-ns :sql) s/Any})

(s/defn ^:always-validate upsert-rows
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.sql/upsert-rows
                             :onyx/type :output
                             :onyx/medium :sql
                             :onyx/doc "Writes segments from the :rows keys to the SQL database based on :where key"}
                            opts)}
    :schema {:task-map SqlUpserterTaskMap}})
  ([task-name :- s/Keyword
    classname :- s/Str
    subprotocol :- s/Str
    subname :- s/Str
    user :- s/Str
    password :- s/Str
    db-name :- s/Str
    table :- s/Keyword
    task-opts :- {s/Any s/Any}]
   (upsert-rows task-name (merge {:sql/classname classname
                                  :sql/subprotocol subprotocol
                                  :sql/subname subname
                                  :sql/user user
                                  :sql/password password
                                  :sql/db-name db-name
                                  :sql/table table}
                                 task-opts))))
