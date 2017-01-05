(ns onyx.sql.information-model)

(def model
  {:catalog-entry
   {:onyx.plugin.sql/partition-keys
    {:summary "Partitions a table of rows into chunks to be read by another task. Requires a column in the table to be numerically ordered."
     :model {:sql/classname
             {:type :string
              :doc "The JDBC classname driver to use."}

             :sql/subprotocol
             {:type :string
              :doc "The database JDBC subprotocol."}

             :sql/subname
             {:type :string
              :doc "The subname to connect to."}

             :sql/user
             {:type :string
              :doc "The user to authenticate with."}

             :sql/password
             {:type :string
              :doc "The password to authenticate with."}

             :sql/table
             {:type :keyword
              :doc "The table to read/write from/to."}

             :sql/id
             {:type :keyword
              :doc "the name of a unique, monotonically increasing integer column."}

             :sql/lower-bound
             {:type :integer
              :optional? true
              :doc "Overrides calculation of min value from the id column."}

             :sql/upper-bound
             {:type :integer
              :optional? true
              :doc "Overrides calculation of max value from the id column."}

             :sql/rows-per-segment
             {:type :integer
              :optional? true
              :default 1000
              :doc "The number of rows to compress into a single segment."}

             :sql/read-buffer
             {:type :integer
              :optional? true
              :default 1000
              :doc "The number of messages to buffer via core.async, default is `1000`."}}}
    :onyx.plugin.sql/read-rows
    {:summary "Reads a partition of a rows from a SQL table."
     :model {:sql/classname
             {:type :string
              :doc "The JDBC classname driver to use."}

             :sql/subprotocol
             {:type :string
              :doc "The database JDBC subprotocol."}

             :sql/subname
             {:type :string
              :doc "The subname to connect to."}

             :sql/user
             {:type :string
              :doc "The user to authenticate with."}

             :sql/password
             {:type :string
              :doc "The password to authenticate with."}

             :sql/table
             {:type :keyword
              :doc "The table to read/write from/to."}

             :sql/id
             {:type :keyword
              :doc "The name of a unique, monotonically increasing integer column."}}}

    :onyx.plugin.sql/write-rows
    {:summary "Writes segments to a SQL database. Expects segments with the same schema as the table they are being inserted into. They must be batched together into a single segment."
     :model {:sql/classname
             {:type :string
              :doc "The JDBC classname driver to use."}

             :sql/subprotocol
             {:type :string
              :doc "The database JDBC subprotocol."}

             :sql/subname
             {:type :string
              :doc "The subname to connect to."}

             :sql/user
             {:type :string
              :doc "The user to authenticate with."}

             :sql/password
             {:type :string
              :doc "The password to authenticate with."}

             :sql/table
             {:type :keyword
              :doc "The table to read/write from/to."}}}

    :onyx.plugin.sql/write-batch
    {:summary "Like `write-rows`, except the entire batch of segments in the lifecycle are written together in one transaction. Thus, each segment is itself a row,and collecting segments together with `:rows` is not required. "
     :model {:sql/classname
             {:type :string
              :doc "The JDBC classname driver to use."}

             :sql/subprotocol
             {:type :string
              :doc "The database JDBC subprotocol."}

             :sql/subname
             {:type :string
              :doc "The subname to connect to."}

             :sql/user
             {:type :string
              :doc "The user to authenticate with."}

             :sql/password
             {:type :string
              :doc "The password to authenticate with."}

             :sql/table
             {:type :keyword
              :doc "The table to read/write from/to."}}}

    :onyx.plugin.sql/upsert-rows
    {:summary "Upserts segments to a SQL database."
     :model {:sql/classname
             {:type :string
              :doc "The JDBC classname driver to use."}

             :sql/subprotocol
             {:type :string
              :doc "The database JDBC subprotocol."}

             :sql/subname
             {:type :string
              :doc "The subname to connect to."}

             :sql/user
             {:type :string
              :doc "The user to authenticate with."}

             :sql/password
             {:type :string
              :doc "The password to authenticate with."}

             :sql/table
             {:type :keyword
              :doc "The table to read/write from/to."}}}}

   :lifecycles-entry
   {:onyx.plugin.sql/partition-keys
    {:model
     [{:task.lifecycle/name :partition-keys
       :lifecycle/calls :onyx.plugin.sql/partition-keys-calls}]}

    :onyx.plugin.sql/read-rows
    {:model
     [{:task.lifecycle/name :read-rows
       :lifecycle/calls :onyx.plugin.sql/read-rows-calls}]}

    :onyx.plugin.sql/write-rows
    {:model
     [{:task.lifecycle/name :write-rows
       :lifecycle/calls :onyx.plugin.sql/write-rows-calls}]}

    :onyx.plugin.sql/upsert-rows
    {:model
     [{:task.lifecycle/name :upsert-rows
       :lifecycle/calls :onyx.plugin.sql/upsert-rows-calls}]}

    :onyx.plugin.sql/write-batch
    {:model
     [{:task.lifecycle/name :write-batch
       :lifecycle/calls :onyx.plugin.sql/write-rows-calls}]}}

   :display-order
   {:onyx.plugin.sql/partition-keys
    [:sql/classname
     :sql/subname
     :sql/subprotocol
     :sql/user
     :sql/password
     :sql/table
     :sql/id
     :sql/lower-bound
     :sql/upper-bound
     :sql/rows-per-segment
     :sql/read-buffer]

    :onyx.plugin.sql/read-rows
    [:sql/classname
     :sql/subname
     :sql/subprotocol
     :sql/user
     :sql/password
     :sql/table
     :sql/id]

    :onyx.plugin.sql/write-rows
    [:sql/classname
     :sql/subname
     :sql/subprotocol
     :sql/user
     :sql/password
     :sql/table]

    :onyx.plugin.sql/upsert-rows
    [:sql/classname
     :sql/subname
     :sql/subprotocol
     :sql/user
     :sql/password
     :sql/table]

    :onyx.plugin.sql/write-batch
    [:sql/classname
     :sql/subname
     :sql/subprotocol
     :sql/user
     :sql/password
     :sql/table]}})
