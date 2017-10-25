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

             :sql/db-name
             {:type :string
              :doc "The database to connect to."}

             :sql/table
             {:type :keyword
              :doc "The table to read/write from/to."}

             :sql/id
             {:type :keyword
              :doc "the name of a unique, monotonically increasing integer column."}

             :sql/lower-bound
             {:type :integer
              :doc "Overrides calculation of min value from the id column."}

             :sql/upper-bound
             {:type :integer
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
              :optional? true
              :doc "The user to authenticate with."}

             :sql/password
             {:type :string
              :doc "The password to authenticate with."}

             :sql/db-name
             {:type :string
              :optional? true
              :doc "The database to connect to."}

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
              :optional? true
              :doc "The user to authenticate with."}

             :sql/password
             {:type :string
              :doc "The password to authenticate with."}

             :sql/db-name
             {:type :string
              :optional? true
              :doc "The database to connect to."}

             :sql/table
             {:type :keyword
              :doc "The table to read/write from/to."}

             :sql/copy?
             {:type :boolean
              :optional? true
              :doc "Whether or not to use COPY for batch inserts. Note: currently only supported for PostgreSQL."}

             :sql/copy-columns
             {:type :vector
              :optional? true
              :doc "When copy? is true, describes the order in which the columns are sent to the db."}}}

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

             :sql/db-name
             {:type :string
              :doc "The database to connect to."}

             :sql/table
             {:type :keyword
              :doc "The table to read/write from/to."}}}}

   :lifecycles-entry
   {:onyx.plugin.sql/partition-keys
    {:model
     []}

    :onyx.plugin.sql/read-rows
    {:model
     []}

    :onyx.plugin.sql/write-rows
    {:model
     []}

    :onyx.plugin.sql/upsert-rows
    {:model
     []}}

   :display-order
   {:onyx.plugin.sql/partition-keys
    [:sql/classname
     :sql/subname
     :sql/subprotocol
     :sql/user
     :sql/password
     :sql/db-name
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
     :sql/db-name
     :sql/table
     :sql/id]

    :onyx.plugin.sql/write-rows
    [:sql/classname
     :sql/subname
     :sql/subprotocol
     :sql/user
     :sql/password
     :sql/db-name
     :sql/table
     :sql/copy?
     :sql/copy-columns]

    :onyx.plugin.sql/upsert-rows
    [:sql/classname
     :sql/subname
     :sql/subprotocol
     :sql/user
     :sql/password
     :sql/db-name
     :sql/table]}})
