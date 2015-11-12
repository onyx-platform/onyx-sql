## onyx-sql

Onyx plugin providing read and write facilities for SQL databases with JDBC support.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-sql "0.8.0.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.sql])
```

#### Functions

##### partition-keys

Partitions a table of rows into chunks to be read by another task. Requires a column in the table to be numerically ordered.

Catalog entry:

```clojure
{:onyx/name :partition-keys
 :onyx/plugin :onyx.plugin.sql/partition-keys
 :onyx/type :input
 :onyx/medium :sql
 :sql/classname "my.class.name"
 :sql/subprotocol "db-sub-protocol"
 :sql/subname "db-subname"
 :sql/user "db-user"
 :sql/password "db-pass"
 :sql/table :table-name
 :sql/id :column-to-split-by
 :sql/rows-per-segment 1000
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Partitions a range of primary keys into subranges"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :partition-keys
 :lifecycle/calls :onyx.plugin.sql/partition-keys-calls}
```

##### read-rows

Reads a partition of a rows from a SQL table.

Catalog entry:

```clojure
{:onyx/name :read-rows
 :onyx/ident :sql/read-rows
 :onyx/fn :onyx.plugin.sql/read-rows
 :onyx/type :function
 :onyx/batch-size batch-size
 :sql/classname "my.class.name"
 :sql/subprotocol "db-subprotocol"
 :sql/subname "db-sub-name"
 :sql/user "db-user"
 :sql/password "db-pass"
 :sql/table :table-name
 :sql/id :column-to-split-by
 :onyx/doc "Reads rows of a SQL table bounded by a key range"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :read-rows
 :lifecycle/calls :onyx.plugin.sql/read-rows-calls}
```

##### write-rows

Writes segments to a SQL database.

Catalog entry:

```clojure
{:onyx/name :write-rows
 :onyx/plugin :onyx.plugin.sql/write-rows
 :onyx/type :output
 :onyx/medium :sql
 :sql/classname "my.class.name"
 :sql/subprotocol "db-subprotocol"
 :sql/subname "db-sub-name"
 :sql/user "db-user"
 :sql/password "db-pass"
 :sql/table :table-name
 :onyx/batch-size batch-size
 :onyx/doc "Writes segments from the :rows keys to the SQL database"}
```

##### upsert-

Upsert segments to a SQL database.

Catalog entry:

```clojure
{:onyx/name :write-rows
 :onyx/plugin :onyx.plugin.sql/upsert-rows
 :onyx/type :output
 :onyx/medium :sql
 :sql/classname "my.class.name"
 :sql/subprotocol "db-subprotocol"
 :sql/subname "db-sub-name"
 :sql/user "db-user"
 :sql/password "db-pass"
 :sql/table :table-name
 :onyx/batch-size batch-size
 :onyx/doc "Upserts segments from the :rows keys to the SQL database"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-rows
 :lifecycle/calls :onyx.plugin.sql/write-rows-calls}
```

#### Attributes

|key                     | type      | description
|------------------------|-----------|------------
|`:sql/classname`        | `string`  | The JDBC classname driver to use
|`:sql/subprotocol`      | `string`  | The database JDBC subprotool
|`:sql/subname`          | `string`  | The subname to connect to
|`:sql/user`             | `string`  | The user to authenticate with
|`:sql/password`         | `string`  | The password to authenticate with
|`:sql/table`            | `keyword` | The table to read/write from/to
|`:sql/id`               | `keyword` | The name of a unique, monotonically increasing integer column
|`:sql/rows-per-segment` | `integer` | The number of rows to compress into a single segment
|`:sql/read-buffer`      | `integer` | The number of messages to buffer via core.async, default is `1000`

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
