## onyx-sql

Onyx plugin providing read and write facilities for SQL databases with JDBC support.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-sql "0.14.0.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.sql])
```

#### Overview

A few notes are warranted on the read functionality of this plugin. Efficient distributed reads are facilitated using the assumption of the presence of a monotonically increasing integer key column on the replicated table. This allows the plugin to evenly break up a database table into chunks which can be read by multiple peers.

Because of this design, two functions are required to read from a database table. The first, `partition-keys` or `partition-keys-by-uuid` take care of breaking up the database table into discrete chunks. `partition-keys` is used to partition a table with an integer key, while `partition-keys-by-uuid` is used to partition a table with an UUID key. The second, `read-rows`, takes the output of `partition-keys` or `partition-keys-by-uuid` and performs the actual reading of rows based on these partitions. Note, `partition-keys-by-uuid` is only supported on MySQL.

If there's any possibility of data being modified in the table being read from while the job runs, it's recommended that you obtain a lock on the table to ensure data consistency. This is best accomplished by adding lifecycle functions to the `read-rows` step which lock and unlock the table before and after the job's start and completion (respectively).

Reading from tables without an integer key column is [currently not supported](https://github.com/onyx-platform/onyx-sql/issues/8), though there's the possibility of work in this direction. In the mean time, if you don't have an integer key column it's recommended you use the [onyx-seq](https://github.com/onyx-platform/onyx/blob/0.12.x/src/onyx/plugin/seq.clj) plugin included in onyx core, and read rows in using a single `SELECT` query. Keep in mind that this restricts you to a single reader, which may consequently become a bottleneck in your throughput. Also, keep in mind that in this approach, it's still advised to lock the database table for consistency over the course of the read.

#### Functions

Below are the details of the functions offered by this plugin.

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
 :sql/columns [:*]
 ;; Optional
 :sql/lower-bound selected-min
 ;; Optional
 :sql/upper-bound selected-max
 ;; 500 * 1000 = 50,000 rows
 ;; to be processed within :onyx/pending-timeout, 60s by default
 :sql/rows-per-segment 500
 :onyx/max-pending 1000
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Partitions a range of primary keys into subranges"}
```

`:sql/columns` supports restricting the select to only certain columns, e.g. `:sql/columns [:id :name]`.

`:sql/lower-bound` overrides `partition-key` calculation of min from the `:sql/id` column.

`:sql/upper-bound` overrides `partition-key` calculation of max from the `:sql/id` column.

##### read-rows

Reads a partition of a rows from a SQL table.

Catalog entry:

```clojure
{:onyx/name :read-rows
 :onyx/tenancy-ident :sql/read-rows
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

##### write-rows

Writes segments to a SQL database. Expects segments with the same schema as the
table they are being inserted into. They must be batched together into a
single segment.


``` clojure
{:rows [{:id 1 :column1 "hello" :column2 "world}]}
```

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
 :sql/copy? boolean
 :sql/copy-fields [:first :second :third]
 :onyx/batch-size batch-size
 :onyx/doc "Writes segments from the :rows keys to the SQL database"}
```

When `sql/copy?` is true, attempts to import rows using the `COPY` statement for
supported databases (currently only PostgreSQL). If enables, expects
`sql/copy-fields` to be a vector that determines the order in which columns will
be rendered.

##### upsert-rows

Upserts segments to a SQL database. The `:where` map must include the primary
key. If the primary key's value exists, an update will be performed. If it does
not exist, an insert will be performed. Upserts work with MySQL and PostgreSQL
databases.

In the example below `:id` is the primary key.

``` clojure
{:rows [{:column1 "hello" :column2 "world}] :where {:id 1}}
```

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

#### Attributes

|key                     | type      | description
|------------------------|-----------|------------
|`:sql/classname`        | `string`  | The JDBC classname driver to use
|`:sql/subprotocol`      | `string`  | The database JDBC subprotocol
|`:sql/subname`          | `string`  | The subname to connect to
|`:sql/user`             | `string`  | The user to authenticate with
|`:sql/password`         | `string`  | The password to authenticate with
|`:sql/table`            | `keyword` | The table to read/write from/to
|`:sql/columns`          | `vector`  | Columns to select
|`:sql/id`               | `keyword` | The name of a unique, monotonically increasing integer column
|`:sql/lower-bound` | `integer` | Overrides the calculation of min value from the id column.
|`:sql/upper-bound` | `integer` | Overrides the calculation of max value from the id column.
|`:sql/rows-per-segment` | `integer` | the number of rows to compress into a single segment
|`:sql/read-buffer`      | `integer` | The number of messages to buffer via core.async, default is `1000`

#### Contributing

Pull requests into the master branch are welcomed.

Running the tests can be easily performed by starting a mysql and postgres container with docker:
```
 docker run -e MYSQL_ROOT_PASSWORD="password" -e MYSQL_DATABASE="onyx_input_test" -p 3306:3306 -d mysql:latest
 docker run -e POSTGRES_PASSWORD=password -e POSTGRES_USER=postgresql -e POSTGRES_DB=onyx_input_test -p 5432:5432 -d library/postgres
 ```

#### License

Copyright © 2016 Distributed Masonry
