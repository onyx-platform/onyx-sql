## onyx-sql

Onyx plugin providing read and write facilities for SQL databases with JDBC support.

#### Installation

In your project file:

```clojure
[com.mdrogalis/onyx-sql "0.3.3"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.sql])
```

#### Catalog entries

##### partition-keys

```clojure
{:onyx/name :partition-keys
 :onyx/ident :sql/partition-keys
 :onyx/type :input
 :onyx/medium :sql
 :onyx/consumption :sequential
 :onyx/bootstrap? true
 :sql/classname "com.my.jdbc.Driver"
 :sql/subprotocol "my-subprotocol"
 :sql/subname "//my.sub.name:3306/db"
 :sql/user "user"
 :sql/password "password"
 :sql/table :table-name
 :sql/id :id-column
 :sql/rows-per-segment n
 :onyx/batch-size batch-size
 :onyx/doc "Partitions a range of primary keys into subranges"}
```

##### load-rows

```clojure
{:onyx/name :load-rows
 :onyx/ident :sql/load-rows
 :onyx/fn :onyx.plugin.sql/load-rows
 :onyx/type :transformer
 :onyx/consumption :concurrent
 :sql/classname "com.my.jdbc.Driver"
 :sql/subprotocol "my-subprotocol"
 :sql/subname "//my.sub.name:3306/db"
 :sql/user "user"
 :sql/password "password"
 :sql/table :table-name
 :sql/id :id-column
 :onyx/batch-size batch-size
 :onyx/doc "Reads rows of a SQL table bounded by a key range"}
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

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2014 Michael Drogalis

Distributed under the Eclipse Public License, the same as Clojure.
