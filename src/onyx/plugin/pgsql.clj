(ns onyx.plugin.pgsql
  "Implementation of PostgreSQL-specific utility functions. Use `available?` to determine if this module
   can be used."

  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [honeysql.core :as hsql]
            [honeysql-postgres.format]))

(def ht-byte (.getBytes (str "\t") "UTF-8"))
(def nl-byte (.getBytes (str "\n") "UTF-8"))

(def available?
  "True when necessary PostgreSQL JDBC libraries are available and this module can safely
  be used."

  (try
    (import '[org.postgresql.jdbc PgConnection]
            '[org.postgresql.util PGobject]
            '[org.postgresql.copy CopyManager
                                  PGCopyOutputStream])
    true
    (catch Throwable e
      (println "unabable to include postgresql: " (.getMessage e))
      false)))

(when available?
  (defn- column->sql-value
    "Coerces a column a format that's understood by the PostgreSQL COPY TEXT format."
    [col]
    (let [xform-escape (fn [x]
                         (cond
                           (string? x)
                           (.replace x "\\" "\\\\")

                           (= (type x) PGobject)
                           (.replace (.toString (cast PGobject x)) "\\" "\\\\")

                           :else
                           x))
          xform-null (fn [x]
                       (if (nil? x) "\\N" x))

          xform (comp str
                      xform-null
                      xform-escape
                      jdbc/sql-value)]
      (xform col)))

  (defn copy
    "Uses PostgreSQL CopyManager to quickly load batches of rows into our destination table. Transaction
   is guaranteed to be committed after function returns."
    [table cols conn rows]

    (let [pgconn (.unwrap (:connection conn) PgConnection)
              copyman (.getCopyAPI pgconn)
              copy (.copyIn copyman (str "COPY " (name table) " FROM STDIN"))
              ostream (PGCopyOutputStream. copy)]

          (doseq [row rows]
            (loop [values (map #(% row) cols)]
              (let [cur (column->sql-value (first values))
                    more (next values)
                    buf (.getBytes cur "UTF-8")]
                (.write ostream buf)

                (when more
                  (.write ostream ht-byte)
                  (recur more))))

            (.write ostream nl-byte))
          (.endCopy ostream))))

(defn upsert
  "Using honeysql-postgres, construct a SQL string to do upserts with Postgres.

  We expect the key(s) in the 'where' map to be primary keys or otherwise
  raise the conflict to perform the update."
  [table row where]
  (hsql/format {:insert-into table
                :values [(merge where row)]
                :upsert {:on-conflict (keys where)
                         :do-update-set (keys row)}}))
