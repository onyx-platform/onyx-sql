(ns onyx.plugin.mysql
  "Implementation of MySQL-specific utility functions."

  (:require [honeysql.core :as hsql]
            [honeysql.format :as hformat]))

(defn upsert
  "Using honeysql-postgres, construct a SQL string to do upserts with Postgres.
  We expect the key in the 'where' map to be a primary key."
  [table row where]
  (hsql/format {:insert-into table
                :values [(merge where row)]
                :on-duplicate-key-update row}))

(defmethod hformat/format-clause :on-duplicate-key-update [[_ values] _]
  (str "ON DUPLICATE KEY UPDATE "
       (hformat/comma-join (for [[k v] values]
                             (str (hformat/to-sql k) " = "
                                  (hformat/to-sql v))))))

(hformat/register-clause! :on-duplicate-key-update 225)
