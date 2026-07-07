(ns pulsemesh.infra.migrate
  "Schema management via Migratus — the single, authoritative way the event
   store schema is created and evolved.

   Migrations live in `resources/db/migrations/*.sql` and are applied at boot
   (see `pulsemesh.main/start-system`). Migratus records what it has run in a
   `schema_migrations` table, so this is idempotent and forward-only in normal
   operation. `db/ensure-schema!` remains only as a dependency-free fallback
   for environments where running the migration machinery is undesirable
   (e.g. a throwaway test database); it produces the same table."
  (:require [migratus.core :as migratus]
            [pulsemesh.infra.db :as db]
            [clojure.tools.logging :as log]))

(defn config
  "Build a Migratus config from a HikariCP datasource. We hand Migratus the
   pooled datasource directly so it shares the app's connection settings."
  [ds]
  {:store         :database
   :migration-dir "db/migrations"
   :db            {:datasource ds}})

(defn migrate!
  "Apply all pending migrations. Returns :migrated on success. Falls back to
   the ad-hoc `ensure-schema!` DDL (which is byte-for-byte equivalent to the
   initial migration) if Migratus is unavailable, so the service can always
   come up with a valid schema."
  [ds]
  (try
    (migratus/migrate (config ds))
    (log/info "database migrations applied")
    :migrated
    (catch Throwable e
      (log/warn e "migratus failed; falling back to ensure-schema! bootstrap")
      (db/ensure-schema! ds)
      :fallback)))
