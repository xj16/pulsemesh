(ns pulsemesh.infra.db
  "PostgreSQL as the append-only event store.

   The `events` table is the single source of truth. Each row is one
   immutable domain event, ordered within a `channel_id` stream by a 1-based
   `version`. A UNIQUE (channel_id, version) constraint gives us optimistic
   concurrency: two racing writers targeting the same expected version cannot
   both succeed."
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [jsonista.core :as json]
            [clojure.tools.logging :as log])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           [org.postgresql.util PGobject]
           [java.sql Timestamp]
           [java.time Instant]))

(def ^:private mapper
  (json/object-mapper {:decode-key-fn keyword}))

(defn ->jsonb
  "Wrap a Clojure value as a Postgres jsonb parameter."
  [value]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (json/write-value-as-string value mapper))))

(defn ->clj
  "Decode a Postgres jsonb value back into Clojure data."
  [^PGobject pg]
  (when pg (json/read-value (.getValue pg) mapper)))

;; ---------------------------------------------------------------------------
;; Pooled datasource lifecycle.
;; ---------------------------------------------------------------------------

(defn open-datasource
  "Build a HikariCP-pooled datasource from a config map."
  ^HikariDataSource [{:keys [jdbc-url pool-size]}]
  (let [cfg (doto (HikariConfig.)
              (.setJdbcUrl jdbc-url)
              (.setMaximumPoolSize (int (or pool-size 8)))
              (.setPoolName "pulsemesh-pg")
              (.setAutoCommit true))]
    (HikariDataSource. cfg)))

(defn close-datasource [^HikariDataSource ds]
  (when ds (.close ds)))

;; ---------------------------------------------------------------------------
;; Reads.
;; ---------------------------------------------------------------------------

(def ^:private opts
  {:builder-fn rs/as-unqualified-lower-maps})

(defn- row->event [{:keys [id channel_id version type payload occurred_at]}]
  (merge {:id         (str id)
          :channel-id channel_id
          :version    version
          :type       (keyword type)
          :occurred-at (some-> ^Timestamp occurred_at .toInstant str)}
         (->clj payload)))

(defn load-stream
  "Return every event for a channel, ordered by version ascending."
  [ds channel-id]
  (->> (jdbc/execute! ds
                      ["SELECT id, channel_id, version, type, payload, occurred_at
                          FROM events
                         WHERE channel_id = ?
                         ORDER BY version ASC" channel-id]
                      opts)
       (mapv row->event)))

(defn current-version
  "Highest event version currently stored for a channel (0 if none)."
  [ds channel-id]
  (-> (jdbc/execute-one! ds
                        ["SELECT COALESCE(MAX(version), 0) AS v
                            FROM events WHERE channel_id = ?" channel-id]
                        opts)
      :v))

(defn recent-messages
  "Read model straight from the log: the last `n` message-posted events for a
   channel, newest first. Used as the durable fallback behind the Redis cache."
  [ds channel-id n]
  (->> (jdbc/execute! ds
                      ["SELECT id, channel_id, version, type, payload, occurred_at
                          FROM events
                         WHERE channel_id = ? AND type = 'message-posted'
                         ORDER BY version DESC
                         LIMIT ?" channel-id n]
                      opts)
       (mapv row->event)))

;; ---------------------------------------------------------------------------
;; Append (write side) with optimistic concurrency.
;; ---------------------------------------------------------------------------

(defn- ->timestamp ^Timestamp [at]
  (Timestamp/from (cond
                    (instance? Instant at) at
                    (string? at)           (Instant/parse at)
                    :else                  (Instant/now))))

(defn- version-conflict? [^Exception e]
  ;; PostgreSQL unique_violation SQLSTATE is 23505.
  (loop [t e]
    (cond
      (nil? t) false
      (and (instance? java.sql.SQLException t)
           (= "23505" (.getSQLState ^java.sql.SQLException t))) true
      :else (recur (.getCause t)))))

(defn append-events!
  "Atomically append `events` to `channel-id`'s stream, expecting the stream
   to currently be at `expected-version`. Assigns each event a sequential
   version starting at expected-version+1. Returns the persisted events with
   their assigned ids/versions.

   Concurrency control is twofold:
     1. a per-channel transaction-scoped advisory lock serializes concurrent
        writers to the same stream (so they queue instead of colliding), and
     2. the UNIQUE (channel_id, version) constraint is the authoritative
        backstop.
   Either mechanism surfacing a clash is translated into an ex-info of
   {:type ::version-conflict}, which the command handler retries."
  [ds channel-id expected-version events]
  (if (empty? events)
    []
    (try
      (jdbc/with-transaction [tx ds]
        ;; Serialize writers for this channel within the transaction. The lock
        ;; is keyed on a stable hash of the channel id (hashtext -> int4 ->
        ;; bigint), so concurrent appends to the same stream queue up.
        (jdbc/execute-one! tx
          ["SELECT pg_advisory_xact_lock(hashtext(?)::bigint)" channel-id])
        ;; Re-check the current version under the lock; if another writer got
        ;; here first, bail out as a conflict.
        (let [actual (current-version tx channel-id)]
          (when (not= actual expected-version)
            (throw (ex-info "optimistic concurrency conflict"
                            {:type ::version-conflict
                             :channel-id channel-id
                             :expected expected-version
                             :actual actual})))
          (doall
           (map-indexed
            (fn [i event]
              (let [version (+ expected-version i 1)
                    ;; payload = event minus the columns we store separately
                    payload (dissoc event :type :channel-id :version :id)
                    row (jdbc/execute-one! tx
                          ["INSERT INTO events (channel_id, version, type, payload, occurred_at)
                            VALUES (?, ?, ?, ?, ?)
                            RETURNING id, channel_id, version, type, payload, occurred_at"
                           channel-id version (name (:type event))
                           (->jsonb payload)
                           (->timestamp (:at event))]
                          opts)]
                (row->event row)))
            events))))
      (catch java.sql.SQLException e
        (if (version-conflict? e)
          (throw (ex-info "optimistic concurrency conflict"
                          {:type ::version-conflict :channel-id channel-id} e))
          (throw e))))))

;; ---------------------------------------------------------------------------
;; Schema bootstrap FALLBACK. Migratus (pulsemesh.infra.migrate) is the
;; authoritative schema driver and runs at boot; this ad-hoc DDL exists only
;; as a dependency-free fallback (used if Migratus is unavailable) and is kept
;; byte-for-byte equivalent to the initial migration in
;; resources/db/migrations/.
;; ---------------------------------------------------------------------------

(def schema-ddl
  "CREATE TABLE IF NOT EXISTS events (
     id          BIGSERIAL PRIMARY KEY,
     channel_id  TEXT        NOT NULL,
     version     BIGINT      NOT NULL,
     type        TEXT        NOT NULL,
     payload     JSONB       NOT NULL DEFAULT '{}'::jsonb,
     occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
     CONSTRAINT uq_events_stream UNIQUE (channel_id, version)
   )")

(defn ensure-schema!
  "Create the events table + supporting index if absent. Idempotent."
  [ds]
  (jdbc/execute! ds [schema-ddl])
  (jdbc/execute! ds ["CREATE INDEX IF NOT EXISTS idx_events_type
                        ON events (channel_id, type, version)"])
  (log/info "event store schema ensured"))
