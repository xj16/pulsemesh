(ns pulsemesh.command.handler
  "The write side. Orchestrates the pure aggregate against the infrastructure:

     1. load the channel's event stream from Postgres and replay it
     2. `decide` the command against that state (pure)
     3. append the resulting events with optimistic concurrency
     4. project them into the Redis read models
     5. publish them to RabbitMQ for the fabric to fan out

   On a version conflict (another writer won the race) we retry the whole
   flow a bounded number of times, since a fresh replay may now accept or
   reject the command differently."
  (:require [pulsemesh.event.channel :as channel]
            [pulsemesh.event.schema :as schema]
            [pulsemesh.infra.db :as db]
            [pulsemesh.infra.redis :as redis]
            [pulsemesh.infra.rabbit :as rabbit]
            [clojure.tools.logging :as log])
  (:import [java.time Instant]))

(def ^:private max-retries 5)

(defn- stamp-time
  "Ensure the command carries a timestamp used by emitted events."
  [command]
  (assoc command :at (str (Instant/now))))

(defn- attempt
  "One pass of decide+append. Returns {:accepted events} on success,
   {:rejected reason} if the domain refused, or throws ::version-conflict
   which the caller retries."
  [{:keys [ds]} command]
  (let [channel-id (:channel-id command)
        events     (db/load-stream ds channel-id)
        state      (channel/replay events)
        expected   (:version state)
        decision   (channel/decide state command)]
    (if (map? decision) ;; {:reject ...}
      {:rejected (:reject decision)}
      (let [persisted (db/append-events! ds channel-id expected decision)]
        {:accepted persisted}))))

(defn handle
  "Validate and process a command end-to-end. Returns one of:
     {:status :accepted :events [...]}
     {:status :rejected :reason <kw>}
     {:status :invalid  :problems [...]}
     {:status :conflict}"
  [{:keys [redis-conn publisher] :as sys} raw-command]
  (let [{:keys [ok error]} (schema/validate-command raw-command)]
    (if error
      {:status :invalid :problems error}
      (let [command (stamp-time ok)]
        (loop [tries 0]
          (let [result (try
                         (attempt sys command)
                         (catch clojure.lang.ExceptionInfo e
                           (if (= ::db/version-conflict (:type (ex-data e)))
                             ::retry
                             (throw e))))]
            (cond
              (= result ::retry)
              (if (< tries max-retries)
                (recur (inc tries))
                (do (log/warn "giving up after" max-retries "conflicts on"
                              (:channel-id command))
                    {:status :conflict}))

              (:rejected result)
              {:status :rejected :reason (:rejected result)}

              :else
              (let [events (:accepted result)]
                ;; Fan out to read models + broker. Best-effort; already durable.
                (doseq [e events]
                  (redis/project-event! redis-conn e))
                (rabbit/publish-all! publisher events)
                {:status :accepted :events events}))))))))
