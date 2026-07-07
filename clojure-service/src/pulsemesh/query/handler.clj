(ns pulsemesh.query.handler
  "The read side. Serves queries from the Redis read models, transparently
   falling back to a Postgres replay when the cache is cold. This is the CQRS
   split: writes go through the aggregate + event log; reads are cheap lookups
   against derived projections."
  (:require [pulsemesh.event.channel :as channel]
            [pulsemesh.infra.db :as db]
            [pulsemesh.infra.redis :as redis]))

(defn channel-presence
  "Return {:channel-id .. :members {user-id -> presence}} for a channel.
   Tries Redis first; on a cache miss replays the log and warms the state."
  [{:keys [ds redis-conn]} channel-id]
  (let [cached (redis/presence redis-conn channel-id)]
    (if cached
      {:channel-id channel-id :members cached :source "cache"}
      (let [state (channel/replay (db/load-stream ds channel-id))
            members (into {} (for [[uid m] (:members state)]
                               [uid (:presence m)]))]
        {:channel-id channel-id :members members :source "log"}))))

(defn recent-messages
  "Return the `n` most recent messages for a channel, newest first. Redis list
   first, Postgres log as the fallback of record."
  [{:keys [ds redis-conn]} channel-id n]
  (let [n (max 1 (min (or n 50) 200))
        cached (redis/recent-messages redis-conn channel-id n)]
    (if cached
      {:channel-id channel-id :messages cached :source "cache"}
      (let [rows (db/recent-messages ds channel-id n)
            msgs (mapv (fn [e] {:user-id (:user-id e)
                                :body    (:body e)
                                :at      (:occurred-at e)})
                       rows)]
        {:channel-id channel-id :messages msgs :source "log"}))))

(defn channel-history
  "Return the full ordered event stream for a channel (audit/debug view).
   Always served from the durable log."
  [{:keys [ds]} channel-id]
  {:channel-id channel-id
   :events (db/load-stream ds channel-id)})

(defn channel-events-since
  "Return the ordered events for a channel with version strictly greater than
   `since` (0 => the whole stream). This is the replay endpoint the fabric
   uses on (re)connect to close any gap: a consumer that was offline, or a
   presence tracker that crashed and restarted with empty state, calls this to
   rehydrate straight from the durable log instead of waiting for future
   events. It is what makes the 'consumers catch up / replay' failure-model
   claim actually true."
  [{:keys [ds]} channel-id since]
  (let [since  (max 0 (or since 0))
        events (->> (db/load-stream ds channel-id)
                    (filterv #(> (:version %) since)))]
    {:channel-id channel-id
     :since      since
     :count      (count events)
     :events     events}))
