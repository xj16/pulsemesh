(ns pulsemesh.infra.redis
  "Redis-backed read models (materialized projections of the event log).

   Two projections are maintained on every successful write:

     * presence set   pulsemesh:presence:<channel>  (hash user-id -> state)
     * recent messages pulsemesh:messages:<channel>  (capped list, newest first)

   Redis is a *cache*, not a source of truth. If it is cold or unavailable the
   query side falls back to replaying the Postgres log, so a Redis outage
   degrades latency, never correctness."
  (:require [taoensso.carmine :as car]
            [clojure.tools.logging :as log]))

(defn make-conn
  "Build a Carmine connection spec map from a redis:// URI."
  [{:keys [uri]}]
  {:pool {} :spec {:uri uri}})

(defmacro ^:private wcar [conn & body]
  `(car/wcar ~conn ~@body))

(def ^:private message-cap 100)

(defn- presence-key [channel-id] (str "pulsemesh:presence:" channel-id))
(defn- messages-key  [channel-id] (str "pulsemesh:messages:" channel-id))

;; ---------------------------------------------------------------------------
;; Projection updates. Each takes a single domain event and mutates the cache.
;; ---------------------------------------------------------------------------

(defn project-event!
  "Apply one event to the Redis read models. Best-effort: a Redis failure is
   logged and swallowed so it can never fail a durable write that already
   committed to Postgres."
  [conn {:keys [type channel-id user-id presence body occurred-at] :as event}]
  (try
    (case type
      :member-joined
      (wcar conn (car/hset (presence-key channel-id) user-id "online"))

      :member-left
      (wcar conn (car/hdel (presence-key channel-id) user-id))

      :presence-changed
      (wcar conn (car/hset (presence-key channel-id) user-id presence))

      :message-posted
      (let [entry (pr-str {:user-id user-id :body body :at occurred-at})]
        (wcar conn
          (car/lpush (messages-key channel-id) entry)
          (car/ltrim (messages-key channel-id) 0 (dec message-cap))))

      ;; channel-created and anything else: nothing to project.
      nil)
    (catch Exception e
      (log/warn e "redis projection failed for event" (:type event)))))

;; ---------------------------------------------------------------------------
;; Query helpers used by the read API.
;; ---------------------------------------------------------------------------

(defn presence
  "Return {user-id -> presence-state} for a channel, or nil if the cache is
   cold (caller should fall back to Postgres replay)."
  [conn channel-id]
  (try
    (let [flat (wcar conn (car/hgetall (presence-key channel-id)))]
      (when (seq flat)
        (into {} (map vec (partition 2 flat)))))
    (catch Exception e
      (log/warn e "redis presence read failed")
      nil)))

(defn recent-messages
  "Return up to `n` recently-posted messages (newest first) from the cache, or
   nil if cold."
  [conn channel-id n]
  (try
    (let [raw (wcar conn (car/lrange (messages-key channel-id) 0 (dec n)))]
      (when (seq raw)
        (mapv read-string raw)))
    (catch Exception e
      (log/warn e "redis messages read failed")
      nil)))
