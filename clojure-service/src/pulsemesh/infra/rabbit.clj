(ns pulsemesh.infra.rabbit
  "RabbitMQ publisher. Committed events are published to a topic exchange so
   the Erlang/OTP fabric (and any other consumer) can fan them out to live
   connections. The routing key is `channel.<channel-id>.<event-type>` so the
   fabric can subscribe per-channel or wildcard everything.

   Publishing happens *after* the Postgres commit. If Rabbit is down, the
   event is still durably stored; a consumer can rebuild by replaying the log,
   which keeps the whole system correct under partial failure."
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.exchange :as le]
            [langohr.basic :as lb]
            [jsonista.core :as json]
            [clojure.tools.logging :as log]))

(def ^:private mapper (json/object-mapper {:encode-key-fn name}))

(defrecord RabbitPublisher [conn channel exchange])

(defn connect
  "Open a connection + channel and declare the durable topic exchange."
  [{:keys [uri exchange]}]
  (let [conn (rmq/connect {:uri uri})
        ch   (lch/open conn)]
    (le/declare ch exchange "topic" {:durable true :auto-delete false})
    (log/info "connected to RabbitMQ, exchange" exchange)
    (->RabbitPublisher conn ch exchange)))

(defn close [{:keys [conn channel]}]
  (some-> channel lch/close)
  (some-> conn rmq/close))

(defn- routing-key [{:keys [channel-id type]}]
  (str "channel." channel-id "." (name type)))

(defn publish!
  "Publish one committed event to the exchange. Best-effort: logs and swallows
   failures so a broker outage never rolls back a durable write."
  [{:keys [channel exchange] :as publisher} event]
  (when publisher
    (try
      (lb/publish channel exchange (routing-key event)
                  (json/write-value-as-string event mapper)
                  {:content-type "application/json"
                   :persistent   true
                   :message-id   (str (:id event))
                   :type         (name (:type event))})
      (catch Exception e
        (log/warn e "rabbit publish failed for" (:type event))))))

(defn publish-all!
  "Publish a batch of events in stream order."
  [publisher events]
  (doseq [e events] (publish! publisher e)))
