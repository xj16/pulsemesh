(ns pulsemesh.event.schema
  "Canonical vocabulary for PulseMesh commands and events.

   PulseMesh is event-sourced: the write side turns *commands* into
   *events*, appends events to an immutable log, and never mutates state
   in place. Read models (in Redis) and the Erlang fan-out fabric are both
   derived, disposable projections of this log.

   Aggregates
   ==========
   The unit of consistency is a *channel* (a chat/presence room). Every
   event belongs to exactly one channel stream, identified by `:channel-id`.
   Within a stream, events are totally ordered by `:version` (1-based)."
  (:require [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Command types the API accepts (write side input).
;; ---------------------------------------------------------------------------

(def command-types
  #{:join-channel      ;; a user joins a channel
    :leave-channel     ;; a user leaves a channel
    :post-message      ;; a user posts a chat message
    :set-presence})    ;; a user updates presence (online/away/offline/...)

;; ---------------------------------------------------------------------------
;; Event types the domain emits (write side output; the durable truth).
;; ---------------------------------------------------------------------------

(def event-types
  #{:channel-created
    :member-joined
    :member-left
    :message-posted
    :presence-changed})

(def presence-states
  #{"online" "away" "busy" "offline"})

;; ---------------------------------------------------------------------------
;; Validation helpers. We keep these dependency-free (no spec/malli) so the
;; service stays lightweight and the rules read as plain data checks.
;; ---------------------------------------------------------------------------

(defn- non-blank-string? [x]
  (and (string? x) (not (str/blank? x))))

(defn- problems-for
  "Return a vector of human-readable problems for `cmd`, or [] if valid."
  [{:keys [type channel-id user-id body presence] :as _cmd}]
  (cond-> []
    (not (contains? command-types type))
    (conj (str "unknown command type: " (pr-str type)))

    (not (non-blank-string? channel-id))
    (conj "channel-id must be a non-blank string")

    (not (non-blank-string? user-id))
    (conj "user-id must be a non-blank string")

    (and (= type :post-message) (not (non-blank-string? body)))
    (conj "post-message requires a non-blank body")

    (and (= type :post-message) (non-blank-string? body) (> (count body) 4000))
    (conj "message body must be <= 4000 characters")

    (and (= type :set-presence) (not (contains? presence-states presence)))
    (conj (str "presence must be one of " (pr-str presence-states)))))

(defn validate-command
  "Validate a decoded command map. Returns `{:ok cmd}` on success or
   `{:error [msgs...]}` describing every problem found."
  [cmd]
  (let [cmd      (update cmd :type keyword)
        problems (problems-for cmd)]
    (if (seq problems)
      {:error problems}
      {:ok cmd})))
