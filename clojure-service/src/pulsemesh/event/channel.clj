(ns pulsemesh.event.channel
  "The channel aggregate: a *pure* event-sourced domain model.

   Two pure functions drive the whole write side:

     (apply-event state event)  -> state'      ;; fold, rebuild state from log
     (decide state command)     -> [events] | {:reject reason}

   Neither touches IO. Given the same inputs they always produce the same
   outputs, which is exactly what makes an event-sourced core easy to test
   and safe to replay. The infrastructure layer (Postgres/Redis/Rabbit) is
   the only place side effects live."
  (:require [pulsemesh.event.schema :as schema]))

;; ---------------------------------------------------------------------------
;; State: the in-memory shape we fold events into. Cheap to rebuild by replay.
;; ---------------------------------------------------------------------------

(def empty-state
  {:exists?  false
   :members  {}        ;; user-id -> {:presence "online" :joined-at <inst>}
   :version  0})       ;; last applied event version

;; ---------------------------------------------------------------------------
;; Fold: apply a single event to state. This must handle every event-type.
;; ---------------------------------------------------------------------------

(defmulti ^:private apply-event*
  (fn [_state event] (:type event)))

(defmethod apply-event* :channel-created [state _event]
  (assoc state :exists? true))

(defmethod apply-event* :member-joined [state {:keys [user-id at]}]
  (update state :members assoc user-id {:presence "online" :joined-at at}))

(defmethod apply-event* :member-left [state {:keys [user-id]}]
  (update state :members dissoc user-id))

(defmethod apply-event* :message-posted [state _event]
  ;; Messages don't change membership/presence state; they are pure history.
  state)

(defmethod apply-event* :presence-changed [state {:keys [user-id presence]}]
  (if (contains? (:members state) user-id)
    (assoc-in state [:members user-id :presence] presence)
    state))

(defmethod apply-event* :default [state _event]
  state)

(defn apply-event
  "Fold one event into `state`, bumping the applied version."
  [state event]
  (-> (apply-event* state event)
      (assoc :version (:version event (inc (:version state 0))))))

(defn replay
  "Rebuild aggregate state from an ordered sequence of events."
  [events]
  (reduce apply-event empty-state events))

;; ---------------------------------------------------------------------------
;; Decide: turn a command + current state into new events (or a rejection).
;; The returned events carry no version/id yet; the store assigns those.
;; ---------------------------------------------------------------------------

(defn- member? [state user-id]
  (contains? (:members state) user-id))

(defmulti ^:private decide*
  (fn [_state command] (:type command)))

(defmethod decide* :join-channel [state {:keys [channel-id user-id at]}]
  (let [base (when-not (:exists? state)
               [{:type :channel-created :channel-id channel-id :at at}])]
    (if (member? state user-id)
      ;; Idempotent: already a member -> only ensure channel exists.
      (vec base)
      (conj (vec base)
            {:type :member-joined :channel-id channel-id :user-id user-id :at at}))))

(defmethod decide* :leave-channel [state {:keys [channel-id user-id at]}]
  (if (member? state user-id)
    [{:type :member-left :channel-id channel-id :user-id user-id :at at}]
    {:reject :not-a-member}))

(defmethod decide* :post-message [state {:keys [channel-id user-id body at]}]
  (if (member? state user-id)
    [{:type :message-posted :channel-id channel-id :user-id user-id
      :body body :at at}]
    {:reject :not-a-member}))

(defmethod decide* :set-presence [state {:keys [channel-id user-id presence at]}]
  (cond
    (not (member? state user-id))
    {:reject :not-a-member}

    (= presence (get-in state [:members user-id :presence]))
    ;; No-op: presence already at that value -> emit nothing (idempotent).
    []

    :else
    [{:type :presence-changed :channel-id channel-id :user-id user-id
      :presence presence :at at}]))

(defmethod decide* :default [_state command]
  {:reject (keyword (str "unknown-command-" (name (:type command))))})

(defn decide
  "Pure command handler. Returns a vector of new events (possibly empty for
   idempotent no-ops) or a `{:reject reason}` map. Assumes `command` already
   passed `schema/validate-command`."
  [state command]
  {:pre [(contains? schema/command-types (:type command))]}
  (decide* state command))
