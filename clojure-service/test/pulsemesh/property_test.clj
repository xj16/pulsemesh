(ns pulsemesh.property-test
  "Property-based tests for the core event-sourcing invariant.

   The whole design rests on one equivalence: the write-side aggregate state,
   a fresh replay of the emitted event log, and the read-side presence
   projection (the one the Erlang fabric folds) must all agree. If they can
   ever diverge, presence shown to a client would drift from the source of
   truth. We nail that down by generating random *valid* command sequences,
   driving them through decide/apply-event, and asserting the three views are
   identical for every generated history."
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [pulsemesh.event.channel :as channel]))

;; ---------------------------------------------------------------------------
;; A Clojure port of the Erlang presence projection (pulsemesh_presence.erl's
;; `project/2`). This is the *read side* the fabric maintains: user-id ->
;; presence-state, driven purely by the emitted events. Keeping a faithful
;; port here lets us assert write-side == read-side without a running node.
;; ---------------------------------------------------------------------------

(defn presence-project
  "Fold one emitted event into the fabric-style presence map."
  [members {:keys [type user-id presence]}]
  (case type
    :member-joined    (assoc members user-id "online")
    :member-left      (dissoc members user-id)
    :presence-changed (if (contains? members user-id)
                        (assoc members user-id presence)
                        members)
    ;; channel-created, message-posted, anything else: no presence change.
    members))

(defn fabric-presence
  "The presence map the Erlang fabric would hold after these events."
  [events]
  (reduce presence-project {} events))

(defn aggregate-presence
  "The presence map implied by the write-side aggregate state."
  [state]
  (into {} (for [[uid m] (:members state)] [uid (:presence m)])))

;; ---------------------------------------------------------------------------
;; Generators: build a random but *valid* command history by tracking the set
;; of current members as we go, so we mostly exercise the accepting paths
;; (the domain rejects e.g. leaving a channel you're not in).
;; ---------------------------------------------------------------------------

(def gen-user (gen/elements ["u1" "u2" "u3" "u4" "u5"]))
(def gen-presence (gen/elements ["online" "away" "busy" "offline"]))
(def channel-id "prop-chan")

(defn gen-command
  "Generate a plausible command given the current member set."
  [members]
  (if (empty? members)
    (gen/fmap (fn [u] {:type :join-channel :channel-id channel-id :user-id u})
              gen-user)
    (gen/one-of
     [(gen/fmap (fn [u] {:type :join-channel :channel-id channel-id :user-id u})
                gen-user)
      (gen/fmap (fn [u] {:type :leave-channel :channel-id channel-id :user-id u})
                (gen/elements (vec members)))
      (gen/fmap (fn [[u b]] {:type :post-message :channel-id channel-id
                             :user-id u :body b})
                (gen/tuple (gen/elements (vec members)) gen/string-alphanumeric))
      (gen/fmap (fn [[u p]] {:type :set-presence :channel-id channel-id
                             :user-id u :presence p})
                (gen/tuple (gen/elements (vec members)) gen-presence))])))

(def gen-history
  "Generate a full command history, threading membership so commands stay
   mostly valid. Returns the vector of commands."
  (gen/bind
   (gen/choose 0 40)
   (fn [n]
     (letfn [(step [i members acc]
               (if (zero? i)
                 (gen/return acc)
                 (gen/bind (gen-command members)
                           (fn [cmd]
                             (let [members'
                                   (case (:type cmd)
                                     :join-channel  (conj members (:user-id cmd))
                                     :leave-channel (disj members (:user-id cmd))
                                     members)]
                               (step (dec i) members' (conj acc cmd)))))))]
       (step n #{} [])))))

;; ---------------------------------------------------------------------------
;; Drive a history through the real aggregate, assigning versions the way the
;; event store does, collecting every emitted event.
;; ---------------------------------------------------------------------------

(defn run-history
  "Apply commands in order via decide, accumulating emitted events (versioned)
   and the evolving aggregate state. Returns {:events [...] :state st}."
  [commands]
  (reduce
   (fn [{:keys [events state version]} cmd]
     (let [decision (channel/decide state cmd)]
       (if (map? decision) ;; rejection -> no state change
         {:events events :state state :version version}
         (let [numbered (map-indexed
                         (fn [i e] (assoc e :version (+ version i 1)))
                         decision)
               state'   (reduce channel/apply-event state numbered)]
           {:events  (into events numbered)
            :state   state'
            :version (+ version (count numbered))}))))
   {:events [] :state channel/empty-state :version 0}
   commands))

;; ---------------------------------------------------------------------------
;; The properties.
;; ---------------------------------------------------------------------------

(def prop-equivalence
  (prop/for-all [commands gen-history]
    (let [{:keys [events state]} (run-history commands)
          agg     (aggregate-presence state)
          fabric  (fabric-presence events)
          replay  (aggregate-presence (channel/replay events))]
      (and (= agg fabric)     ;; write-side state == fabric read model
           (= agg replay))))) ;; write-side state == fresh replay

(deftest aggregate-fabric-replay-equivalence
  (let [result (tc/quick-check 500 prop-equivalence)]
    (is (:pass? result)
        (str "equivalence property failed: " (pr-str (:shrunk result))))))

(def prop-versions-contiguous
  ;; Every emitted event stream is 1..N contiguous (the store relies on this
  ;; for optimistic concurrency).
  (prop/for-all [commands gen-history]
    (let [{:keys [events]} (run-history commands)
          versions (mapv :version events)]
      (= versions (vec (range 1 (inc (count events))))))))

(deftest emitted-versions-are-contiguous
  (let [result (tc/quick-check 500 prop-versions-contiguous)]
    (is (:pass? result)
        (str "version contiguity failed: " (pr-str (:shrunk result))))))

(def prop-idempotent-join
  ;; Re-issuing the same accepted command sequence twice never corrupts state:
  ;; a second identical join for a present member emits nothing.
  (prop/for-all [u gen-user]
    (let [after-join (channel/replay
                      [{:type :channel-created :channel-id channel-id}
                       {:type :member-joined :channel-id channel-id :user-id u}])
          again (channel/decide after-join
                                {:type :join-channel :channel-id channel-id
                                 :user-id u})]
      (= [] again))))

(deftest join-is-idempotent-for-any-user
  (is (:pass? (tc/quick-check 100 prop-idempotent-join))))
