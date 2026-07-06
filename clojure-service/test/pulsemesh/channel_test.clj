(ns pulsemesh.channel-test
  "Tests for the pure event-sourced aggregate. No infrastructure required:
   these exercise decide/apply-event/replay directly, which is the whole
   point of keeping the domain core pure."
  (:require [clojure.test :refer [deftest testing is]]
            [pulsemesh.event.channel :as channel]
            [pulsemesh.event.schema :as schema]))

(defn- decide-with
  "Apply a sequence of prior events, then decide `cmd` against the result."
  [prior-events cmd]
  (channel/decide (channel/replay prior-events) cmd))

(deftest join-creates-channel-and-member
  (testing "first join to a fresh channel creates it and adds the member"
    (let [events (decide-with []
                              {:type :join-channel :channel-id "c1" :user-id "u1"})]
      (is (= [:channel-created :member-joined] (mapv :type events))))))

(deftest join-is-idempotent
  (testing "joining a channel you're already in emits no new events"
    (let [prior [{:type :channel-created :channel-id "c1"}
                 {:type :member-joined :channel-id "c1" :user-id "u1"}]
          events (decide-with prior
                              {:type :join-channel :channel-id "c1" :user-id "u1"})]
      (is (= [] events)))))

(deftest second-user-join-does-not-recreate-channel
  (let [prior [{:type :channel-created :channel-id "c1"}
               {:type :member-joined :channel-id "c1" :user-id "u1"}]
        events (decide-with prior
                            {:type :join-channel :channel-id "c1" :user-id "u2"})]
    (is (= [:member-joined] (mapv :type events)))
    (is (= "u2" (:user-id (first events))))))

(deftest leave-requires-membership
  (testing "leaving without being a member is rejected"
    (is (= {:reject :not-a-member}
           (decide-with [{:type :channel-created :channel-id "c1"}]
                        {:type :leave-channel :channel-id "c1" :user-id "ghost"}))))
  (testing "a member can leave"
    (let [prior [{:type :channel-created :channel-id "c1"}
                 {:type :member-joined :channel-id "c1" :user-id "u1"}]]
      (is (= [:member-left]
             (mapv :type (decide-with prior
                                      {:type :leave-channel :channel-id "c1"
                                       :user-id "u1"})))))))

(deftest post-message-requires-membership
  (is (= {:reject :not-a-member}
         (decide-with [{:type :channel-created :channel-id "c1"}]
                      {:type :post-message :channel-id "c1" :user-id "u1"
                       :body "hi"})))
  (let [prior [{:type :channel-created :channel-id "c1"}
               {:type :member-joined :channel-id "c1" :user-id "u1"}]
        events (decide-with prior
                            {:type :post-message :channel-id "c1" :user-id "u1"
                             :body "hello world"})]
    (is (= [:message-posted] (mapv :type events)))
    (is (= "hello world" (:body (first events))))))

(deftest presence-transitions
  (let [prior [{:type :channel-created :channel-id "c1"}
               {:type :member-joined :channel-id "c1" :user-id "u1"}]]
    (testing "changing presence emits an event"
      (is (= [:presence-changed]
             (mapv :type (decide-with prior
                                      {:type :set-presence :channel-id "c1"
                                       :user-id "u1" :presence "away"})))))
    (testing "setting the same presence is a no-op"
      (is (= []
             (decide-with prior
                          {:type :set-presence :channel-id "c1"
                           :user-id "u1" :presence "online"}))))
    (testing "presence for a non-member is rejected"
      (is (= {:reject :not-a-member}
             (decide-with prior
                          {:type :set-presence :channel-id "c1"
                           :user-id "u2" :presence "away"}))))))

(deftest replay-rebuilds-state
  (testing "folding a full stream reconstructs membership and presence"
    (let [stream [{:type :channel-created :channel-id "c1" :version 1}
                  {:type :member-joined :channel-id "c1" :user-id "u1" :version 2}
                  {:type :member-joined :channel-id "c1" :user-id "u2" :version 3}
                  {:type :presence-changed :channel-id "c1" :user-id "u1"
                   :presence "busy" :version 4}
                  {:type :member-left :channel-id "c1" :user-id "u2" :version 5}]
          state (channel/replay stream)]
      (is (:exists? state))
      (is (= 5 (:version state)))
      (is (= #{"u1"} (set (keys (:members state)))))
      (is (= "busy" (get-in state [:members "u1" :presence]))))))

(deftest version-assignment-is-sequential
  (testing "apply-event bumps version even when events omit it"
    (let [state (reduce channel/apply-event channel/empty-state
                        [{:type :channel-created :channel-id "c1"}
                         {:type :member-joined :channel-id "c1" :user-id "u1"}])]
      (is (= 2 (:version state))))))

(deftest command-validation
  (testing "valid command passes"
    (is (:ok (schema/validate-command
              {:type "join-channel" :channel-id "c1" :user-id "u1"}))))
  (testing "missing user-id is caught"
    (is (:error (schema/validate-command
                 {:type "join-channel" :channel-id "c1"}))))
  (testing "empty message body is caught"
    (is (:error (schema/validate-command
                 {:type "post-message" :channel-id "c1" :user-id "u1" :body "  "}))))
  (testing "bad presence value is caught"
    (is (:error (schema/validate-command
                 {:type "set-presence" :channel-id "c1" :user-id "u1"
                  :presence "vibing"}))))
  (testing "unknown command type is caught"
    (is (:error (schema/validate-command
                 {:type "explode" :channel-id "c1" :user-id "u1"})))))
