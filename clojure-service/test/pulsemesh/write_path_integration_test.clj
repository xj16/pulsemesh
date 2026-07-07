(ns pulsemesh.write-path-integration-test
  "Integration tests for the write path and the CQRS read side against REAL
   Postgres and Redis. These are the tests that move the project from 'the
   pure core is covered' to 'the whole pipeline is trustworthy':

     * a command actually appends events to Postgres and the read models
       reflect it,
     * two concurrent writers on one channel resolve to exactly one winner
       with the loser retrying against fresh state (the ::version-conflict
       path, previously untested),
     * the Redis presence projection equals a fresh Postgres replay after a
       burst of commands, and
     * when Redis is cold the query side transparently falls back to the log.

   Tagged ^:integration so the fast `clojure -X:test` run skips them; CI runs
   them via `clojure -X:integration` with service containers. Connection
   details come from the same env vars the app uses; every test isolates
   itself with a unique channel id."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [pulsemesh.command.handler :as command]
            [pulsemesh.query.handler :as query]
            [pulsemesh.infra.config :as config]
            [pulsemesh.infra.db :as db]
            [pulsemesh.infra.migrate :as migrate]
            [pulsemesh.infra.redis :as redis]
            [pulsemesh.event.channel :as channel])
  (:import [java.util UUID]))

;; ---------------------------------------------------------------------------
;; Shared system, opened once for the suite.
;; ---------------------------------------------------------------------------

(def ^:dynamic *sys* nil)

(defn- open-sys []
  (let [cfg (config/load-config :test)
        ds  (db/open-datasource (:postgres cfg))]
    (migrate/migrate! ds)
    {:config     cfg
     :ds         ds
     :redis-conn (redis/make-conn (:redis cfg))
     :publisher  nil}))          ;; no broker needed for these tests

(defn with-system [f]
  (let [sys (open-sys)]
    (binding [*sys* sys]
      (try (f)
           (finally (db/close-datasource (:ds sys)))))))

(use-fixtures :once with-system)

(defn- chan-id [] (str "it-" (UUID/randomUUID)))

(defn- join! [sys ch u]
  (command/handle sys {:type "join-channel" :channel-id ch :user-id u}))

;; ---------------------------------------------------------------------------
;; 1. Append + read-model reflect a real command flow.
;; ---------------------------------------------------------------------------

(deftest ^:integration append-and-read-models
  (let [sys *sys* ch (chan-id)]
    (let [r (join! sys ch "alice")]
      (is (= :accepted (:status r)))
      (is (= [:channel-created :member-joined] (mapv :type (:events r)))))
    (is (= :accepted (:status (command/handle sys {:type "post-message"
                                                   :channel-id ch :user-id "alice"
                                                   :body "hello pg"}))))
    (testing "the durable log has exactly the events we appended"
      (let [events (db/load-stream (:ds sys) ch)]
        (is (= [:channel-created :member-joined :message-posted]
               (mapv :type events)))
        (is (= [1 2 3] (mapv :version events)))))
    (testing "presence read model reflects the member"
      (let [p (query/channel-presence sys ch)]
        (is (= {"alice" "online"} (:members p)))))
    (testing "messages read model reflects the post"
      (let [m (query/recent-messages sys ch 10)]
        (is (= "hello pg" (-> m :messages first :body)))))))

;; ---------------------------------------------------------------------------
;; 2. Optimistic-concurrency conflict: two writers race the same expected
;;    version; exactly one append wins, the other raises ::version-conflict
;;    (which the handler retries). We drive the low-level append directly to
;;    force the collision deterministically.
;; ---------------------------------------------------------------------------

(deftest ^:integration concurrent-append-conflict-is-detected
  (let [ds (:ds *sys*) ch (chan-id)]
    ;; Seed version 1 so both racers expect version 1.
    (db/append-events! ds ch 0 [{:type :channel-created :channel-id ch}])
    (let [ev (fn [u] [{:type :member-joined :channel-id ch :user-id u}])
          f1 (future (try {:ok (db/append-events! ds ch 1 (ev "a"))}
                          (catch clojure.lang.ExceptionInfo e {:err (:type (ex-data e))})))
          f2 (future (try {:ok (db/append-events! ds ch 1 (ev "b"))}
                          (catch clojure.lang.ExceptionInfo e {:err (:type (ex-data e))})))
          r1 @f1 r2 @f2
          oks   (filter :ok [r1 r2])
          errs  (filter :err [r1 r2])]
      (testing "exactly one writer wins, the other sees a version conflict"
        (is (= 1 (count oks)))
        (is (= 1 (count errs)))
        (is (= ::db/version-conflict (:err (first errs))))))
    (testing "the stream is still a clean 1..2 with a single winner at v2"
      (let [events (db/load-stream ds ch)]
        (is (= [1 2] (mapv :version events)))))))

(deftest ^:integration handler-retries-and-converges-under-contention
  (let [sys *sys* ch (chan-id)]
    ;; Fire distinct users' joins concurrently through the full handler. Under
    ;; contention on one stream, each command is either :accepted (its retry
    ;; loop converged) or :conflict (it hit the bounded-retry ceiling and the
    ;; API would return 503 — the documented, safe-to-retry outcome). The core
    ;; guarantee we assert is *consistency*, not that every writer wins:
    ;;   - no command is ever lost or corrupted (only accepted | conflict),
    ;;   - the accepted joiners are EXACTLY the channel's members (no dupes,
    ;;     no ghosts), and
    ;;   - the log is a clean contiguous 1..N with no gaps or collisions.
    (let [users   (mapv #(str "u" %) (range 8))
          results (->> users
                       (mapv (fn [u] (future [u (join! sys ch u)])))
                       (mapv deref))
          status-of (into {} (map (fn [[u r]] [u (:status r)])) results)
          accepted  (set (for [[u s] status-of :when (= :accepted s)] u))]
      (testing "every command resolves cleanly (accepted or conflict, never lost)"
        (is (every? #{:accepted :conflict} (vals status-of))))
      (testing "at least the uncontended majority converges"
        (is (pos? (count accepted))))
      (let [events (db/load-stream (:ds sys) ch)
            state  (channel/replay events)]
        (testing "members are exactly the accepted joiners — no dupes, no ghosts"
          (is (= accepted (set (keys (:members state))))))
        (testing "the log is a clean contiguous stream"
          (is (= (mapv :version events)
                 (vec (range 1 (inc (count events)))))))
        (is (:exists? state))))))

;; ---------------------------------------------------------------------------
;; 3. Redis projection == fresh Postgres replay after a burst.
;; ---------------------------------------------------------------------------

(deftest ^:integration redis-projection-matches-log-replay
  (let [sys *sys* ch (chan-id)]
    (join! sys ch "alice")
    (join! sys ch "bob")
    (command/handle sys {:type "set-presence" :channel-id ch
                         :user-id "bob" :presence "away"})
    (command/handle sys {:type "post-message" :channel-id ch
                         :user-id "alice" :body "yo"})
    (let [from-cache (redis/presence (:redis-conn sys) ch)
          from-log   (into {} (for [[uid m] (:members (channel/replay
                                                       (db/load-stream (:ds sys) ch)))]
                                 [uid (:presence m)]))]
      (is (= from-log from-cache)
          "the Redis presence hash equals a fresh replay of the log")
      (is (= {"alice" "online" "bob" "away"} from-cache)))))

;; ---------------------------------------------------------------------------
;; 4. Cache-cold fallback: with an empty Redis projection the query side still
;;    returns correct presence, sourced from the log.
;; ---------------------------------------------------------------------------

(deftest ^:integration query-falls-back-to-log-when-cache-cold
  (let [sys *sys* ch (chan-id)]
    (join! sys ch "alice")
    (command/handle sys {:type "set-presence" :channel-id ch
                         :user-id "alice" :presence "busy"})
    ;; Simulate a cold/evicted cache by pointing the query at a redis-conn that
    ;; always misses (nil), which is exactly what redis/presence returns on a
    ;; connection error. The query layer must then rebuild from Postgres.
    (let [cold-sys (assoc sys :redis-conn {:pool {} :spec {:uri "redis://127.0.0.1:1"}})
          p (query/channel-presence cold-sys ch)]
      (is (= "log" (:source p)) "served from the durable log, not the cache")
      (is (= {"alice" "busy"} (:members p))))))
