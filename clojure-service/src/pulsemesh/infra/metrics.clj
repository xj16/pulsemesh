(ns pulsemesh.infra.metrics
  "A tiny, dependency-free metrics registry that renders Prometheus text
   exposition format at GET /metrics.

   Kept deliberately minimal (counters + a couple of summaries) so the
   observability story needs no extra library and is fully readable. The
   command handler increments these on every command; the HTTP layer exposes
   them. Values are process-local atoms — exactly the model a single-node
   reference service needs, and the same shape you'd back with a real client
   in production."
  (:require [clojure.string :as str]))

;; counter-name -> long
(defonce ^:private counters (atom {}))
;; summary-name -> {:count long :sum double}
(defonce ^:private summaries (atom {}))

(def ^:private counter-help
  {"pulsemesh_commands_accepted_total"  "Commands accepted and appended."
   "pulsemesh_commands_rejected_total"  "Commands rejected by the domain."
   "pulsemesh_commands_invalid_total"   "Commands failing schema validation."
   "pulsemesh_commands_conflict_total"  "Commands abandoned after max retries."
   "pulsemesh_command_retries_total"    "Optimistic-concurrency retry attempts."})

(def ^:private summary-help
  {"pulsemesh_command_latency_seconds"  "End-to-end command handling latency."})

(defn inc-counter!
  ([name] (inc-counter! name 1))
  ([name n] (swap! counters update name (fnil + 0) n) nil))

(defn observe!
  "Record a value into a summary (e.g. a latency in seconds)."
  [name v]
  (swap! summaries update name
         (fn [{:keys [count sum] :or {count 0 sum 0.0}}]
           {:count (inc count) :sum (+ sum (double v))}))
  nil)

(defmacro timed
  "Evaluate body, observing its wall-clock duration (seconds) into `summary`."
  [summary & body]
  `(let [start# (System/nanoTime)
         result# (do ~@body)]
     (observe! ~summary (/ (- (System/nanoTime) start#) 1.0e9))
     result#))

(defn reset-all! []
  (reset! counters {})
  (reset! summaries {}))

(defn- fmt-counter [sb name v]
  (when-let [help (counter-help name)]
    (.append sb (str "# HELP " name " " help "\n"))
    (.append sb (str "# TYPE " name " counter\n")))
  (.append sb (str name " " v "\n")))

(defn- fmt-summary [sb name {:keys [count sum]}]
  (when-let [help (summary-help name)]
    (.append sb (str "# HELP " name " " help "\n"))
    (.append sb (str "# TYPE " name " summary\n")))
  (.append sb (str name "_count " count "\n"))
  (.append sb (str name "_sum " (format "%.6f" (double sum)) "\n")))

(defn render
  "Render all registered metrics in Prometheus text exposition format. Always
   emits the known counters (0 if untouched) so scrapers see a stable series."
  []
  (let [sb (StringBuilder.)
        cs (merge (zipmap (keys counter-help) (repeat 0)) @counters)]
    (doseq [[name v] (sort-by key cs)] (fmt-counter sb name v))
    (doseq [[name s] (sort-by key @summaries)] (fmt-summary sb name s))
    (str/trim-newline (.toString sb))))
