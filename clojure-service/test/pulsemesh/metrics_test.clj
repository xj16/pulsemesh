(ns pulsemesh.metrics-test
  "Unit tests for the dependency-free metrics registry."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as str]
            [pulsemesh.infra.metrics :as metrics]))

(use-fixtures :each (fn [f] (metrics/reset-all!) (f) (metrics/reset-all!)))

(deftest counters-accumulate
  (metrics/inc-counter! "pulsemesh_commands_accepted_total")
  (metrics/inc-counter! "pulsemesh_commands_accepted_total")
  (metrics/inc-counter! "pulsemesh_commands_rejected_total" 3)
  (let [out (metrics/render)]
    (is (str/includes? out "pulsemesh_commands_accepted_total 2"))
    (is (str/includes? out "pulsemesh_commands_rejected_total 3"))))

(deftest known-counters-always-present-at-zero
  (let [out (metrics/render)]
    (testing "scrapers see a stable series even before any traffic"
      (is (str/includes? out "pulsemesh_commands_accepted_total 0"))
      (is (str/includes? out "pulsemesh_commands_conflict_total 0")))))

(deftest summary-records-count-and-sum
  (metrics/observe! "pulsemesh_command_latency_seconds" 0.010)
  (metrics/observe! "pulsemesh_command_latency_seconds" 0.030)
  (let [out (metrics/render)]
    (is (str/includes? out "pulsemesh_command_latency_seconds_count 2"))
    (is (str/includes? out "pulsemesh_command_latency_seconds_sum 0.040000"))))

(deftest timed-macro-observes-duration
  (metrics/timed "pulsemesh_command_latency_seconds" (+ 1 1))
  (is (str/includes? (metrics/render)
                     "pulsemesh_command_latency_seconds_count 1")))

(deftest render-is-valid-prometheus-text
  (metrics/inc-counter! "pulsemesh_commands_accepted_total")
  (let [out (metrics/render)]
    (is (str/includes? out "# TYPE pulsemesh_commands_accepted_total counter"))
    (is (str/includes? out "# HELP pulsemesh_commands_accepted_total"))))
