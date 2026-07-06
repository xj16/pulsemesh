(ns pulsemesh.infra.config
  "Environment-driven configuration via Aero. Every value has a sane default
   so the service can boot in a local docker-compose network with no extra
   env, while still being fully overridable in production."
  (:require [aero.core :as aero]
            [clojure.java.io :as io]))

(defn load-config
  "Read resources/config.edn, resolving #env / #or tags. Optionally takes a
   profile keyword (:dev / :prod / :test) exposed to the edn as #profile."
  ([] (load-config :prod))
  ([profile]
   (aero/read-config (io/resource "config.edn") {:profile profile})))
