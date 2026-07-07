(ns pulsemesh.main
  "Composition root. Wires config -> infrastructure -> HTTP handler and manages
   the lifecycle of the running system. Resilient boot: if Redis or RabbitMQ
   are momentarily unavailable, the service still starts (those paths degrade
   gracefully); Postgres is required since it is the source of truth."
  (:require [ring.adapter.jetty :as jetty]
            [pulsemesh.infra.config :as config]
            [pulsemesh.infra.db :as db]
            [pulsemesh.infra.migrate :as migrate]
            [pulsemesh.infra.redis :as redis]
            [pulsemesh.infra.rabbit :as rabbit]
            [pulsemesh.api :as api]
            [clojure.tools.logging :as log])
  (:gen-class))

(defn- try-connect
  "Attempt `f`, returning its result or nil (with a warning) on failure. Used
   for the optional infra so the service can boot degraded."
  [label f]
  (try
    (f)
    (catch Exception e
      (log/warn e (str label " unavailable at boot; continuing degraded"))
      nil)))

(defn start-system
  "Open all resources and start the HTTP server. Returns a system map suitable
   for `stop-system`."
  [profile]
  (let [cfg        (config/load-config profile)
        ds         (db/open-datasource (:postgres cfg))
        _          (migrate/migrate! ds)
        redis-conn (redis/make-conn (:redis cfg))
        publisher  (try-connect "RabbitMQ"
                                #(rabbit/connect (:rabbitmq cfg)))
        sys        {:config     cfg
                    :ds         ds
                    :redis-conn redis-conn
                    :publisher  publisher}
        handler    (api/handler sys)
        {:keys [host port]} (:http cfg)
        server     (jetty/run-jetty handler
                                    {:host host :port port :join? false})]
    (log/info "PulseMesh command/query service listening on" (str host ":" port))
    (assoc sys :server server)))

(defn stop-system [{:keys [server ds publisher]}]
  (some-> server .stop)
  (some-> publisher rabbit/close)
  (some-> ds db/close-datasource)
  (log/info "PulseMesh command/query service stopped"))

(defn -main [& _args]
  (let [profile (keyword (or (System/getenv "PULSEMESH_PROFILE") "prod"))
        system  (start-system profile)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn [] (stop-system system))))
    ;; Block the main thread on the (non-joining) Jetty server.
    (.join (:server system))))
