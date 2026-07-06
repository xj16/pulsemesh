(ns pulsemesh.api
  "HTTP surface for the command/query service. A thin adapter: it decodes
   requests, delegates to the command/query handlers, and maps domain results
   to HTTP status codes. No business logic lives here."
  (:require [reitit.ring :as ring]
            [reitit.ring.middleware.muuntaja :as muuntaja]
            [reitit.ring.middleware.parameters :as parameters]
            [muuntaja.core :as m]
            [pulsemesh.command.handler :as command]
            [pulsemesh.query.handler :as query]))

;; ---------------------------------------------------------------------------
;; Command endpoint: POST a command, get back the emitted events.
;; ---------------------------------------------------------------------------

(defn- command-handler [sys]
  (fn [request]
    (let [cmd    (:body-params request)
          result (command/handle sys cmd)]
      (case (:status result)
        :accepted {:status 201 :body {:status "accepted"
                                      :events (:events result)}}
        :rejected {:status 409 :body {:status "rejected"
                                      :reason (name (:reason result))}}
        :conflict {:status 503 :body {:status "conflict"
                                      :reason "too many concurrent writers, retry"}}
        :invalid  {:status 400 :body {:status "invalid"
                                      :problems (:problems result)}}))))

;; ---------------------------------------------------------------------------
;; Query endpoints.
;; ---------------------------------------------------------------------------

(defn- presence-handler [sys]
  (fn [{{:keys [channel-id]} :path-params}]
    {:status 200 :body (query/channel-presence sys channel-id)}))

(defn- messages-handler [sys]
  (fn [{{:keys [channel-id]} :path-params
        {:strs [limit]} :query-params}]
    (let [n (some-> limit (Long/parseLong))]
      {:status 200 :body (query/recent-messages sys channel-id n)})))

(defn- history-handler [sys]
  (fn [{{:keys [channel-id]} :path-params}]
    {:status 200 :body (query/channel-history sys channel-id)}))

(defn- health-handler [_sys]
  (fn [_request]
    {:status 200 :body {:status "ok" :service "pulsemesh-command-query"}}))

;; ---------------------------------------------------------------------------
;; Router.
;; ---------------------------------------------------------------------------

(defn router [sys]
  (ring/router
   [["/health" {:get (health-handler sys)}]
    ["/api"
     ["/commands"                    {:post (command-handler sys)}]
     ["/channels/:channel-id"
      ["/presence"                   {:get (presence-handler sys)}]
      ["/messages"                   {:get (messages-handler sys)}]
      ["/history"                    {:get (history-handler sys)}]]]]
   {:data {:muuntaja   m/instance
           :middleware [parameters/parameters-middleware
                        muuntaja/format-middleware]}}))

(defn handler
  "Build the top-level ring handler, including a JSON 404 fallback."
  [sys]
  (ring/ring-handler
   (router sys)
   (ring/create-default-handler
    {:not-found (constantly {:status 404
                             :headers {"Content-Type" "application/json"}
                             :body "{\"error\":\"not found\"}"})})))
