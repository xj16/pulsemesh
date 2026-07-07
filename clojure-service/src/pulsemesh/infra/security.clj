(ns pulsemesh.infra.security
  "Dependency-free Ring middleware for the command/query HTTP surface:

     * rate limiting      — token bucket per client, in-memory
     * request-size guard — reject oversized bodies before parsing
     * optional auth       — bearer-token hook (off by default) that, when
                             enabled, authenticates the caller and derives the
                             acting user-id from the token principal instead of
                             trusting the request body
     * CORS                — configurable allow-list (the browser dashboard on
                             the fabric port calls this service cross-origin)

   Everything is toggleable via config/env so the demo stays frictionless while
   the production posture is a single flag away. No external libraries: these
   are plain higher-order handlers so the security story is auditable in one
   file."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]))

;; ===========================================================================
;; Rate limiting — a per-key token bucket.
;;
;; Each client (keyed by principal or IP) gets `burst` tokens that refill at
;; `rps` tokens/second. A request costs one token; empty bucket => 429. This
;; smooths spikes while allowing sustained `rps`. State is a single atom of
;; {key -> {:tokens double :ts millis}} — fine for a single-node reference
;; service; a distributed deployment would move this to Redis.
;; ===========================================================================

(defn make-limiter
  "Create a rate-limiter state + check-fn closure.
   Opts: :rps (refill rate), :burst (bucket size)."
  [{:keys [rps burst] :or {rps 20 burst 40}}]
  (let [buckets (atom {})
        rps     (double rps)
        burst   (double burst)]
    (fn allow? [k]
      (let [now (System/currentTimeMillis)
            ;; swap! returns the whole map; pull this key's :ok flag out of it.
            m'  (swap! buckets
                       (fn [m]
                         (let [{:keys [tokens ts] :or {tokens burst ts now}} (get m k)
                               elapsed  (/ (- now ts) 1000.0)
                               refilled (min burst (+ tokens (* elapsed rps)))]
                           (if (>= refilled 1.0)
                             (assoc m k {:tokens (- refilled 1.0) :ts now :ok true})
                             (assoc m k {:tokens refilled :ts now :ok false})))))]
        (get-in m' [k :ok])))))

(defn- client-key [request]
  (or (get-in request [:headers "x-forwarded-for"])
      (:remote-addr request)
      "unknown"))

(defn wrap-rate-limit
  "429 when the caller exceeds its token bucket."
  [handler opts]
  (if-not (:enabled opts true)
    handler
    (let [allow? (make-limiter opts)]
      (fn [request]
        (if (allow? (client-key request))
          (handler request)
          {:status 429
           :headers {"Content-Type" "application/json"
                     "Retry-After" "1"}
           :body "{\"status\":\"rate-limited\",\"reason\":\"too many requests\"}"})))))

;; ===========================================================================
;; Request-size guard — reject bodies larger than `max-bytes` up front. The
;; command schema already caps message bodies at 4000 chars; this is a coarse
;; outer bound so a giant payload can't be buffered/parsed at all.
;; ===========================================================================

(defn wrap-body-limit
  [handler {:keys [max-bytes] :or {max-bytes 65536}}]
  (fn [request]
    (let [len (some-> (get-in request [:headers "content-length"])
                      (parse-long))]
      (if (and len (> len max-bytes))
        {:status 413
         :headers {"Content-Type" "application/json"}
         :body "{\"status\":\"too-large\",\"reason\":\"request body too large\"}"}
        (handler request)))))

;; ===========================================================================
;; Optional bearer-token auth.
;;
;; Disabled by default. When a shared secret (or a token->user map) is
;; configured, the middleware:
;;   * requires `Authorization: Bearer <token>` on /api/commands,
;;   * resolves the token to a principal (user-id), and
;;   * OVERWRITES the command body's :user-id with that principal, so a caller
;;     can only ever act as themselves. This is the honest fix for "the API
;;     trusts user-id from the body".
;;
;; Two modes:
;;   :shared-secret "s"    -> any request with that token authenticates as the
;;                            body's user-id (identity asserted, transport
;;                            secured) — a minimal gate for the demo.
;;   :tokens {"tok" "uid"} -> token maps to a fixed principal; body user-id is
;;                            replaced by it. This is the stricter posture.
;; ===========================================================================

(defn- bearer-token [request]
  (some-> (get-in request [:headers "authorization"])
          (as-> h (when (str/starts-with? (str/lower-case h) "bearer ")
                    (str/trim (subs h 7))))))

(defn wrap-auth
  "Optional authentication. `opts` may contain :shared-secret and/or :tokens.
   If neither is present, auth is a no-op (frictionless demo)."
  [handler {:keys [shared-secret tokens] :as opts}]
  (if (and (str/blank? (str shared-secret)) (empty? tokens))
    handler
    (fn [request]
      (let [token (bearer-token request)
            principal (cond
                        (and token (contains? tokens token)) (get tokens token)
                        (and token shared-secret (= token shared-secret)) ::asserted
                        :else nil)]
        (cond
          (nil? principal)
          {:status 401
           :headers {"Content-Type" "application/json"
                     "WWW-Authenticate" "Bearer"}
           :body "{\"status\":\"unauthorized\",\"reason\":\"missing or invalid bearer token\"}"}

          ;; Token mapped to a concrete principal: pin the acting user-id to it
          ;; so the body can't impersonate someone else.
          (not= principal ::asserted)
          (handler (assoc request ::principal principal
                          :body-params (some-> (:body-params request)
                                               (assoc :user-id principal))))

          ;; Shared-secret mode: transport is authenticated, identity is taken
          ;; from the (now trusted) body.
          :else
          (handler (assoc request ::principal (get-in request [:body-params :user-id]))))))))

;; ===========================================================================
;; CORS — needed so the browser dashboard served by the fabric (:8090) can POST
;; commands to this service (:8080). Allow-list is configurable; default echoes
;; any localhost origin for the local stack and nothing else.
;; ===========================================================================

(def ^:private default-cors-origins
  #{"http://localhost:8090" "http://127.0.0.1:8090"
    "http://localhost:8080" "http://127.0.0.1:8080"})

(defn normalize-allowed
  "Translate a configured :allowed value into what origin-allowed? expects:
     :default / nil -> the built-in localhost allow-list
     \"*\"          -> :all (echo any origin; dev only)
     a set          -> used as-is
     a single origin string -> a one-element set"
  [allowed]
  (cond
    (or (nil? allowed) (= allowed :default)) default-cors-origins
    (= allowed "*")  :all
    (= allowed :all) :all
    (set? allowed)   allowed
    (string? allowed) #{allowed}
    :else default-cors-origins))

(defn- origin-allowed? [allowed origin]
  (cond
    (nil? origin) false
    (= allowed :all) true
    (set? allowed) (contains? allowed origin)
    :else false))

(defn wrap-cors
  "Adds CORS headers for allowed origins and answers preflight OPTIONS.
   `:allowed` may be :default, \"*\", a set, or a single origin string."
  [handler {:keys [allowed]}]
  (let [allowed (normalize-allowed allowed)]
   (fn [request]
    (let [origin (get-in request [:headers "origin"])
          ok?    (origin-allowed? allowed origin)
          cors   (when ok?
                   {"Access-Control-Allow-Origin"  origin
                    "Access-Control-Allow-Methods" "GET, POST, OPTIONS"
                    "Access-Control-Allow-Headers" "Content-Type, Authorization"
                    "Access-Control-Max-Age"       "600"
                    "Vary"                          "Origin"})]
      (if (= :options (:request-method request))
        {:status 204 :headers (or cors {}) :body ""}
        (let [resp (handler request)]
          (update resp :headers merge cors)))))))

;; ===========================================================================
;; Compose the outer security stack (everything that does NOT need a parsed
;; body). Auth is applied separately as route middleware in the API layer,
;; because it must run AFTER muuntaja has parsed :body-params so it can pin the
;; acting user-id to the authenticated principal.
;; ===========================================================================

(defn wrap-outer
  "Wrap `handler` with CORS, rate limiting, and the body-size guard. These run
   before request-body parsing. A nil/empty config leaves the demo open except
   the coarse body limit, which is always-on cheap safety."
  [handler {:keys [rate-limit body-limit cors] :as _sec-cfg}]
  (-> handler
      (wrap-rate-limit (or rate-limit {}))
      (wrap-body-limit (or body-limit {}))
      (wrap-cors (or cors {}))))

(defn auth-middleware
  "Return a reitit-compatible middleware value for the write route. Applied
   after body parsing so it can rewrite :user-id to the authenticated
   principal. No-ops (identity) when auth is unconfigured."
  [auth-cfg]
  (when (or (not (str/blank? (str (:shared-secret auth-cfg))))
            (seq (:tokens auth-cfg)))
    (log/info "command/query auth ENABLED (bearer token required on writes)"))
  (fn [handler] (wrap-auth handler (or auth-cfg {}))))
