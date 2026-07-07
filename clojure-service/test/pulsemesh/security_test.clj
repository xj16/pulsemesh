(ns pulsemesh.security-test
  "Unit tests for the security middleware. No infra: these drive plain Ring
   request maps through each middleware and assert the response, so the
   rate-limiter, body guard, auth hook, and CORS logic are all covered
   directly (these were previously entirely untested)."
  (:require [clojure.test :refer [deftest is testing]]
            [pulsemesh.infra.security :as sec]))

(defn- ok-handler [_req] {:status 200 :headers {} :body "ok"})

;; ---------------------------------------------------------------------------
;; Rate limiting
;; ---------------------------------------------------------------------------

(deftest rate-limit-allows-burst-then-blocks
  (let [h (sec/wrap-rate-limit ok-handler {:rps 0 :burst 3})
        req {:remote-addr "1.2.3.4"}
        codes (repeatedly 5 #(:status (h req)))]
    (testing "first `burst` requests pass, the rest are 429"
      (is (= [200 200 200 429 429] codes)))))

(deftest rate-limit-is-per-client
  (let [h (sec/wrap-rate-limit ok-handler {:rps 0 :burst 1})]
    (is (= 200 (:status (h {:remote-addr "a"}))))
    (is (= 429 (:status (h {:remote-addr "a"}))))
    (is (= 200 (:status (h {:remote-addr "b"})))
        "a different client has its own bucket")))

(deftest rate-limit-refills-over-time
  (let [allow? (sec/make-limiter {:rps 1000 :burst 1})]
    (is (true? (allow? "k")))
    (is (false? (allow? "k")))
    (Thread/sleep 15)                    ;; 1000 rps => refills within 15ms
    (is (true? (allow? "k")) "bucket refilled")))

(deftest rate-limit-can-be-disabled
  (let [h (sec/wrap-rate-limit ok-handler {:enabled false :burst 0})]
    (is (= 200 (:status (h {:remote-addr "x"}))))))

;; ---------------------------------------------------------------------------
;; Body-size guard
;; ---------------------------------------------------------------------------

(deftest body-limit-rejects-oversized
  (let [h (sec/wrap-body-limit ok-handler {:max-bytes 100})]
    (is (= 413 (:status (h {:headers {"content-length" "1000"}}))))
    (is (= 200 (:status (h {:headers {"content-length" "50"}}))))
    (is (= 200 (:status (h {:headers {}})))
        "no content-length => passes (chunked/absent)")))

;; ---------------------------------------------------------------------------
;; Auth
;; ---------------------------------------------------------------------------

(deftest auth-noop-when-unconfigured
  (let [h (sec/wrap-auth ok-handler {})]
    (is (= 200 (:status (h {:headers {}})))
        "no secret/tokens => auth disabled, request passes")))

(deftest auth-shared-secret-gate
  (let [h (sec/wrap-auth ok-handler {:shared-secret "s3cret"})]
    (testing "missing/invalid token => 401"
      (is (= 401 (:status (h {:headers {}}))))
      (is (= 401 (:status (h {:headers {"authorization" "Bearer nope"}})))))
    (testing "valid token => passes"
      (is (= 200 (:status (h {:headers {"authorization" "Bearer s3cret"}
                              :body-params {:user-id "alice"}})))))))

(deftest auth-token-map-pins-principal
  ;; The stricter posture: a token maps to a fixed principal, and the body's
  ;; user-id is OVERWRITTEN so a caller cannot impersonate anyone else.
  (let [captured (atom nil)
        capture  (fn [req] (reset! captured (get-in req [:body-params :user-id]))
                   {:status 200})
        h (sec/wrap-auth capture {:tokens {"tok-bob" "bob"}})]
    (h {:headers {"authorization" "Bearer tok-bob"}
        :body-params {:user-id "alice"}})       ;; caller lies, claims alice
    (is (= "bob" @captured)
        "user-id is forced to the token's principal, not the body's claim")))

;; ---------------------------------------------------------------------------
;; CORS
;; ---------------------------------------------------------------------------

(deftest cors-allows-listed-origin
  (let [h (sec/wrap-cors ok-handler {:allowed #{"http://good.example"}})
        resp (h {:request-method :get :headers {"origin" "http://good.example"}})]
    (is (= "http://good.example"
           (get-in resp [:headers "Access-Control-Allow-Origin"])))))

(deftest cors-omits-headers-for-unlisted-origin
  (let [h (sec/wrap-cors ok-handler {:allowed #{"http://good.example"}})
        resp (h {:request-method :get :headers {"origin" "http://evil.example"}})]
    (is (nil? (get-in resp [:headers "Access-Control-Allow-Origin"])))))

(deftest cors-preflight-answers-204
  (let [h (sec/wrap-cors ok-handler {:allowed #{"http://good.example"}})
        resp (h {:request-method :options :headers {"origin" "http://good.example"}})]
    (is (= 204 (:status resp)))
    (is (= "http://good.example"
           (get-in resp [:headers "Access-Control-Allow-Origin"])))))

(deftest cors-wildcard-and-default-normalization
  (is (= :all (sec/normalize-allowed "*")))
  (is (set? (sec/normalize-allowed :default)))
  (is (= #{"http://one"} (sec/normalize-allowed "http://one")))
  (let [h (sec/wrap-cors ok-handler {:allowed "*"})
        resp (h {:request-method :get :headers {"origin" "http://anything"}})]
    (is (= "http://anything"
           (get-in resp [:headers "Access-Control-Allow-Origin"])))))
