(ns build
  "tools.build script for the PulseMesh Clojure command/query service.
   Produces an AOT-compiled uberjar at target/pulsemesh.jar."
  (:require [clojure.tools.build.api :as b]))

(def lib 'xj16/pulsemesh)
(def version "0.1.0")
(def class-dir "target/classes")
(def uber-file "target/pulsemesh.jar")

(defn- basis []
  (b/create-basis {:project "deps.edn"}))

(defn clean [_]
  (b/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs   ["src" "resources"]
               :target-dir class-dir})
  (b/compile-clj {:basis      (basis)
                  :src-dirs   ["src"]
                  :class-dir  class-dir
                  :ns-compile '[pulsemesh.main]})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis     (basis)
           :main      'pulsemesh.main})
  (println "Built" uber-file))
