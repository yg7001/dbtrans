(ns dbtrans.config   (:require [clojure.edn :as edn]    [clojure.string :as str] ) )
(def settings (ref {}))

(defn db-prop[dbsec,pname]   (-> @settings dbsec pname) )

(defn db-pg-prop[pname]   (db-prop :db-pg pname))
(defn db-ora-prop[pname]  (db-prop :db-ora pname))
(defn pg-user[]  (db-pg-prop :user))
(defn pg-pass[]  (db-pg-prop :pass))
(defn pg-host[]  (db-pg-prop :host))
(defn pg-db[]  (db-pg-prop :db))
(defn pg-port[]  (db-pg-prop :port))

(defn ora-user[]  (db-ora-prop :user))
(defn ora-pass[]  (db-ora-prop :pass))
(defn ora-host[]  (db-ora-prop :host))
(defn ora-db[]  (db-ora-prop :db))
(defn ora-port[]  (db-ora-prop :port))

(defn max-days[]  (-> @settings :trans :max-days))
(defn tmp-path[]  (-> @settings :trans :tmp-path))
(defn interval
  "return interval in unit of miliseconds"
  []
  (let [rs (re-find #"(\d+)\s*(\w+)" (-> @settings :trans :interval)) ,sn (rs 1),n (Long/parseLong sn),unit (str/lower-case (rs 2))]
    (println (str "num->[" sn "] unit->[" unit "]"))
    (case unit
      ("h" "hour" "hours") (* n 60 60 1000)
      ("m" "min" "minute" "minutes") (* n 60 1000)
      ("s" "sec" "second" "seconds") (* n 1000)
      ("ms" "milisecond" "miliseconds") n
      n
    )
  )
)

(defn reload[]
  (dosync (ref-set settings  (edn/read-string (slurp "config.edn"))))
)
;; load when required
(reload)
