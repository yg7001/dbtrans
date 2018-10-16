(ns dbtrans.main  (:gen-class)
 (:require  [clojure.string :as str]
   [clojure.java.jdbc :as db]
   [clojure.tools.logging :as log]
   [clj-time.jdbc]   
   [dbtrans.data.recorder :as rr]
   [dbtrans.data.dbtools :as dt]   
   [dbtrans.utils.timeutils :as tu]
   [dbtrans.config :as cfg]
   [dbtrans.trans.tools :as tt]
   ;[dbtrans.trans.p2p :as  td]
   [dbtrans.trans.pfp :as  td]   
   [dbtrans.cli :as cli]
 )
)
(def root-path (System/getProperty "user.dir"))

(defn trans-dicts! [^clojure.lang.PersistentVector all-dicts & {:keys [limit] :or {limit 0}}]
  (tt/yield-prn "[=== all-dicts count=" (count all-dicts), ",limit=" limit " ===]")
  (let [tables (if (zero? limit) all-dicts (take limit all-dicts)),tc (atom 0), dc (atom 0)] 
    (doseq [tbl tables]
      (swap! tc inc)
      (tt/yield-pr "## << dict table " (format "%04d" @tc) ">> [" tbl "]")
      (time (when (> (td/trans-dict! tbl) 0) (swap! dc inc) ) )
    ) ;;end doseq of businesss
    (tt/yield-prn "======= Really transported dicts count : [" @dc "] =======")
  )
)

(defn trans-businesses!
  " transport business tables data "
  [^clojure.lang.PersistentVector all-businesses & {:keys [limit] :or {limit 0}}]
  (tt/yield-prn "[=== all-businesses count=" (count all-businesses) , ",limit=" limit " ===]")
  (let [tables (if (zero? limit) all-businesses (take limit all-businesses)),tc (atom 0)]
    (doseq [tbl tables]
      (swap! tc inc)
      (tt/yield-pr "## << business table " (format "%04d" @tc) ">> [" tbl "]")
      (time (td/trans-business! tbl))
    ) ;;end doseq of businesss
  )
)

(defn exit [status msg]   (println msg)  (System/exit status))

(defn -main
  "this is main func"
  [& args]
  (log/info "run with arguments = " args  "at=" root-path);;*command-line-args*)   
  (let [{:keys [action options exit-message ok?]} (cli/validate-args args)]
    (prn options)
    (if exit-message    (exit (if ok? 0 1) exit-message) )
    ;;else
    (let [{:keys [limit dlimit blimit] :or {limit "0"}} options,d-limit (if (nil? dlimit) limit dlimit),b-limit (if (nil? blimit) limit blimit)]
      (println "limit=" limit ",dl=" d-limit ,",bl=" b-limit)
      ;(log/with-logs 
        (try
          (time 
            (do
             (rr/load-records)  (dt/query-ora "select count(*) from user_tables") (dt/query-pg "select count(*) from pg_database")
              (when (not (neg? d-limit)) (trans-dicts! (dt/get-ora-dict-tables) :limit d-limit) )
             (when (not (neg? b-limit)) (trans-businesses! (dt/get-ora-business-tables) :limit b-limit) )  
              (rr/save)
              "done!"
            )
          )
          (catch Exception e      (do  (log/error e)  (rr/save)     )
          )
        )
      ;)
    )
  )
)