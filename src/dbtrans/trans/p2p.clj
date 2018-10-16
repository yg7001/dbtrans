(ns dbtrans.trans.p2p
  (:require  [clojure.string :as str]
   [clojure.java.jdbc :as db]
   [dbtrans.data.recorder :as rr]
   [dbtrans.data.dbtools :as dt]
   [clj-time.jdbc]
   [dbtrans.utils.timeutils :as tu]
   [dbtrans.data.struhelper :as stru]
   [dbtrans.config :as cfg]
   [dbtrans.trans.tools :as tt] 
   [clojure.tools.logging :as log]
   ))


(def trans-time (tu/now))
(def batch-num 100)


(defn ^Integer trans-dict![^String tbl]
  (let [tblpg (str/lower-case tbl) ,in-cache (rr/exists-dict? tbl) , 
        in-pg (dt/exists-pg-table? tblpg),need-trans? (if in-pg (tt/dict-updated? tbl) true)]
    (tt/prepare-dict-table tblpg)
    (if need-trans? 
      (do              
        (let [rs (dt/query-ora (str "select * from " tbl) ) , total (atom 0) ,bc (atom 0)
            cols (ref []),rows (ref []) , hasblob? (dt/ora-table-has-blob? tblpg)]
          (if (seq rs) ;; if has data?            
              (do (tt/yield-pr "[data trans started]")
                (dosync (ref-set cols (vec (keys (first rs))))) 
                (doseq [row rs]
                  (swap! total  inc )     
                  (swap! bc inc)
                  (dosync (alter rows conj (vec (vals (tt/prepare-row tblpg row) ))))
                  (if (= @bc batch-num) 
                     (do (tt/batch-trans tblpg @cols @rows)
                     ;(do  (tt/yield-pr ".")
                         (dosync (ref-set rows [])) (reset! bc 0)                       
                     )
                  ) ;; end if              
                ) ;; end doseq
                (if (> @bc 0) (tt/batch-trans tblpg @cols @rows) )
                (tt/yield-prn " [" @total "] rows")        
                (rr/up-dict tblpg trans-time @total)
                1
              )
              ;; else if empty dataset
              (do (tt/yield-prn " no data!")
                  (rr/up-dict tblpg trans-time 0)
                  0
              )
           );;end if empty? dataset
        );; end let  query data
      );; end need-trans? do
      (do  ;; no need trans
        (tt/yield-prn "\t[no updated, skiped!]")
        0
      )
    );; end if need trans
  );; end let each table
)

(defn ^Integer trans-business![tbl]
  (tt/yield-pr "##  transfering business [" tbl "]")
     (let [tblpg (str/lower-case tbl),last-time (tt/get-real-last-time tblpg)
            my-time (rr/calc-transtime last-time trans-time)]
        (tt/yield-pr "trans data between [" (tu/datetime-str-l last-time) "] and [" (tu/datetime-str-l my-time) "]\n")
        (tt/prepare-business-table tblpg)        
        (let [rs (dt/query-ora (tt/get-my-sql tblpg last-time my-time)) , total (atom 0) ,bc (atom 0)
              cols (ref []),rows (ref []) , hasblob? (dt/ora-table-has-blob? tblpg)]
          (if (seq rs) ;; if has data?            
            (do (tt/yield-pr "[data trans started]")
              (dosync (ref-set cols (vec (keys (first rs))))) 
              (doseq [row rs]
                  (swap! total  inc )     
                  (swap! bc inc)
                  (dosync (alter rows conj (vec (vals (tt/prepare-row tblpg row) ))))
                  (if (= @bc batch-num) 
                     (do (tt/batch-trans tblpg @cols @rows)
                     ;(do  (tt/yield-pr ".")
                       (dosync (ref-set rows [])) (reset! bc 0)                       
                     )
                  ) ;; end if              
                ) ;; end doseq
                    (if (> @bc 0) (tt/batch-trans tblpg @cols @rows) )
                (tt/yield-prn " [" @total "] rows")        
                (rr/up-business tblpg my-time @total)
            )
            ;; else if empty dataset
            (do (tt/yield-prn " no data!")
                (rr/up-business tblpg my-time 0)
                0
            )
         );;end if empty? dataset
    );; end let  query data
  )
)

;(defn trans-dicts! [^clojure.lang.PersistentVector all-dicts & {:keys [limit] :or {limit 0}}]
;  (tt/yield-prn "[=== all-dicts count=" (count all-dicts), ",limit=" limit " ===]")
;  (let [tables (if (zero? limit) all-dicts (take limit all-dicts)),tc (atom 0)] 
;    (doseq [tbl tables]
;      (swap! tc inc)
;      (tt/yield-pr "## dict table " (format "%04d" @tc) "[" tbl "]")
;      (time (trans-dict! tbl))
;    ) ;;end doseq of businesss
;  )
;)
;
;(defn trans-businesses!
;  " transport business tables data "
;  [^clojure.lang.PersistentVector all-businesses & {:keys [limit] :or {limit 0}}]
;  (tt/yield-prn "[=== all-businesses count=" (count all-businesses) , ",limit=" limit " ===]")
;  (let [tables (if (zero? limit) all-businesses (take limit all-businesses)),tc (atom 0)]
;    (doseq [tbl tables]
;     (swap! tc inc)
;      (tt/yield-pr "## business table " (format "%04d" @tc) "[" tbl "]")
;      (time (trans-business! tbl))
;    ) ;;end doseq of businesss
;  )
;)
