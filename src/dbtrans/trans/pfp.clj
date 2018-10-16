(ns dbtrans.trans.pfp
  (:require  [clojure.string :as str]
   [clojure.java.jdbc :as db]
   [dbtrans.data.recorder :as rr]
   [dbtrans.data.dbtools :as dt]
   [dbtrans.data.struhelper :as stru]
   [clj-time.jdbc]
   [dbtrans.utils.timeutils :as tu]
   [clojure.java.shell :as shell]
   [clojure.java.io :as io]
   [dbtrans.config :as cfg]
   [dbtrans.trans.tools :as tt]
   [clojure.tools.logging :as log]
   ))
;; (let [{:keys[exit out err]} (shell/sh "psql/psql.exe"  "-depbd" "-h192.168.58.15" "-Uyh" "-c\\dt"
;;    :out-enc "cp936"  )] (println "exit with " exit)(println out)(println "err:" err))

(def trans-time (tu/now))
(def batch-num 100)
(def tmppath (cfg/tmp-path))

(defn ^Integer trans-dict! [tbl]
  (let [tblpg (str/lower-case tbl),dat (str (tt/concat-path tmppath tblpg) ".dat") ,in-cache (rr/exists-dict? tbl)
        in-pg (dt/exists-pg-table? tblpg),need-trans? (if in-pg (tt/dict-updated? tbl) true)]
    (tt/prepare-dict-table tblpg)
    (if need-trans? 
      (do        
        (let [rs (dt/query-ora (str "select * from " tbl) ) , total (atom 0) ,bc (atom 0), cols (ref []),rows (ref [])]
          (if (seq rs) ;; if has data?            
              (do (tt/yield-pr "[data trans started] [exporting to file]")
              (dosync (ref-set cols (vec (keys (first rs))))) 
                (io/delete-file (io/file dat) true)
              (with-open [fh (io/writer dat :encoding "UTF-8" :buffer-size 10240)]
                (doseq [row rs]
                  (swap! total  inc )     
                  (swap! bc inc)
                  (dosync (alter rows conj (str/join \| (vals (tt/prepare-row tblpg row) ))))
                  (if (= @bc batch-num) 
                      (do (tt/yield-pr ".")
                          (.write fh (if (> @total batch-num) (str "\n" (str/join "\n" @rows)) (str/join "\n" @rows)))                       
                          (tt/yield-pr "w")
                          (dosync (ref-set rows [])) (reset! bc 0)                       
                     )
                  ) ;; end if              
                       ) ;; end doseq
                    (if (> @bc 0) 
                      (do (tt/yield-pr ".")
                      (.write fh (if (> @total batch-num) (str "\n" (str/join "\n" @rows)) (str/join "\n" @rows))  ) 
                      (tt/yield-pr "w")) ) ;; remaining rows
                   (tt/yield-prn " [" @total "] rows")        
               (rr/up-dict tblpg trans-time @total)
             );; end with-open
                   (tt/yield-pr "[importing...]")
               (if (zero? (tt/import-data tblpg (str/join \, @cols) dat))
                 (do (tt/yield-pr " [total " @total " rows]")        
                   (rr/up-dict tblpg trans-time @total)
                     1
                 ) )
          
            );;end do has data
                ;; else if empty dataset
              (do (tt/yield-prn " no data!")
              (rr/up-dict tblpg trans-time 0)
                0
            );;end do no-data
          );;end if test empty? dataset
        );; end let  query data
      );; end need-trans? do
      (do  ;; no need trans
        (tt/yield-prn "\t[no updated, skiped!]")
        0
      )
    );; end if need trans
  );; end let each table
)


(defn ^Integer trans-business! [tbl]  
  (let [tblpg (str/lower-case tbl),dat (str (tt/concat-path tmppath tblpg) ".dat"),last-time (tt/get-real-last-time tblpg)
          my-time (rr/calc-transtime last-time trans-time) ]
    (tt/yield-pr "trans data between [" (tu/datetime-str-l last-time) "] and [" (tu/datetime-str-l my-time) "]\n")
    (tt/prepare-business-table tblpg)        
    (let [rs (dt/query-ora (tt/get-my-sql tblpg last-time my-time)) , total (atom 0) ,bc (atom 0), cols (ref []),rows (ref []) ]
      (if (seq rs) ;; if has data?            
        (do (tt/yield-pr "[data trans started] [exporting to file]")
            (dosync (ref-set cols (vec (keys (first rs)))))
           (io/delete-file (io/file dat) true)
           (with-open [fh (io/writer dat :encoding "UTF-8" :buffer-size 10240)]
              (doseq [row rs]
                (swap! total  inc )     
                (swap! bc inc)
                (dosync (alter rows conj (str/join \| (vals (tt/prepare-row tblpg row) ))))
                (if (= @bc batch-num) 
                   (do (tt/yield-pr ".")
                     (.write fh (if (> @total batch-num) (str "\n" (str/join "\n" @rows)) (str/join "\n" @rows)))
                       (tt/yield-pr "w")
                       (dosync (ref-set rows [])) (reset! bc 0)                       
                   )
                ) ;; end if              
              ) ;; end doseq
              (if (> @bc 0) (do (tt/yield-pr ".")
                             (.write fh (if (> @total batch-num) (str "\n" (str/join "\n" @rows)) (str/join "\n" @rows))  ) 
                             (tt/yield-pr "w")) )  ;; remaining rows 
             );; end with-open
             (tt/yield-pr "[importing...]")
            (if (zero? (tt/import-data tblpg (str/join \, @cols) dat))
               (do (tt/yield-pr " [total " @total " rows]")        
                 (rr/up-business tblpg my-time @total)
               )
               1
            )
          );; end do has data
        ;; else if empty dataset
        (do (tt/yield-prn " no data!")
            (rr/up-business tblpg my-time 0)
            0
        );;end do no-data
      );;end if empty? dataset
    );; end let  query data
  );; end let each table
)


