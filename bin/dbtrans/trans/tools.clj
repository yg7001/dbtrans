(ns dbtrans.trans.tools
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
   [clojure.tools.logging :as log]
 ))

(def  cache (ref []))
(defn yield[] (Thread/sleep 0))
(defn yield-pr[& ps] 
  (let [ss (str/join " " ps)]   
    (dosync (alter cache conj ss))
    (apply print ps) (flush) (yield)
    (when (str/ends-with? ss "\n") (do (log/info (str/join " " @cache)) (dosync ( ref-set cache [])) ))
  )
)
(defn yield-prn[& ps]  (apply yield-pr (conj (vec ps) "\n") ) )

(defn concat-path[& paths]
  (str/join (java.io.File/separator) paths)
)

  
(defn drop-pg-table! [tbl]
  (dt/execute-pg (str "drop table " tbl))
  (yield-pr "\t[Table droped!]")
)  
(defn make-pg-table! [tbl]
  (let [sql (dt/get-pg-ddl tbl)]     ;(println "## create table [" tbl "] with sql => \n" sql)    
    (dt/execute-pg sql)   (yield-pr "\t[Table created]")
  )
)
(defn prepare-dict-table [tbl]
  (let [tblpg (str/lower-case tbl) ,in-pg (dt/exists-pg-table? tblpg)]
    (stru/register-tbl tblpg) 
    (if in-pg 
        ;(do (dt/execute-pg  (str "truncate table " tblpg)) )
        (do (drop-pg-table! tblpg) (make-pg-table! tblpg) )
        ;;else not exists
        (make-pg-table! tblpg)
    )  )  )

(defn prepare-business-table [tbl]
  (let [tblpg (str/lower-case tbl) ,in-pg (dt/exists-pg-table? tblpg)]
    (stru/register-tbl tblpg) 
    (if (not in-pg)
        (make-pg-table! tblpg)
        ;; else exists already        
        ;(do (dt/execute-pg  (str "truncate table " tblpg)) )
        (do (drop-pg-table! tblpg)  (make-pg-table! tblpg) )
    )  )  )
(defn get-db-mintime[tbl]
  (-> (dt/query-ora (str "select min(occur_time) from " (str/upper-case tbl) ))  first first last)
)
(defn get-real-last-time[tbl]
  (let [last-time (rr/get-last-transtime tbl), real-min (get-db-mintime tbl)]
     ;(println "last-time\t" last-time  " ,real-min\t" real-min)
     (if real-min  (if (tu/after? real-min last-time) real-min last-time)
       last-time)      
  ))
(defn get-my-sql [tbl,last-time,this-time]
  (let[ff "yyyy-mm-dd hh24:mi:ss.FF3",start (tu/datetime-str-l last-time),end (tu/datetime-str-l this-time)]
   (str "select * from " ,tbl, " where occur_time>=to_timestamp('" ,start, "','", 
        ff, "') and occur_time<to_timestamp('", end ,"','" ff "')" )
  ))

(defn dict-updated? [tbl]
  (let [last-time (rr/get-dict-transtime tbl),sqlcc (str "select count(*) from " tbl)
        ,cco (-> (dt/query-ora sqlcc) first vals first long) ,ccp (if (> cco 0) (-> (dt/query-pg sqlcc) first vals first long) 0)
        ,sql (str "select to_char(scn_to_timestamp(ora_rowscn),'yyyy-mm-dd hh24:mi:ss.FF3') scn from " tbl " where ora_rowscn=(select max(ora_rowscn) from " tbl ") and rownum=1")
        ]
    ;(println "last-time" last-time)
    (if (> cco 0) 
      (if (not= cco ccp) (do (prn "ccora=" cco ",ccpg=",ccp) true)
        (try
          (-> (dt/query-ora sql) first (get "scn")  (tu/parse) (tu/after? last-time))
        (catch java.sql.SQLException e  (tu/after? (tu/date-time 1900 1 1) last-time) ))
      )
      false
    )
  )
)


;(reduce #(do (println %2)(Thread/sleep 0) (inc %) ) 0 (db/reducible-query dt/pg-pool ["select * from yx_bw"] {:keywordize? false}))
(defn batch-trans[tblpg cols values]
  (yield-pr "!") 
  (dt/insert-pg-multi!  tblpg cols values) 
  (yield-pr ".")
)

(defn import-data[tbl,cols ,fp ]
  (let [ cmd (concat-path "psql"  "psql.exe"), ccc (str "\\copy " tbl "(" cols ") from '" fp "'  WITH DELIMITER '|' NULL '';") ]
    ;(println "cmd=" cmd)
    ;(println "ccc=" ccc)
    (let [{:keys [exit out err]} (shell/sh cmd  "-d" (cfg/pg-db) "-h" (first (str/split (cfg/pg-host) #":")) (when (cfg/pg-port) (str "-p" (cfg/pg-port)) )  "-U" (cfg/pg-user) "-c" ccc  :out-enc "cp936" :env {"PGPASSWORD" (cfg/pg-pass)} )]
      (if (= exit 0) (yield-pr "[data imported successful!]" out)
        (log/error "[error occured when import data! errmsg]" err)
      )
      exit
    )
  )
)
(defn replace-special[fld,rd]
  (let [val (rd fld),nv (str/replace val "\n" "<br/>"),sv (str/replace nv "\\" "/")]
    (into rd {fld sv})
  )
)
(defn replace-nil-special [tblname,rowmap]
  (let [tbl (str/lower-case tblname),fields (keys rowmap)]
    (when (not (stru/exists? tbl)) (stru/register-tbl tbl))
    (loop [flds fields,rd rowmap]
      (let [fld (first flds) , key (str tbl "."  fld), def-val (stru/fld-default key)]
        (if (next flds) 
          (if (nil? (rd fld)) 
            (recur (rest flds) (into rd {fld def-val }))
            (recur (rest flds) (replace-special fld rd) )
          )
          (if (nil? (rd fld))
            (into rd {fld  (stru/fld-default key)})
            (replace-special fld rd)
          )
        )
      )
    );; end loop 
  )
)
(defn prepare-row [tblname,rowmap]
  (let [hasblob? (stru/table-has-blob? tblname)]
    ;(if hasblob? (dt/trans-ora-blob rowmap))
   (if hasblob?  (replace-nil-special tblname (dt/trans-ora-blob rowmap))
     (replace-nil-special tblname rowmap)
    )
  )
)