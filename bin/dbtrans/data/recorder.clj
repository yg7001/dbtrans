(ns  dbtrans.data.recorder
    (:require [clojure.java.io :as io]
        [clojure.string :as str]
        [clojure.data.json :as json]
        [clj-time.core :as t]
        [dbtrans.utils.timeutils :as tu]
        [dbtrans.config :as cfg]
     )
)
(def   trans-record-file "records.json")
(def   trans-record (ref (map {})))

(defn value-reader [key value]
  (if (str/ends-with? key "Time")
    (tu/parse value)
    value
  )
)
(defn value-writer [key value]
  (if (str/ends-with? key "Time")
    (tu/datetime-str-l value)
    value
  )
)

(defn  save[]
   (with-open [ww (io/writer  trans-record-file)]
        (let [js @trans-record]
          (json/write js ww  :value-fn value-writer)
            ;(binding [*out* ww]  ;; nouse now, pprint not support :value-fn
            ;    (json/pprint  js) )
        )
   ) )

(defn  load-records[]
    (try (with-open [rr (io/reader  trans-record-file)]
           (dosync (ref-set trans-record (json/read rr :eof-error? false :eof-value {} :value-fn value-reader)))
         )
    (catch java.io.FileNotFoundException e (dosync (ref-set trans-record {})))
    )
)
(defn reload[](load-records)) 
  

(defn  dict-data
 ([]
   (or (trans-record "dicts") [])
 )
 ([index]
    (get (dict-data) index)
 )
)
(defn  business-data
 ([]
    (or (trans-record "businesses") [])
 )
 ([index]
    (get (business-data) index)
 )
)


(def min-time (tu/date-time 1900))
  

(defn ^org.joda.time.DateTime calc-transtime [^org.joda.time.DateTime lasttime , ^org.joda.time.DateTime thistime]
  (let [days (cfg/max-days), endt (t/plus  lasttime (t/days days) ) ,toolong? (t/after?  thistime  endt ) ]
    (if toolong? endt  thistime)
  )
)

(defn  exists-item-name? [items name]
    (if (not items) false
      (loop [ss  items]
        (let  [item  (first ss)  thename (item "name")]
            ( if (=  thename name)
                true
                (if  (next ss) (recur (rest ss))  false)
            )
        )
      )
    )
)

(defn index-item-name [items name]    
  (if (not items) -1
    (let  [cc (count items)]
        (loop [ind 0]
            (if (= ind cc) -1
                (let [item (get items ind) , tn (item "name")]
                    (if (= tn name)  ind
                        (recur (inc ind))
                    )
                )
            )
        )    
    )
  )
)
(defn index-dict [name]
    (let  [items  (trans-record "dicts")]
        (index-item-name items name)
    )
)

(defn index-business [name]
    (let  [items  (trans-record "businesses")]
        (index-item-name items name)
    )
)

(defn  exists-dict? [tname]
    (let  [name (str/lower-case tname) ,items  (trans-record "dicts")]
        (exists-item-name?    items name)
     ) 
)

(defn  exists-business? [tname]
    (let  [name (str/lower-case tname) ,items  (trans-record "businesses")]
        (exists-item-name?    items name)
     ) 
)

(defn  up-dict [tname trans-time trans-count]
    (let [name (str/lower-case tname) ,ind (index-dict name),run-time (t/now) ,data {"name" name,"runTime" run-time, "transTime" trans-time "transCount" trans-count}]
        (if (= ind -1)
            (dosync (alter  trans-record  assoc  "dicts"  (conj (dict-data) data ) ) )
            (dosync (alter  trans-record  assoc  "dicts"  (assoc (dict-data) ind data ) ) )
        )
    )
    (save)
)

(defn  up-business [tname trans-time trans-count]
    (let [name (str/lower-case tname) ,ind (index-business name) ,run-time (t/now)
          data {"name" name, "transTime" trans-time, "runTime" run-time, "transCount" trans-count ,"totalCount" trans-count}]
        (if (= ind -1)
            (dosync (alter  trans-record  assoc  "businesses"  (conj (business-data) data ) ) )
            (let [old (business-data ind) tcc (+ trans-count (old "totalCount")) ]
                (dosync (alter  trans-record  assoc  "businesses"  (assoc (business-data) ind (assoc data "totalCount" tcc ) ) ))
            )
        )
    )
    (save)
)
(defn get-dict-transtime
  "get last time for trans data this table"
  [tname]
  (let [name (str/lower-case tname) ,ind (index-dict name) ,tt (if (= ind -1) min-time (-> (dict-data ind) (get "transTime"))) ]
      tt
  )
)
(defn get-last-transtime
  "get last time for trans data this table"
  [tname]
  (let [name (str/lower-case tname) ,ind (index-business name) ,tt (if (= ind -1) min-time (-> (business-data ind) (get "transTime"))) ]
      tt
  )
)
;(dosync (alter  trans-record  assoc  "dicts"  (conj (rec "dicts") {"name" "yx_device" "transTime" "1900" "transCount" 0} ) ) )

(reload)
