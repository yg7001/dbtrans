(ns dbtrans.utils.timeutils
    (:require (clj-time [core :as t][format :as f] [coerce :as c])
        [clojure.data.json :as json]
    )
)
(def clazz-datetime (type (t/now)))
(def format-multi (f/formatter (t/default-time-zone) "yyyy-MM-dd" "yyyy-MM-dd HH:mm" "yyyy-MM-dd HH:mm:ss" "yyyy-MM-dd HH:mm:ss.SSS" "yyyy-MM-dd'T'HH:mm:ss" "yyyy-MM-dd'T'HH:mm:ss.SSS" "yyyy-MM-dd'T'HH:mm:ssZ" "yyyy-MM-dd'T'HH:mm:ss.SSSZ"   ))
(def format-dt (f/formatter   "yyyy-MM-dd HH:mm:ss"  (t/default-time-zone) ))
(def format-dtl (f/formatter  "yyyy-MM-dd HH:mm:ss.SSS"  (t/default-time-zone) ))
(def format-date (f/formatter "yyyy-MM-dd" (t/default-time-zone) ))
(def format-time (f/formatter   "HH:mm:ss"  (t/default-time-zone) ))
(def format-timel (f/formatter  "HH:mm:ss.SSS"  (t/default-time-zone)  ))
;usage: 
;(f/parse format-multi "1990-12-18 10:01:11.112")
;(f/parse format-multi "1990-12-18T10:01:11.112")

(defn datetime-str
    ([]
        (f/unparse format-dt (t/now))
    )
    ([^org.joda.time.DateTime dt]
        (f/unparse format-dt dt)
    )
)
(defn datetime-str-l
    ([]
        (f/unparse format-dtl (t/now))
    )
     ([^org.joda.time.DateTime dt]
        (f/unparse format-dtl dt)
    )
)
(defn time-str
    ([]
        (f/unparse format-time (t/now))
    )
    ([^org.joda.time.DateTime dt]
        (f/unparse format-time dt)
    )
)
(defn time-str-l
    ([]
        (f/unparse format-timel (t/now))
    )
     ([^org.joda.time.DateTime dt]
        (f/unparse format-timel dt)
    )
)
(defn date-str
    ([]
        (f/unparse format-date (t/now))
    )
    ([^org.joda.time.DateTime dt]
        (f/unparse format-date dt)
    )
)
(defn parse[dtstr]
    (f/parse format-multi  dtstr)
)

(defn now[]  (t/now)   )

(defn date-time [year & args]
  (apply t/date-time year args)
)

(defn datetime-at [hh mm & sss]
  (apply t/today-at hh mm sss) 
)

(defn to-java-long [dt]
  (c/to-long dt)
)

(defn  from-java-long [n]
  (c/from-long n)
)

(defn after? [dt1 dt2] (t/after? dt1 dt2) )
(defn plus [dt & args] (apply t/plus dt args) )
(defn equal? [dt1 dt2] (t/equal? dt1 dt2))
(defn equal? [dt1 dt2] (t/equal? dt1 dt2))
