(ns dbtrans.data.struhelper
  (:require
    [clojure.string :as str]
    [dbtrans.data.dbtools :as dt]
  )
)

(def  stru-map (ref {}))

(defn exists? [^String tblname]
  (let [tbl (str/lower-case tblname)]
    (stru-map tbl false)
  )
)

(defn  gen-key [tbl , fld]
  (str (str/lower-case tbl) "." fld)
)

(defn  fld-default 
  ([^String key]
    (if (stru-map  key false) (stru-map key)
      ""
    )
  )
  ([tbl,fld]
    (let [key (gen-key tbl fld)]
      (fld-default  key)
    )
  )
)
  
(defn gen-fld-def
   " param map -> tblname , {:column_name \"COMBINED_ID\", :data_type \"NUMBER\", :data_length 22M} "
   [tblname {:keys [column_name,data_type,data_length,data_scale]}]
   (let [tbl (str/lower-case tblname ),fld_name (str/lower-case column_name) ,fld_type (str/lower-case data_type) , key (str tbl "." fld_name)]
		(case fld_type
			 "nchar"        {key ""}
			 "varchar"      {key ""}
			 "varchar2"     {key ""}
			 "char"         {key ""}
			 "long"         {key 0}
			 "clob"         {key ""}
			 "raw"          {key ""}
			 "blob"         (into {key "[]"} {(str tbl "-has-blob") true})
			 "date"         {key "1900-01-01T0:0:0.000Z"}
			 "number"       (if (> data_scale 0) {key 0.0}
			 									{key 0}
										  )
			(if (str/starts-with? fld_type "timestamp")
			 	{key "1900-01-01T0:0:0.000Z"}
				{key ""}
		  )
		)
  )
)

(defn gen-flds-def [tbl,column-defs]
	(loop [cdfs column-defs,clauses {}]
		(let [cdf (first cdfs), clause (gen-fld-def tbl cdf)]
			(if (next cdfs) (recur (rest cdfs) (into clauses clause) )
				(into clauses  clause)
		  )
		)
  )
)

(defn register-tbl 
  ([tname]
    (let [tbl (str/lower-case tname) ]
      (when (not (exists? tbl))
    	 (let [cdfs (dt/get-ora-structor (str/upper-case tname)) flds-def (gen-flds-def tbl cdfs)]
         (dosync 
           (alter stru-map into {tbl true})
           (alter stru-map into flds-def)
        )
    	 );; end of let
      );; end when 
      ) )
  ([tname,cdfs]
    (let [tbl (str/lower-case tname) ]
      (when (not (exists? tbl))
    	 (let [ flds-def (gen-flds-def tbl cdfs)]
         (dosync 
           (alter stru-map into {tbl true})
           (alter stru-map into flds-def)
        )
    	 );; end of let
      );;end when  
      ) )  
)

(defn table-has-blob? [tbl]
  (let [key (str (str/lower-case tbl) "-has-blob")]
  (if (contains? @stru-map key)
     (@stru-map key)
     (let [has? (dt/ora-table-has-blob? tbl)]
       (dosync (alter stru-map into {key has?}))
       has?
     )
  )
  ))


