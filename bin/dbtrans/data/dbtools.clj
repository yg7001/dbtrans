(ns dbtrans.data.dbtools
	(:require [clojure.java.jdbc :as db] 
	 [jdbc.pool.c3p0 :as pool]
	 [clojure.string :as str]
   [dbtrans.config :as cfg]
	)
)


;(def db_pg_user (System/getenv "PG_USER"))
;(def db_pg_password (System/getenv "PG_PASSWORD"))

; business Tables in ora
(def sql-business "select table_name from user_tables where exists (select * from user_tab_cols where table_name=user_tables.table_name and column_name='OCCUR_TIME' and data_type='DATE') order by table_name")
; dictionary tables in ora

(def sql-dict "select table_name from user_tables where table_name in (select upper(table_name_eng) from sys_table_info   where table_type <>1 ) and not exists (select * from user_tab_cols where  column_name='OCCUR_TIME' and table_name=user_tables.table_name)")

(def sql-all "select table_name from user_tables")

;(def pg  {  :dbtype "postgresql"    :dbname "epbd" :host "ep-bd05"   :user "yh"   :password "yh"           :ssl false  :sslfactory "org.postgresql.ssl.NonValidatingFactory"})
(def pg4pool {  :classname  "org.postgresql.Driver"		:subprotocol "postgresql"	
     :subname (str "//" (cfg/pg-host) (when (cfg/pg-port) (str ":" (cfg/pg-port)))  "/" (cfg/pg-db))
     :user (cfg/pg-user)	:password (cfg/pg-pass)   
     ;:subname "//192.168.0.140:5432/epbd"	:user "yh"	:password "yh2016yh"
		 :initial-pool-size  5	,:max-pool-size 100 , :max-wait 30000 ,:max-connection-idle-lifetime 6000000
     :debug? false
})


(def ora  {:subprotocol "oracle"
     ;; :classname   "oracle.jdbc.OracleDriver"
		 ;;:subname (str "thin:@" (cfg/ora-host)  (when (cfg/ora-port) (str ":" (cfg/ora-port)))  "/" (cfg/ora-db)) 
     ;; above 2 lines' form are  for DRIVER-> org.clojars.zentrope/ojdbc
     :classname "oracle.jdbc.driver.OracleDriver"
     ;; Oracle 要用下面的形式，因为 c3p0 使用 SERVICE NAME 连接而不是SID ！！！
     :subname (str "thin:@//" (cfg/ora-host)  (when (cfg/ora-port) (str ":" (cfg/ora-port)))  "/" (cfg/ora-db))
     :user  (cfg/ora-user)		:password  (cfg/ora-pass)
		 :initial-pool-size  5  ,:max-pool-size 100 ,:max-wait 30000 ,:max-connection-idle-lifetime 6000000
     :debug? false
})
;; must set as-arrays? false  to use lazy seq
(def query-opt {:as-arrays? false :keywordize? false})
(def pg-insert-opt {:return-keys []} )

;; Convert the standard dbspec to an other dbspec with `:datasource` key
(def pg-pool (pool/make-datasource-spec pg4pool))
(def ora-pool (pool/make-datasource-spec ora))

(def example-tables ["COMBINED_DEVICE","YX_BW","OTHER_EVENT_WARN"])

;(def  rspg (db/query pg-pool   ["select * from combined_device limit 10"]  {:as-arrays? true}))
;(def rsora (db/query ora-pool   ["select * from combined_device where rownum<11"]  {:as-arrays? true}))

(defn get-ora-tables-sql	[^String sql]
	(let [ods (take 10000 (db/query ora-pool [sql] {:as-arrays true}) )]
		(loop [ds ods,tables []]
			(let [tdf (first ds), tname (tdf :table_name)]
				(if (next ds) (recur (rest ds) (conj tables tname))
					(conj tables tname)
				)
			)
		)
	)
)
(defn  prepare-statement [dbcon sql]
  (db/prepare-statement (.get-conn dbcon) sql {:fetch-size 1000})
 )
(defn get-ora-all-tables
	" get all user tables from Oracle"
	[](get-ora-tables-sql sql-all)
)

(defn get-ora-dict-tables[]
	(get-ora-tables-sql sql-dict)
)

(defn get-ora-business-tables[]
	(get-ora-tables-sql sql-business)
)

(defn query-ora
	([^String sql]
    (db/with-db-connection [dbcon  ora-pool]
      ;(db/query dbcon  [(prepare-statement dbcon sql)] query-opt)
      (db/query dbcon [sql] query-opt)
    )
	)
  ([^String sql ^clojure.lang.PersistentArrayMap opt]
    (db/with-db-connection [dbcon  ora-pool]
      ;(db/query dbcon  [(prepare-statement dbcon sql)] (merge query-opt opt))
      (db/query dbcon [sql]  (merge query-opt opt))
    )
	)
	([^String sql ^clojure.lang.PersistentVector params ^clojure.lang.PersistentArrayMap opt ]
		(let [wh (vec params)]						
      (db/with-db-connection [dbcon ora-pool]
        ;(db/query dbcon (vec (cons (prepare-statement dbcon sql) wh)) query-opt)
        (db/query dbcon (vec (cons sql wh)) (merge query-opt opt))
       )			
		)
	)
)
(defn query-pg
  ([^String sql]
    (db/with-db-connection [dbcon pg-pool]
      ;(db/query dbcon [(prepare-statement dbcon sql)] query-opt)
      (db/query dbcon [sql] query-opt)
     )
	)
  ([^String sql ^clojure.lang.PersistentArrayMap opt]
    (db/with-db-connection [dbcon  pg-pool]
      ;(db/query dbcon  [(prepare-statement dbcon sql)] (merge query-opt opt))
      (db/query dbcon [sql]  (merge query-opt opt))
    )
	)
	([^String sql param & params]
		(loop [wh [param] ps params]
			(if (> (count ps) 0) 
				(recur (conj wh (first ps)) (rest ps))
        (db/with-db-connection [dbcon pg-pool]
          ;(db/query dbcon (vec (cons (prepare-statement dbcon sql) wh)) query-opt)
          (db/query dbcon (vec (cons sql wh)) query-opt)
        )
			)
		)
	)
)

(defn reducible-query-pg
  "rfn must accept 2 params , and return count , like #(do (println %2)(Thread/sleep 0) (inc %) )"   
  [sql rfn]  
  (reduce (rfn)  0  (db/reducible-query pg-pool [sql] {:keywordize? false}))
)

(defn  insert-pg! 
    ([table row]
      (db/with-db-transaction [tcon pg-pool]
        (db/insert! tcon table row pg-insert-opt)
      )
    )
    ([table cols-or-row values-or-opts]
      (if (map? values-or-opts)
        (db/with-db-transaction [tcon pg-pool]
          (db/insert! tcon table cols-or-row  (merge values-or-opts pg-insert-opt)) )
        (db/with-db-transaction [tcon pg-pool]
          (db/insert! tcon table cols-or-row  values-or-opts pg-insert-opt))
      )
    )
    ([table cols values opts]
      (db/with-db-transaction [tcon pg-pool]
        (db/insert! tcon table cols values (merge opts pg-insert-opt ) ) )
    )  
)

(defn  insert-pg-multi!  
    ([table rows]
      (db/with-db-transaction [tcon pg-pool]
        (db/insert-multi! tcon table rows pg-insert-opt))
    )
    ([table cols-or-rows values-or-opts]
      (if (map? values-or-opts)
        (db/with-db-transaction [tcon pg-pool]
          (db/insert-multi! tcon table cols-or-rows  (merge values-or-opts pg-insert-opt)) )
        (db/with-db-transaction [tcon pg-pool]
          (db/insert-multi! tcon table cols-or-rows  values-or-opts pg-insert-opt) )
      )
    )
    ([table cols values opts]
      (db/with-db-transaction [tcon pg-pool]
        (db/insert-multi! tcon table cols values (merge opts pg-insert-opt ) ) )
    )  
)

(defn  execute-pg 
  ([^String sql]
    (db/with-db-transaction [tcon pg-pool]
      (db/execute! tcon [sql] query-opt) )
  )
  ([^String sql  param & params]
    (loop [wh [sql param] ps params]
			(if (> (count ps) 0) 
				(recur (conj wh (first ps)) (rest ps))			
        (db/with-db-transaction [tcon pg-pool]
          (db/execute! tcon wh  query-opt) )
			)
		)
  )
)

(defn get-pg-field-clause
" param map -> {:column_name \"COMBINED_ID\", :data_type \"NUMBER\", :data_length 22M} "
[{:keys [column_name,data_type,data_length,data_scale]}]
	;;(prn "colname=",column_name," type=",data_type,)
	(let [fld_name (str "\"",(str/lower-case column_name),"\"") fld_type (str/lower-case data_type)]
		(case fld_type
			 "nchar"        (str fld_name ," " ,"varchar(", (* data_length 2),") default ''" )
			 "varchar"      (str fld_name ," " ,"varchar(", data_length,") default ''" )
			 "varchar2"     (str fld_name ," " ,"varchar(", data_length,") default ''" )
			 "char"         (str fld_name ," " ,"varchar(", data_length,") default ''" )
			 "long"         (str fld_name ," " ,"int  default 0")
			 "clob"         (str fld_name ," " ,"text default '' ")
			 "raw"          (str fld_name ," " ,"text default '' ")
			 "blob"         (str fld_name ," " ,"bytea") ;;"bigint")  ;;should be [bytea]
			 "date"         (str fld_name," ","timestamp default '1900-01-01T0:0:0.000Z'")
			 "number"       (if (> data_scale 0) (str fld_name ," " ,"float  default 0.0")
			 									(str fld_name ," " ,"int  default 0")
										  )
			(if (str/starts-with? fld_type "timestamp")
			 	(str fld_name," ","timestamp")
				(str fld_name ," " ,"text")
		  )
		)
  )
)

(defn to-type-tbl-map
"param -> table_name&column defs , trans to uniique map in format
 {data_type1 table_name data_type2 table_name...} "
[tname,cdfs]
	;(prn "tname->" tname "cdfs->" cdfs)
	  (loop [cs cdfs, ts {}]
		(let [cdf (first cs),dt (get cdf :data_type)]
			;(prn "dt->" dt)
			  (if (next cs)
				(recur (rest cs) (clojure.set/union ts {dt tname}))
				(clojure.set/union ts {dt tname})
		  )
		)
  )
)

(defn get-pg-field-clauses[column-defs]
	(loop [cdfs column-defs,clauses []]
		(let [cdf (first cdfs), clause (get-pg-field-clause cdf)]
			(if (next cdfs) (recur (rest cdfs) (conj clauses clause) )
				(conj clauses  clause)
		  )
		)
  )
)

(defn get-unique-types
"get unique data types of fields of all tables, just for DEV stage, first table is the value
 param [{tbl ({fld defs}...)}{tbl ({fld defs}...)}...]"
[coldefs]
	(loop [ffs coldefs , uts {}]
		(let [sdefs (first ffs) ,tname (-> sdefs keys first) ,cdfs (-> sdefs vals first), ts (to-type-tbl-map tname cdfs)]
				;(prn sdefs)
				  ;(prn "tname->" tname)
			    ;(prn "cdf->" cdf)
				  (if (next ffs)
					(recur (next ffs)  (clojure.set/union uts ts))
					(clojure.set/union uts ts)
				 )
		)
	)
)


;param -> table name str
  (defn get-ora-structor[tbl]
	(db/query ora-pool ["select COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_SCALE from user_tab_cols where table_name=?",tbl] {:as-arrays true} )
)

;param vector -> ["table1","table2".....]
  (defn  get-coldefs[tbls]
	(loop [tables tbls,coldefs []]
		(let [tbl (first tables), tbl-defs (get-ora-structor tbl) ]
				(if (next tables) (recur (rest tables) (conj coldefs {tbl tbl-defs}))
					(conj coldefs {tbl tbl-defs})
				)
		)
	 )
)

;whether or not the table name exists in postgresql
(defn exists-pg-table? [tname]
	 (let [rs (db/query pg-pool ["select count(*) from information_schema.tables where table_schema='public' and table_name=?",tname] {:as-arrays true})]
		 (= 1 (-> rs first :count))
	 )
)

;param -> table name
(defn get-pg-ddl[tname]
	(let [cdfs (get-ora-structor (str/upper-case tname)) pgtname (str/lower-case tname) flds (get-pg-field-clauses cdfs) fldstr (str/join "," flds)]    
		(str "create table ",pgtname,"(",fldstr,")")
	)
)

;; test table has BLOB
(defn ora-table-has-blob? [tbl]
  (let [tblora (str/upper-case tbl) ] 
   (-> (query-ora "select count(*) from user_tab_cols where table_name=? and data_type='BLOB'" [tblora] {}) first first last (> 0) )
  )
)
;; value type oracle.sql.BLOB
(defn trans-ora-blob [rowmap]
  (loop [kvs rowmap, rst {}]
    (let [kv (first kvs),f (kv 0), v (kv 1) ,blob? (some-> v type .getSimpleName (= "BLOB"))]      
      (if (next kvs) 
        (if blob? (recur (rest kvs) (assoc rst f (.getBytes v)) )   (recur (rest kvs) (assoc rst f v)) )
        (if blob? (assoc rst f (.getBytes v))  (assoc rst f v))
      )
    )    
  )
)

;; just for debug test in repl
  ;(get-unique-types (get-coldefs example-tables))

  ;# get tables' structor
  ;(get-coldefs example-tables)

  ;(get-unique-types (get-coldefs (get-ora-tables ora)))
  ;(get-unique-types (get-coldefs example-tables))

  
