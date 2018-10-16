(defproject dbtrans "0.1.0-SNAPSHOT"
  :description "ETL for Epcos oracle data to HAWQ"
  :url "http://yinghuan.com/product/software/dbtrans"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
		 [org.clojure/java.jdbc "0.7.8"]
		 [org.postgresql/postgresql "42.2.5"]
		 ;[org.clojars.zentrope/ojdbc "11.2.0.3.0"]
		 [mysql/mysql-connector-java "5.1.11"]		 
		 [org.clojure/data.json "0.2.6"]
		 [clj-time "0.14.4"]
     [com.mchange/c3p0 "0.9.5.2"]
     [org.clojure/tools.cli "0.4.1"]
     [org.clojure/tools.logging "0.4.1"]
     [ch.qos.logback/logback-classic "1.2.3"]
		]
  :resource-paths ["resources/ojdbc7.jar"]
  :dev-dependencies [    
       
  ]
  :plugins [
  ;          [lein-idefiles "0.2.1"]
   ]
  :main dbtrans.main
  :repositories [
          ["yh" "http://192.168.0.110:8081/repository/maven-public/"]
          ;;clojars 中科大镜像
          ["zkd" "http://mirrors.ustc.edu.cn/clojars/"]
          ["ali" "http: //maven.aliyun.com/nexus/content/groups/public"]
          ["clojars" "https://mirrors.tuna.tsinghua.edu.cn/clojars/"]
	]  
)
;; 这个是官方的启用非严格检查方案,DO NOT put in (defproject) 是关闭高版本lein的严格仓库检查。
(require 'cemerick.pomegranate.aether)
(cemerick.pomegranate.aether/register-wagon-factory! "http" #(org.apache.maven.wagon.providers.http.HttpWagon.))
