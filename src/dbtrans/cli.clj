(ns dbtrans.cli
  (:require  [clojure.string :as str] [clojure.tools.logging :as log]
             [clojure.tools.cli :as cli]
  )
)

(def cli-options
  ;; An option with a required argument
  [["-l" "--limit n" "limit number for all"
    :default 0
    :parse-fn #(Integer/parseInt %)
    :validate [#(< -1 % 10000) "Must be a number between 0 and 10000"]]   
   ["-d" "--dlimit n" "limit number for dict tables , override limit option"        
    
    :parse-fn #(Integer/parseInt %)
    ]
   ["-b" "--blimit n" "limit number for business tables , override limit option"        
    
    :parse-fn #(Integer/parseInt %)
    ]
   ;; A boolean option defaulting to nil
   ["-h" "--help"]])


(defn usage [options-summary]
  (->> ["\n#######################################"
        ""
        "Usage: dbtrans [options] action"
        ""
        "Options:"
        options-summary
        ]
       (str/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

(defn validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with a error message, and optional ok status), or a map  "
  [args]
  (let [{:keys [options arguments errors summary]} (cli/parse-opts args cli-options)]
    (cond
      (:help options) ; help => exit OK with usage summary
      {:exit-message (usage summary) :ok? true}
      errors ; errors => exit with description of errors
      {:exit-message (error-msg errors)}
      ;; custom validation on arguments
      ;(and (= 1 (count arguments))    (#{"start" "stop" "status"} (first arguments)))
      ;{:action (first arguments) :options options}
      ;:else ; failed custom validation => exit with usage summary
      ;{:exit-message (usage summary)}
      :else   {:options options}
    )
))


