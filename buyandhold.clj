(ns buyandhold
  (:use net.jeffhui.mongodb
        strategyutils
        [pricehistory :only (price-close price-close-seq load-price-histories price-history-contains? enough-price-history?)]
        [portfolio :only (buy-amap sell-all hold-shares?)]
        [schedule :only (soonest-scheduled-time weekday-hours)]
        [dke.contrib.dkeutils :only (run-as-script? multi-assoc-in tupleize)]
        [dke.contrib.datetime :only (datetime random-datetime date-pair interval-between expand-interval increment-time)]
        [dke.contrib.statistics :only (sample-normal-distribution distribution-std-dev)])
  (:import [org.joda.time Period]))

(defn buy-and-hold-init-state [eparams sparams tsparams tparams constant-state]
  (hash-map :current-time (constant-state :trading-period-start)
            :portfolio {
              :cash (sparams :principal)
              :securities (hash-map)
            }
            :transactions []))

(defn buy-and-hold-next-state [eparams sparams tsparams tparams constant-state current-state]
  (let [current-time (current-state :current-time)
        start-time (constant-state :trading-period-start)
        price-history-map (tparams-price-history-map tparams)
        ticker (first (tsparams-ticker-set tsparams))
        time-increment (sparams-time-increment sparams)]
    (merge current-state
           (if (= current-time start-time)
             (buy-amap current-state (sparams-commission sparams) ticker current-time price-history-map))
           {:current-time (soonest-scheduled-time (increment-time current-time time-increment)
                                                  (sparams-trading-schedule sparams))})))

(defn generate-bah-parameter-set [commission principal trading-period-length time-increment trading-schedule]
  (struct-map strategy-params :commission commission
                              :principal principal
                              :trading-period-length trading-period-length
                              :time-increment time-increment
                              :trading-schedule trading-schedule

                              :build-constant-state default-constant-state
                              :build-init-state buy-and-hold-init-state
                              :is-final-state? trading-period-expired?
                              :build-next-state buy-and-hold-next-state
                              :build-return-state pv-final-state))

(defn trial-set-stats2 [state-sequence]
  (let [portfolio-values (map #(:portfolio-value %) state-sequence)
        distribution (sample-normal-distribution portfolio-values)
        std-dev (distribution-std-dev distribution)]
    (insert-docs state-sequence)
    (assoc distribution :std-dev std-dev)))

(defn run-bah-experiment [weekly-trading-hours]
  (let [trading-period-length (Period/years 1)
        start-dt (datetime "19800101120000")
        end-dt (datetime "20110401120000")
        ; adjusted-start-dt start-dt
        ; ph-date-span (interval-between adjusted-start-dt end-dt)
        filenames (nthnext *command-line-args* 1)
        ; ph-files (filter #(price-history-contains? % ph-date-span) filenames)
        ph-files (filter #(enough-price-history? % trading-period-length) filenames)
        ; ph-files filenames
        companies-tested-per-experiment 500
        eparams (struct-map experiment-params
          :ph-files ph-files
          :trial-count 100000          ; (* 20 companies-tested-per-experiment)
          :start-datetime start-dt
          :end-datetime end-dt
          :ticker-set-gen tupleize
          :distribute-trial-count randomly-distribute-trial-count
          :prior-price-history (fn [eparams sparams tsparams] (Period/months 1))    ; just to be safe
          :process-final-states trial-set-stats
        )
        sparams (generate-bah-parameter-set 7.00 10000 trading-period-length (Period/years 1) weekly-trading-hours)
        trial-set-result-pairs (run-experiment eparams sparams)]
    (println "Start" (datetime))
    ; (print-results trial-set-result-pairs)
    (println (experiment-stats trial-set-result-pairs))
    (println "End" (datetime))))
    ; (insert-docs trial-set-result-pairs)))

(defn main []
  ; (println (count (filter #(enough-price-history? % (Period/years 1)) (nthnext *command-line-args* 1))))
  ; (run-bah-experiment (weekday-hours))
  (with-mongo ["localhost" 27017] "dke"
    (with-collection "july3pvs"
      (run-bah-experiment (weekday-hours))))
  )

; Only run the application automatically if run as a script, not if loaded in a REPL with load-file.
(if (run-as-script?) (main))