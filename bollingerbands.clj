(ns bollingerbands
  (:use net.jeffhui.mongodb
        ga.ga
        strategyutils
        [averagebands :only (bands-init-state)]
        [pricehistory :only (price-close price-close-seq load-price-histories price-history-contains?)]
        [portfolio :only (buy-amap sell-all hold-shares?)]
        [schedule :only (soonest-scheduled-time reverse-scheduled-time-series weekday-hours n-periods-prior n-periods-prior-absolute-estimate)]
        [dke.contrib.dkeutils :only (run-as-script? multi-assoc-in multi-update-in tupleize rand-int-between form-to-string rand-nth-seq rand-real-between rand-int-offset)]
        [dke.contrib.datetime :only (datetime random-datetime date-pair interval-between expand-interval decrement-time increment-time)]
        [dke.contrib.statistics :only (mean population-std-dev sma sma-update)]
        [clojure.contrib.math :only (round)])
  (:import [org.joda.time Period]))

(defn bollinger-bands-next-state [eparams sparams tsparams tparams constant-state current-state]
  (let [current-time (current-state :current-time)

        price-history-map (tparams-price-history-map tparams)
        ticker (first (tsparams-ticker-set tsparams))
        time-increment (sparams-time-increment sparams)
        weekly-trading-schedule (sparams-trading-schedule sparams)

        n-periods (sparams :n-periods)
        n-period-duration (sparams :n-period-duration)
        k-multiplier (sparams :k-multiplier)

        price (price-close ticker current-time price-history-map)
        last-price (or ((current-state :last-prices) ticker) price)     ; if the last-price isn't available, use the current price

        last-sma ((current-state :last-smas) ticker)
        last-sma-obs ((current-state :last-sma-observations) ticker)

        n-period-prices (vec (take n-periods (price-close-seq ticker current-time n-period-duration price-history-map weekly-trading-schedule)))
        ; NOTE: n-period-prices line holds a reference to the head of a lazy sequence!

        ma (sma-update n-period-prices n-periods last-sma last-sma-obs)
        ; ma (mean n-period-prices)

        ; Bollinger Bands use the population method of calculating standard deviation,
        ; thus the proper divisor for the sigma calculation is n, not n − 1.
        std-dev (population-std-dev n-period-prices ma)
        k-std-dev (* k-multiplier std-dev)
        lower-band (- ma k-std-dev)
        upper-band (+ ma k-std-dev)]
    (multi-assoc-in
      (merge current-state
        (cond                                                     ; The following conditions are mutually exclusive; so we use (cond ...)
          (upcross? lower-band last-price price)                  ; we just upcrossed the low band, so BUY shares!
            (buy-amap current-state (sparams-commission sparams) ticker current-time price-history-map)
          (downcross? upper-band last-price price)                ; we just downcrossed the high band, so SELL shares!
            (sell-all current-state (sparams-commission sparams) ticker current-time price-history-map)
          (and (hold-shares? (current-state :portfolio) ticker)   ; we're holding shares AND
               (downcross? lower-band last-price price))          ; we just downcrossed the low band, so SELL shares!
            (sell-all current-state (sparams-commission sparams) ticker current-time price-history-map))
        {:current-time (soonest-scheduled-time (increment-time current-time time-increment) weekly-trading-schedule)})
      [[:last-prices ticker] price
       [:last-smas ticker] ma
       [:last-sma-observations ticker] n-period-prices])))

(defn generate-bollinger-bands-parameter-set [commission principal trading-period-length time-increment trading-schedule n-periods n-period-duration k-multiplier]
  (struct-map strategy-params :commission commission
                              :principal principal
                              :trading-period-length trading-period-length
                              :time-increment time-increment
                              :trading-schedule trading-schedule

                              :build-constant-state default-constant-state
                              :build-init-state bands-init-state
                              :is-final-state? trading-period-expired?
                              :build-next-state bollinger-bands-next-state
                              :build-return-state pv-final-state

                              :n-periods n-periods
                              :n-period-duration n-period-duration
                              :k-multiplier k-multiplier))

; Simple experiment
(defn run-bollinger-bands-experiment [weekly-trading-hours]
  (let [trading-period-length (Period/years 1)
        start-dt (datetime "19800101120000")
        end-dt (datetime "20110401120000")
        n-periods 20
        k-multiplier 2.0
        n-period-duration (Period/days 1)
        ; adjusted-start-dt (nth (reverse-scheduled-time-series start-dt n-period-duration weekly-trading-hours) n-periods)
        ; ph-date-span (interval-between adjusted-start-dt end-dt)
        filenames (nthnext *command-line-args* 1)
        ; ph-files (filter #(price-history-contains? % ph-date-span) filenames)
        ph-files filenames
        companies-tested-per-experiment 500
        eparams (struct-map experiment-params
          :ph-files ph-files
          :trial-count (* 20 companies-tested-per-experiment)    ; run 20 * 500 = 10,000 trials total
          :start-datetime start-dt
          :end-datetime end-dt
          :ticker-set-gen tupleize
          :distribute-trial-count randomly-distribute-trial-count
          ; :pick-ph-date-range (fn [eparams sparams tsparams] (date-pair (expand-interval ph-date-span :before (Period/months 1) nil nil)))
          :prior-price-history (fn [eparams sparams tsparams]
                                 (.plus #^Period (n-periods-prior-absolute-estimate (sparams :n-periods)
                                                                                    (sparams :n-period-duration)
                                                                                    start-dt
                                                                                    weekly-trading-hours)
                                        (Period/months 1)))
          :process-final-states trial-set-stats
        )

        sparams (generate-bollinger-bands-parameter-set 7.00 10000 trading-period-length (Period/days 1) weekly-trading-hours n-periods n-period-duration k-multiplier)
        trial-set-result-pairs (run-experiment eparams sparams)]
    (println "Start" (datetime))
    ; (print-results trial-set-result-pairs)
    (println (experiment-stats trial-set-result-pairs))
    ; (insert-docs trial-set-result-pairs)))
    (println "End" (datetime))))


; GA experiment
(defn bollinger-bands-rand-individual [eparams
                                       commission
                                       principal
                                       trading-period-length
                                       time-increment
                                       trading-schedule
                                       max-n-periods
                                       n-period-duration
                                       min-k-multiplier
                                       max-k-multiplier]
  (let [n-periods (rand-int-between 1 max-n-periods)
        k-multiplier (rand-real-between min-k-multiplier max-k-multiplier)]
    [eparams (generate-bollinger-bands-parameter-set commission
                                                     principal
                                                     trading-period-length
                                                     time-increment
                                                     trading-schedule
                                                     n-periods
                                                     n-period-duration
                                                     k-multiplier)]))

(defn bollinger-bands-fitness [params-pair]
  (let [ticker-set-trial-set-stat-pairs (apply run-experiment params-pair)
        exp-stats (experiment-stats ticker-set-trial-set-stat-pairs)
        fitness (exp-stats :mean)
        adj-params-pair (persistable-params params-pair)]
    (insert-docs (map (fn [ts-tss-pair]
                         {:params adj-params-pair :trialset-stats ts-tss-pair})
                       ticker-set-trial-set-stat-pairs))
    (insert-docs {:params adj-params-pair :exp-stats exp-stats})
    fitness))

; this function is supposed to return a sequence of individuals
(defn bollinger-bands-crossover [parent-param-pairs]
  (let [p1 (first parent-param-pairs)
        p2 (second parent-param-pairs)
        sparams1 (second p1)
        sparams2 (second p2)
        child [(first p1) (generate-bollinger-bands-parameter-set (sparams-commission sparams1)
                                                                  (sparams-principal sparams1)
                                                                  (sparams-trading-period-length sparams1)
                                                                  (sparams-time-increment sparams1)
                                                                  (sparams-trading-schedule sparams1)
                                                                  (round (mean [(sparams1 :n-periods) (sparams2 :n-periods)]))
                                                                  (sparams1 :n-period-duration)
                                                                  (mean [(sparams1 :k-multiplier) (sparams2 :k-multiplier)]))]]
    [child]))

; this function is supposed to takes an individual and return a mutated individual.
(defn bollinger-bands-mutate [[eparams sparams]]
  (let [mutant [eparams (generate-bollinger-bands-parameter-set (sparams-commission sparams)
                                                                (sparams-principal sparams)
                                                                (sparams-trading-period-length sparams)
                                                                (sparams-time-increment sparams)
                                                                (sparams-trading-schedule sparams)
                                                                (rand-int-offset (sparams :n-periods) -10 10 1)
                                                                (sparams :n-period-duration)
                                                                (* (sparams :k-multiplier)
                                                                   (rand-real-between 0.9 1.1)))]]
    mutant))


(defn bollinger-bands-ga [weekly-trading-hours]
  (let [trading-period-length (Period/years 1)
        start-dt (datetime "19800101120000")
        end-dt (datetime "20110401120000")
        max-n-periods 501
        min-k-multiplier 0.05
        max-k-multiplier 3.0
        n-period-duration (Period/days 1)
        ; adjusted-start-dt (nth (reverse-scheduled-time-series start-dt n-period-duration weekly-trading-hours) max-n-periods)
        ; ph-date-span (interval-between adjusted-start-dt end-dt)
        filenames (nthnext *command-line-args* 1)
        ; ph-files (filter #(price-history-contains? % ph-date-span) filenames)
        ph-files filenames
        companies-tested-per-experiment 500
        eparams (struct-map experiment-params
          :ph-files ph-files
          :trial-count 100                      ;(* 20 companies-tested-per-experiment)
          :start-datetime start-dt
          :end-datetime end-dt
          :ticker-set-gen tupleize              ;#(n-ph-files-per-set companies-tested-per-experiment %)
          :distribute-trial-count randomly-distribute-trial-count
          :prior-price-history (fn [eparams sparams tsparams]
                                 (.plus #^Period (n-periods-prior-absolute-estimate (sparams :n-periods)
                                                                                    (sparams :n-period-duration)
                                                                                    start-dt
                                                                                    weekly-trading-hours)
                                        (Period/months 1)))
          ; :pick-ph-date-range (fn [eparams sparams tsparams] (date-pair (expand-interval ph-date-span :before (Period/months 1) nil nil)))
          :process-final-states trial-set-stats
        )

        generations 15
        gen-rand-ind #(bollinger-bands-rand-individual eparams
                                                       7.00
                                                       10000
                                                       trading-period-length
                                                       (Period/days 1)
                                                       weekly-trading-hours
                                                       max-n-periods
                                                       n-period-duration
                                                       min-k-multiplier
                                                       max-k-multiplier)

        func-map {:init-fn (partial build-rand-pop gen-rand-ind)
                  :fit-fn bollinger-bands-fitness
                  :sel-fn roulette-select
                  :cross-fn bollinger-bands-crossover
                  :mut-fn bollinger-bands-mutate
                  :terminate? (fn [individuals fitness-vals current-generation] (= current-generation generations))}
        set-map {:pop-size 100
                 :crossover-rate 75
                 :mutation-rate 2}]
    (println "Start" (datetime))
    (run-ga func-map set-map)
    (println "End" (datetime))))

(defn main []
  (run-bollinger-bands-experiment (weekday-hours))
  ; (with-mongo ["localhost" 27017] "dke"
  ;   (with-collection "ga-bollinger-bands-norgate-june15"
  ;     (bollinger-bands-ga (weekday-hours))))
  )

; Only run the application automatically if run as a script, not if loaded in a REPL with load-file.
(if (run-as-script?) (main))