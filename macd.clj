(ns macd
  (:use net.jeffhui.mongodb
        ga.ga
        strategyutils
        [pricehistory :only (price-close price-close-seq load-price-histories price-history-contains?)]
        [portfolio :only (buy-amap sell-all hold-shares?)]
        [schedule :only (soonest-scheduled-time reverse-scheduled-time-series weekday-hours n-periods-prior n-periods-prior-absolute-estimate)]
        [dke.contrib.dkeutils :only (run-as-script? multi-assoc-in multi-update-in tupleize rand-int-between form-to-string rand-nth-seq rand-int-offset moving-subseq)]
        [dke.contrib.datetime :only (datetime random-datetime date-pair interval-between expand-interval decrement-time increment-time)]
        [dke.contrib.math :only (millionth)]
        [dke.contrib.statistics :only (mean ema-alpha ema ema-r ema-update ema-update-obs)]
        [clojure.contrib.math :only (round)])
  (:import [org.joda.time Period]))

; NOTE: Read all about MACD at
; http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_average_conve
; http://en.wikipedia.org/wiki/MACD
; https://docs.google.com/Doc?docid=0Aalv7e5er7GuZDV6OWRicF8yNzRnbXNiaGRnZA&hl=en       (Dr. Rushton's MACD article)

; There are 2 main interpretations of the MACD strategy:
; 1. Signal Line Crossovers - "Signal line crossovers are the most common MACD signals."
;   - A bullish crossover occurs when MACD up-crosses the signal line.
;   - A bearish crossover occurs when MACD down-crosses the signal line.
; 2. Centerline Crossovers - "Centerline crossovers are the next most common MACD signals."
;   - A bullish centerline crossover occurs when MACD up-crosses the zero line.
;   - A bearish centerline crossover occurs when MACD down-crosses the zero line.

; This file implements the Signal Line Crossover interpretation.

; Computes MACD as described at http://en.wikipedia.org/wiki/MACD
; MACD = EMA[12] of price - EMA[26] of price
;
; In general:
; MACD[fast,slow] = EMA[fast] of price - EMA[slow] of price
;
; Returns a 3-element vector:
;   [macd fast-ema slow-ema]
(defn macd-oscillator
  ([prices fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha]
    (let [fast-ema (ema prices fast-ema-n fast-ema-alpha)
          slow-ema (ema prices slow-ema-n slow-ema-alpha)
          macd (- fast-ema slow-ema)]
      [macd fast-ema slow-ema]))
  ([prices fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha last-fast-ema last-slow-ema]
    (if (and last-fast-ema last-slow-ema)
      (let [fast-ema (ema-update prices
                                 fast-ema-alpha
                                 last-fast-ema)
            slow-ema (ema-update prices
                                 slow-ema-alpha
                                 last-slow-ema)
            macd (- fast-ema slow-ema)]
        [macd fast-ema slow-ema])
      (macd-oscillator prices fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha))))

; Returns a sequence of MACD-oscillator values. All but the oldest EMAs are computed using the ema-update method.
; Arguments:
; N - the number of MACD-oscillator values to return in the sequence. N > 0.
; The rest of the arguments are the same arguments given to the function macd-oscillator.
; IMPORTANT NOTE: The length of prices MUST be >= n + maximum(fast-ema-n, slow-ema-n) - 1
;
; Returns a sequence of triples of the form:
;   [ [macd1 fast-ema1 slow-ema1] [macd2 fast-ema2 slow-ema2] [macd3 fast-ema3 slow-ema3] ... [macdN fast-emaN slow-emaN]]
; where the first triple is the macd/fast-ema/slow-ema computed over the most recent prices
; and the last triple is the macd/fast-ema/slow-ema computed over the oldest prices
(defn macd-oscillator-seq [n prices fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha]
  (let [historical-prices (reverse (take n (moving-subseq prices)))        ; ex: (reverse (take 5 (moving-subseq [1 2 3 4 5 6]))) -> ([5 6] (4 5 6) (3 4 5 6) (2 3 4 5 6) (1 2 3 4 5 6))
        macd-v (macd-oscillator (first historical-prices) fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha)]
    (loop [historical-prices (rest historical-prices)
           last-fast-ema (macd-v 1)
           last-slow-ema (macd-v 2)
           macd-vec-seq [macd-v]]
      (if (= (count historical-prices) 0)
        (reverse macd-vec-seq)
        (let [prices (first historical-prices)
              macd-v (macd-oscillator prices fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha last-fast-ema last-slow-ema)]
          (recur (rest historical-prices)
                 (macd-v 1)
                 (macd-v 2)
                 (conj macd-vec-seq macd-v)))))))

; Computes MACD signal as described at http://en.wikipedia.org/wiki/MACD
; signal = EMA[9] of MACD
;
; In general:
; signal = EMA[n] of MACD[fast,slow]
(defn macd-signal
  ([prices macd-ema-n macd-ema-alpha fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha]
    ; (try
    (let [macd-seq (map first (macd-oscillator-seq macd-ema-n prices fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha))     ; the macd-seq has to have a length >= macd-ema-n
          signal-ema (ema macd-seq macd-ema-n macd-ema-alpha)]
      signal-ema))
      ; (catch Exception _
      ;   (println macd-ema-n (max fast-ema-n slow-ema-n) (+ macd-ema-n (max fast-ema-n slow-ema-n)))
      ;   (println (take 600 prices)))))
  ([prices macd-ema-n macd-ema-alpha fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha current-macd last-macd-ema]
    (if (and current-macd last-macd-ema)
      (ema-update-obs current-macd macd-ema-alpha last-macd-ema)
      (macd-signal prices macd-ema-n macd-ema-alpha fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha))))

; The 3 functions macd-next-state, macd-init-state, and generate-macd-parameter-set are
; used to run an experiment consisting of a number of trial-sets, with each trial-set consisting of
; a set of trials run against a fixed parameter set and ticker-set.
(defn macd-next-state [eparams sparams tsparams tparams constant-state current-state]
  (let [current-time (current-state :current-time)

        price-history-map (tparams-price-history-map tparams)
        ticker (first (tsparams-ticker-set tsparams))
        time-increment (sparams-time-increment sparams)
        weekly-trading-schedule (sparams-trading-schedule sparams)

        n-period-duration (sparams :n-period-duration)
        macd-ema-n (sparams :macd-ema-n)
        fast-ema-n (sparams :fast-ema-n)
        slow-ema-n (sparams :slow-ema-n)

        macd-ema-alpha (sparams :macd-ema-alpha)
        fast-ema-alpha (sparams :fast-ema-alpha)
        slow-ema-alpha (sparams :slow-ema-alpha)

        ; NOTE: prices line holds a reference to the head of a lazy sequence!
        prices (price-close-seq ticker current-time n-period-duration price-history-map weekly-trading-schedule)

        ; by using the last (previous) EMA in the computation of the current EMA, run-time went from 18 seconds to 1.6 seconds.
        ; IMPORTANT NOTE: Using the previous EMA in the computation of the current EMA is technically incorrect
        ;                 when the n-period-duration is different from time-increment.
        ;                 It is only technically correct when n-period-duration == time-increment.
        ;                 Since we compute variable "prices" with (price-close-seq ... current-time n-period-duration ...),
        ;                 if we increment current-time by an amount other than n-period-duration, then the price history sequence
        ;                 "prices" at time t will not be a continuation of the price history sequence "prices" at time t-1. That is
        ;                 where the problem arises.
        ; Since n-period-duration (1 day) == time-increment (1 day), my EMAs should be correct. So, for my thesis, I'm abiding by the
        ; rule that n-period-duration == time-increment, so using the last EMA in the computation of the current EMA shouldn't cause
        ; me any problems.
        last-fast-ema ((current-state :last-fast-emas) ticker)
        last-slow-ema ((current-state :last-slow-emas) ticker)
        last-macd-ema ((current-state :last-macd-emas) ticker)

        ; macd (macd-oscillator prices fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha)
        macd-v (macd-oscillator prices fast-ema-n slow-ema-n fast-ema-alpha slow-ema-alpha last-fast-ema last-slow-ema)
        macd (macd-v 0)
        fast-ema (macd-v 1)
        slow-ema (macd-v 2)

        last-macd (or ((current-state :last-macds) ticker) macd)
        ; signal 0
        signal (macd-signal prices
                            macd-ema-n
                            macd-ema-alpha
                            fast-ema-n
                            slow-ema-n
                            fast-ema-alpha
                            slow-ema-alpha
                            macd
                            last-macd-ema)      ; the signal is an EMA of MACD values.
        ]
    (multi-assoc-in
      (merge current-state
        (cond                                           ; The following conditions are mutually exclusive; so we use (cond ...)
          (upcross? signal last-macd macd)              ; MACD up-crossed the signal line, so BUY shares!
            (buy-amap current-state (sparams-commission sparams) ticker current-time price-history-map)
          (downcross? signal last-macd macd)            ; MACD down-crossed the signal line, so SELL all shares!
            (sell-all current-state (sparams-commission sparams) ticker current-time price-history-map))
        {:current-time (soonest-scheduled-time (increment-time current-time time-increment) weekly-trading-schedule)})
      [[:last-macds ticker] macd
       [:last-fast-emas ticker] fast-ema
       [:last-slow-emas ticker] slow-ema
       [:last-macd-emas ticker] signal
       ])))

(defn macd-init-state [eparams sparams tsparams tparams constant-state]
  (hash-map :current-time (constant-state :trading-period-start)
            :portfolio {
              :cash (sparams :principal)
              :securities (hash-map)
            }
            :transactions []
            :last-macds (hash-map)
            :last-fast-emas (hash-map)
            :last-slow-emas (hash-map)
            :last-macd-emas (hash-map)
            ))

(defn generate-macd-parameter-set [commission
                                   principal
                                   trading-period-length
                                   time-increment
                                   trading-schedule
                                   n-period-duration
                                   macd-ema-n
                                   fast-ema-n
                                   slow-ema-n]
  (struct-map strategy-params :commission commission
                              :principal principal
                              :trading-period-length trading-period-length
                              :time-increment time-increment
                              :trading-schedule trading-schedule

                              :build-constant-state default-constant-state
                              :build-init-state macd-init-state
                              :is-final-state? trading-period-expired?
                              :build-next-state macd-next-state
                              :build-return-state pv-final-state

                              :n-period-duration n-period-duration
                              :macd-ema-n macd-ema-n
                              :fast-ema-n fast-ema-n
                              :slow-ema-n slow-ema-n

                              ; by pre-computing alpha and reusing it throughout the simulation instead of
                              ; computing alpha every time (ema ...) was called, run-time went from 300 seconds to 
                              ; 18 seconds!!
                              :macd-ema-alpha (ema-alpha macd-ema-n (- 1 millionth))
                              :fast-ema-alpha (ema-alpha fast-ema-n (- 1 millionth))
                              :slow-ema-alpha (ema-alpha slow-ema-n (- 1 millionth))))


; Simple experiment
(defn run-macd-experiment [weekly-trading-hours]
  (let [trading-period-length (Period/years 1)
        start-dt (datetime "19800101120000")
        end-dt (datetime "20110401120000")
        fast-ema-n 12
        slow-ema-n 26
        macd-ema-n 9
        n-period-duration (Period/days 1)
        ; adjusted-start-dt (nth (reverse-scheduled-time-series start-dt n-period-duration weekly-trading-hours)
        ;                        (max macd-ema-n fast-ema-n slow-ema-n))
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

          ; We need (macd-ema-n + maximum(fast-ema-n, slow-ema-n) - 1) days worth of prior price history.
          ; See the note on the macd-oscillator-seq function. The note explains why.
          :prior-price-history (fn [eparams sparams tsparams]
                                 (.plus #^Period (n-periods-prior-absolute-estimate (+ (sparams :macd-ema-n)
                                                                                       (max (sparams :fast-ema-n)
                                                                                            (sparams :slow-ema-n)))
                                                                                    (sparams :n-period-duration)
                                                                                    start-dt
                                                                                    weekly-trading-hours)
                                        (Period/months 1)))
          :process-final-states trial-set-stats
        )
        sparams (generate-macd-parameter-set 7.00
                                             10000
                                             trading-period-length
                                             (Period/days 1)
                                             weekly-trading-hours
                                             n-period-duration
                                             macd-ema-n
                                             fast-ema-n
                                             slow-ema-n)
        trial-set-result-pairs (run-experiment eparams sparams)]
    (println "Start" (datetime))
    ; (print-results trial-set-result-pairs)
    (println (experiment-stats trial-set-result-pairs))
    (println "End" (datetime))))
    ; (insert-docs trial-set-result-pairs)))

; GA experiment
(defn macd-rand-individual [eparams
                            commission
                            principal
                            trading-period-length
                            time-increment
                            trading-schedule
                            n-period-duration
                            max-macd-ema-n
                            max-fast-ema-n
                            max-slow-ema-n]
  (let [macd-ema-n (rand-int-between 1 max-macd-ema-n)
        fast-ema-n (rand-int-between 1 max-fast-ema-n)
        slow-ema-n (rand-int-between 1 max-slow-ema-n)]
    [eparams (generate-macd-parameter-set commission
                                          principal
                                          trading-period-length
                                          time-increment
                                          trading-schedule
                                          n-period-duration
                                          macd-ema-n
                                          fast-ema-n
                                          slow-ema-n)]))

(defn macd-fitness [params-pair]
  (let [ticker-set-trial-set-stat-pairs (apply run-experiment params-pair)
        exp-stats (experiment-stats ticker-set-trial-set-stat-pairs)
        fitness (exp-stats :mean)
        adj-params-pair (persistable-params params-pair)
        ]
    (insert-docs (map (fn [ts-tss-pair]
                         {:params adj-params-pair :trialset-stats ts-tss-pair})
                       ticker-set-trial-set-stat-pairs))
    (insert-docs {:params adj-params-pair :exp-stats exp-stats})
    fitness))

; this function is supposed to return a sequence of individuals
(defn macd-crossover [parent-param-pairs]
  (let [p1 (first parent-param-pairs)
        p2 (second parent-param-pairs)
        sparams1 (second p1)
        sparams2 (second p2)
        child [(first p1) (generate-macd-parameter-set (sparams-commission sparams1)
                                                       (sparams-principal sparams1)
                                                       (sparams-trading-period-length sparams1)
                                                       (sparams-time-increment sparams1)
                                                       (sparams-trading-schedule sparams1)
                                                       (sparams1 :n-period-duration)
                                                       (round (mean [(sparams1 :macd-ema-n) (sparams2 :macd-ema-n)]))
                                                       (round (mean [(sparams1 :fast-ema-n) (sparams2 :fast-ema-n)]))
                                                       (round (mean [(sparams1 :slow-ema-n) (sparams2 :slow-ema-n)])))]]
    [child]))

; this function is supposed to takes an individual and return a mutated individual.
(defn macd-mutate [[eparams sparams]]
  (let [mutant [eparams (generate-macd-parameter-set (sparams-commission sparams)
                                                     (sparams-principal sparams)
                                                     (sparams-trading-period-length sparams)
                                                     (sparams-time-increment sparams)
                                                     (sparams-trading-schedule sparams)
                                                     (sparams :n-period-duration)
                                                     (rand-int-offset (sparams :macd-ema-n) -5 5 1)
                                                     (rand-int-offset (sparams :fast-ema-n) -5 5 1)
                                                     (rand-int-offset (sparams :slow-ema-n) -5 5 1))]]
    mutant))

(defn macd-ga [weekly-trading-hours]
  (let [trading-period-length (Period/years 1)
        start-dt (datetime "19800101120000")
        end-dt (datetime "20110401120000")
        max-macd-ema-n 501
        max-fast-ema-n 501
        max-slow-ema-n 501
        n-period-duration (Period/days 1)
        ; adjusted-start-dt (nth (reverse-scheduled-time-series start-dt n-period-duration weekly-trading-hours)
        ;                        (max max-macd-ema-n max-fast-ema-n max-slow-ema-n))
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

          ; We need (macd-ema-n + maximum(fast-ema-n, slow-ema-n) - 1) days worth of prior price history.
          ; See the note on the macd-oscillator-seq function. The note explains why.
          :prior-price-history (fn [eparams sparams tsparams]
                                 (.plus #^Period (n-periods-prior-absolute-estimate (+ (sparams :macd-ema-n)
                                                                                       (max (sparams :fast-ema-n)
                                                                                            (sparams :slow-ema-n)))
                                                                                    (sparams :n-period-duration)
                                                                                    start-dt
                                                                                    weekly-trading-hours)
                                        (Period/months 1)))
          :process-final-states trial-set-stats
        )

        generations 15
        gen-rand-ind #(macd-rand-individual eparams
                                            7.00
                                            10000
                                            trading-period-length
                                            (Period/days 1)
                                            weekly-trading-hours
                                            n-period-duration
                                            max-macd-ema-n
                                            max-fast-ema-n
                                            max-slow-ema-n)

        func-map {:init-fn (partial build-rand-pop gen-rand-ind)
                  :fit-fn macd-fitness
                  :sel-fn roulette-select
                  :cross-fn macd-crossover
                  :mut-fn macd-mutate
                  :terminate? (fn [individuals fitness-vals current-generation] (= current-generation generations))}
        set-map {:pop-size 100
                 :crossover-rate 75
                 :mutation-rate 2}]
    (println "Start" (datetime))
    (run-ga func-map set-map)
    (println "End" (datetime))))


(defn main []
  (run-macd-experiment (weekday-hours))
  ; (with-mongo ["localhost" 27017] "dke"
  ;   (with-collection "ga-macd-signalline-norgate-june18"
  ;     (macd-ga (weekday-hours))))
  )

; Only run the application automatically if run as a script, not if loaded in a REPL with load-file.
(if (run-as-script?) (main))