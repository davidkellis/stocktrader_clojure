(ns strategyutils
  (:use net.jeffhui.mongodb
        clojure.contrib.json
        [portfolio :only (portfolio-value)]
        [pricehistory :only (files-with-enough-price-history load-price-histories price-history-cdr)]
        [dke.contrib.datetime :only (before? after? random-datetime min-datetime max-datetime date-pair expand-interval interval-between)]
        [dke.contrib.dkeutils :only (multi-assoc-in tupleize rand-nth-seq multi-update-in form-to-string fname)]
        [dke.contrib.statistics :only (mean sample-std-dev summation sample-normal-distribution combine-sample-normal-distributions distribution-std-dev)])
  (:import [org.joda.time DateTime Period Duration]))


; A struct can do lookups faster than a hash-map, especially with an accessor function for specific fields.
(defstruct experiment-params :ph-files
                             :trial-count
                             :start-datetime
                             :end-datetime
                             :ticker-set-gen
                             :distribute-trial-count
                             ; :pick-ph-date-range
                             :prior-price-history
                             :process-final-states)

(defstruct strategy-params :commission
                           :principal
                           :trading-period-length
                           :time-increment
                           :trading-schedule
                           :build-constant-state
                           :build-init-state
                           :is-final-state?
                           :build-next-state
                           :build-return-state)

; build accessors for some of the most used fields
(def sparams-commission (accessor strategy-params :commission))
(def sparams-principal (accessor strategy-params :principal))
(def sparams-trading-period-length (accessor strategy-params :trading-period-length))
(def sparams-time-increment (accessor strategy-params :time-increment))
(def sparams-trading-schedule (accessor strategy-params :trading-schedule))
(def sparams-build-constant-state (accessor strategy-params :build-constant-state))
(def sparams-build-init-state (accessor strategy-params :build-init-state))
(def sparams-is-final-state? (accessor strategy-params :is-final-state?))
(def sparams-build-next-state (accessor strategy-params :build-next-state))
(def sparams-build-return-state (accessor strategy-params :build-return-state))

(defstruct trial-set-params :ticker-set
                            :trial-count)

(def tsparams-ticker-set (accessor trial-set-params :ticker-set))
(def tsparams-trial-count (accessor trial-set-params :trial-count))

(defstruct trial-params :price-history-map
                        :earliest-trading-start
                        :latest-trading-start)

; build accessors for some of the most used fields
(def tparams-price-history-map (accessor trial-params :price-history-map))
(def tparams-earliest-trading-start (accessor trial-params :earliest-trading-start))
(def tparams-latest-trading-start (accessor trial-params :latest-trading-start))


(defn default-constant-state [eparams
                              sparams
                              tsparams
                              tparams
                              trading-period-start
                              trading-period-end]
  (hash-map :trading-period-start trading-period-start
            :trading-period-end trading-period-end
            :commission (sparams-commission sparams)
            :time-increment (sparams-time-increment sparams)
            :trading-schedule (sparams-trading-schedule sparams)
            :ticker-set (tsparams :ticker-set)))

(defn default-final-state [eparams sparams tsparams tparams constant-state current-state]
  (merge current-state {
    :portfolio-value (portfolio-value (current-state :portfolio)
                                      (current-state :current-time)
                                      (tparams-price-history-map tparams))
  }))

; returns a final state consisting of only the portfolio value at the end of the trading period
(defn pv-final-state [eparams sparams tsparams tparams constant-state current-state]
  {
    :portfolio-value (portfolio-value (current-state :portfolio)
                                      (current-state :current-time)
                                      (tparams-price-history-map tparams))
  })

(defn identity-final-state [eparams sparams tsparams tparams constant-state current-state]
  current-state)

; pair-sequence is a lazy sequence of vector pairs where each key/value pair is a ticker-set/trial-set-stats
(defn experiment-stats [pair-sequence]
  (let [trial-set-stats-seq (map second pair-sequence)
        distribution (combine-sample-normal-distributions trial-set-stats-seq)
        std-dev (distribution-std-dev distribution)]
    (assoc distribution :std-dev std-dev)))

(defn trial-set-stats [state-sequence]
  (let [portfolio-values (map #(:portfolio-value %) state-sequence)
        distribution (sample-normal-distribution portfolio-values)
        std-dev (distribution-std-dev distribution)]
    (assoc distribution :std-dev std-dev)))

(defn trading-period-expired? [eparams sparams tsparams tparams constant-state current-state]
  (not (before? (current-state :current-time) (constant-state :trading-period-end))))

; returns true if last-value <= reference-value < curr-value
(defn upcross? [reference-value last-value curr-value]
  (and (<= last-value reference-value)
       (> curr-value reference-value)))

; returns true if last-value >= reference-value > curr-value
(defn downcross? [reference-value last-value curr-value]
  (and (>= last-value reference-value)
       (< curr-value reference-value)))


(defn jsondoc [obj]
  {:jsonobj (json-str obj)})

(defn print-results [s]
  (doseq [result s]
    (println result)))

(defn persistable-params [[eparams sparams]]
  [(multi-update-in eparams
                    [[:ticker-set-gen] #(fname %)
                     [:distribute-trial-count] #(fname %)
                     ; [:pick-ph-date-range] #(fname %)
                     [:prior-price-history] #(fname %)
                     [:process-final-states] #(fname %)])
   (multi-update-in sparams
                    [[:build-constant-state] #(fname %)
                     [:build-init-state] #(fname %)
                     [:is-final-state?] #(fname %)
                     [:build-next-state] #(fname %)
                     [:build-return-state] #(fname %)])])

; This function runs a single randomized trial. The trading-period start date is randomly chosen. That is why
; this function is considered to run a randomized trial instead of just a trial.
;
; Arguments:
;
; inital state - a map with fields that represent the changing strategy state
; constant-state - a map with fields that store strategy parameters that don't change as the strategy is executed
; next-state - a function of 3 parameters, the current strategy state, the constant parameters, and price histories
; is-final-state? - a function of 3 paramters, the current strategy state, the constant parameters, and price histories
; build-return-state - a function of 3 parameters, the current strategy state, the constant parameters, and price histories
; price-history-map - a map of ticker/price-history-tree pairs
;
; eparams - same as in run-experiment
; sparams - same as in run-experiment
; tsparams - same as in run-strategy-trial-set
; tparams - trial paramters
;   :price-history-map - a map of ticker/price-history-tree pairs
;   :earliest-trading-start - the earliest date a trial may begin
;   :latest-trading-start - the latest date a trial may begin
;
; Returns the final state that the strategy was in when the trial run completed.
(defn run-strategy-trial [eparams sparams tsparams tparams]
  (let [#^Period trading-period-length (sparams-trading-period-length sparams)
        build-constant-state (sparams-build-constant-state sparams)
        build-init-state (sparams-build-init-state sparams)
        is-final-state? (sparams-is-final-state? sparams)
        build-next-state (sparams-build-next-state sparams)
        build-return-state (sparams-build-return-state sparams)

        earliest-trading-start (tparams-earliest-trading-start tparams)
        latest-trading-start (tparams-latest-trading-start tparams)

        #^DateTime trading-period-start (random-datetime earliest-trading-start latest-trading-start)
        trading-period-end (.plus trading-period-start trading-period-length)

        constant-state (build-constant-state eparams sparams tsparams tparams trading-period-start trading-period-end)
        initial-state (build-init-state eparams sparams tsparams tparams constant-state)]
    (loop [current-state initial-state]
      (if (is-final-state? eparams sparams tsparams tparams constant-state current-state)
        (build-return-state eparams sparams tsparams tparams constant-state current-state)
        (recur (build-next-state eparams sparams tsparams tparams constant-state current-state))))))

; Arguments:
;
; eparams - same as in run-experiment
; sparams - same as in run-experiment
; tsparams - trial-set parameters - parameters that are constant for all trials in the trial set
;   :ticker-set - A collection of tickers to be traded in each of the trials of the trial set
;   :trial-count - The number of trials to run in this trial set.
;
; DEPRECATED NOTE:
; run-strategy-trial-set assumes that every company in ticker-set may be traded at any
; time between eparams:start-datetime and eparams:end-datetime (inclusive).
; END DEPRECATED NOTE
;
; If the ticker-set does not have any overlapping/common price history, then this function returns nil
; because the companies in the ticker-set cannot be traded simultaneously.
(defn run-strategy-trial-set [{earliest-trading-start :start-datetime
                               #^DateTime latest-trading-end :end-datetime
                               process-final-states :process-final-states
                               ; pick-ph-date-range :pick-ph-date-range
                               prior-price-history :prior-price-history
                               :as eparams}
                              {:keys [#^Period trading-period-length] :as sparams}
                              {:keys [ticker-set trial-count] :as tsparams}]
  (if-let [common-price-history-interval (price-history-cdr ticker-set)]
    (let [[#^DateTime start-ph-cdr #^DateTime end-ph-cdr] common-price-history-interval       ; get the endpoints of the common date range (cdr)

          latest-trading-start (.minus latest-trading-end trading-period-length)

          latest-trading-start-cdr (.minus end-ph-cdr trading-period-length)

          prior-price-history-duration (or (if prior-price-history (prior-price-history eparams sparams tsparams))
                                           Duration/ZERO)

          ; prior-price-history-duration will be either a Period or Duration object.
          ; That's why *warn-on-reflection* complains about the following line.
          earliest-trading-start-cdr (.plus start-ph-cdr prior-price-history-duration)

          ; trials will start sometime in the interval:
          ; [(max earliest-trading-start earliest-trading-start-cdr), (min latest-trading-start latest-trading-start-cdr)]
          ; we have to pick a period in which the price histories of the companies in the ticker-set overlap
          ; the lower bound should not be less than earliest-trading-start
          ; the upper bound should not be greater than latest-trading-start
          trading-start-lower-bound (max-datetime earliest-trading-start earliest-trading-start-cdr)
          trading-start-upper-bound (min-datetime latest-trading-start latest-trading-start-cdr)]

      ; ensure that the lower bound is <= the upper bound
      (if (not (after? trading-start-lower-bound trading-start-upper-bound))
        (let [trading-start-interval (interval-between trading-start-lower-bound trading-start-upper-bound)
              [start-of-ph end-of-ph] (date-pair (expand-interval trading-start-interval
                                                                  :before prior-price-history-duration
                                                                  :after trading-period-length))

              price-histories (load-price-histories ticker-set start-of-ph end-of-ph)

              ; pick-ph-date-range is a function that returns the date range over which price history is REQUIRED.
              ; If pick-ph-date-range returns a pair where the first element is nil, then all the price history will be loaded.
              ; [start-of-ph end-of-ph] (pick-ph-date-range eparams sparams tsparams)

              tparams (struct-map trial-params :price-history-map price-histories
                                               :earliest-trading-start trading-start-lower-bound
                                               :latest-trading-start trading-start-upper-bound)]
          ;(println "Result of" trial-count "trials, trading" ticker-set)
          (process-final-states (doall (pmap (fn [_] (run-strategy-trial eparams sparams tsparams tparams))
                                                   (range trial-count)))))))))
          ; (process-final-states (time (doall (pmap (fn [_] (run-strategy-trial eparams sparams tsparams tparams))
          ;                                          (range trial-count))))))))))

; This is a function that may be used as a ticker-set-gen function
;   (i.e. may be referenced in the aparams:ticker-set-gen field)
; Example: (n-ph-files-per-set 2 ["AAPL.csv" "DELL.csv" "C.csv" "D.csv"])
;          -> (["D.csv"] ["AAPL.csv"])
(defn n-ph-files-per-set [n filenames]
  (tupleize (rand-nth-seq n filenames)))

; This is a function that may be used as a distribute-trial-count function
;   (i.e. may be referenced in the eparams:distribute-trial-count field)
; Example: (passthrough-trial-count [["AAPL.csv"] ["DELL.csv"] ["C.csv"]] 14)
;          -> ([["AAPL.csv"] 5] [["DELL.csv"] 5] [["C.csv"] 5])
(defn passthrough-trial-count [ticker-sets trial-count]
  (map (fn [ticker-set] [ticker-set trial-count])
       ticker-sets))

; This is a function that may be used as a distribute-trial-count function
;   (i.e. may be referenced in the eparams:distribute-trial-count field)
; Example: (randomly-distribute-trial-count [["AAPL.csv"] ["DELL.csv"] ["C.csv"]] 14)
;          -> ([["DELL.csv"] 5] [["C.csv"] 6] [["AAPL.csv"] 3])
(defn randomly-distribute-trial-count [ticker-sets trial-count]
  (map (fn [[k v]] [k v])
       (frequencies (rand-nth-seq trial-count ticker-sets))))

; Run an experiment consisting of a number of trial-sets, with each trial-set consisting of
; a set of trials run against a fixed parameter set and ticker-set.
;
; Arguments:
;
; eparams - experiment parameters
;   :ph-files - a sequence of filenames that reference the price history files that we want to use as our ticker symbols.
;   :trial-count - a number of trials to run. This field's meaning is dependant upon the distribute-trial-count function.
;   :start-datetime - trials may not be run over time periods before this datetime
;                     NOTE: the span of time between start-datetime and end-datetime (inclusive) must be
;                           at least as large as the sparams:trading-period-length
;   :end-datetime - trials may not be run over time periods after this datetime
;                   NOTE: the span of time between start-datetime and end-datetime (inclusive) must be
;                         at least as large as the sparams:trading-period-length
;   :ticker-set-gen - a function that takes a sequence of price history filenames and returns a sequence of ticker sets, each
;                     of which will be used in a trial set.
;   :distribute-trial-count - a function that takes a sequence of ticker sets and a trial count and 
;                             returns pairs of ticker-set/trial-count-per-ticker-set pairs.
;   :prior-price-history - a function that returns an org.joda.time.Period or org.joda.time.Duration object 
;                          representing the amount of price history that the strategy needs
;                          **before** the start date of a trial.
;   DEPRECATED :pick-ph-date-range - a function that returns a pair of datetime objects representing the span of dates over which
;                         the strategy needs price history information. Function arguments: eparams, sparams, tsparams
;   :process-final-states - A function that processes the sequence of final states returned from running a set of trials.
;                           The return value of this function is returned from invocations of run-strategy-trial-set.
;
; sparams - strategy parameters
;   :commission - The cost of commission charged for each trade
;   :principal - Initial investment amount.
;   :trading-period-length - The trading period is the period in which the strategy is active. This defines the length of the trading period.
;   :time-increment - The period of time that passes between trading opportunities.
;   :trading-schedule - The weekly trading schedule that trades must occur within.
;   :build-constant-state - a function of 6 parameters: eparams, sparams, tsparams, tparams, trading-period-start, trading-period-end
;   :build-init-state - a function of 5 parameters: eparams, sparams, tsparams, tparams, constant-state
;   :is-final-state? - a function of 3 paramters: current-state, constant-state, tparams
;   :build-next-state - a function of 3 parameters: current-state, constant-state, tparams
;   :build-return-state - a function of 3 parameters: current-state, constant-state, tparams
;
; Returns a lazy sequence of vector pairs where each key/value pair is a ticker-set/trial-set-return-values.
(defn run-experiment [{:keys [trial-count ph-files ticker-set-gen distribute-trial-count] :as eparams} sparams]
  (let [ticker-sets (ticker-set-gen ph-files)                                           ; e.g. (["AAPL.csv"] ["DELL.csv"] ["C.csv"])
        ticker-set-trial-count-pairs (distribute-trial-count ticker-sets trial-count)   ; e.g. ([["AAPL.csv"] 33] [["DELL.csv"] 33] [["C.csv"] 34])
        tsparams-seq (map #(struct-map trial-set-params
                                       :ticker-set (first %)
                                       :trial-count (second %))
                          ticker-set-trial-count-pairs)             ; e.g. (({:trial-count 33, :ticker-set ["AAPL.csv"]} ...)
        run-trial-set (fn [tsparams]
                        [(tsparams :ticker-set) (run-strategy-trial-set eparams sparams tsparams)])]
    ; since the run-trial-set function returns a pair [a b] where the second element can be nil, 
    ; we want to filter out all the pairs where the second item is nil.
    (filter #(not (nil? (second %)))
            (map run-trial-set tsparams-seq))))
