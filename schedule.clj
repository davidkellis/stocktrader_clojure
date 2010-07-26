(ns schedule
  (:use dke.contrib.datetime)
  (:import [org.joda.time DateTime LocalTime DateTimeConstants]))

; default: M-F: 8:30 to 15:00
(defn weekday-hours
  ([] (weekday-hours (LocalTime. 8 30 0) (LocalTime. 15 0 0)))
  ([start-of-day end-of-day]
    (hash-map DateTimeConstants/MONDAY    [start-of-day end-of-day]
              DateTimeConstants/TUESDAY   [start-of-day end-of-day]
              DateTimeConstants/WEDNESDAY [start-of-day end-of-day]
              DateTimeConstants/THURSDAY  [start-of-day end-of-day]
              DateTimeConstants/FRIDAY    [start-of-day end-of-day])))

; At each day during which trading takes place, the behavior of this function
; includes the start of trading, but excludes the end of trading (i.e. [9:30:00, 15:00:00) ).
(defn within-schedule? [#^DateTime instant weekly-schedule]
  (let [time-pair (weekly-schedule (.getDayOfWeek instant))
        start (.withFields instant (first time-pair))
        end (.withFields instant (second time-pair))]
    (instant-between? instant start end)))

(defn end-of-previous-trading-day [#^DateTime instant weekly-schedule]
  (let [instant-prev-day (.minusDays instant 1)
        day-of-week (.getDayOfWeek instant-prev-day)
        time-pair (weekly-schedule day-of-week)]    ; time pair is a vector pair: [start-time end-time]
    (if time-pair
      (.withFields instant-prev-day                 ; return the datetime occuring 1 second before the end of the previous trading day
                   (.minusSeconds #^LocalTime (second time-pair) 1))
      (recur instant-prev-day weekly-schedule))))   ; no trading hours for the day of instant-next-day, so try subsequent day

(defn start-of-subsequent-trading-day [#^DateTime instant weekly-schedule]
  (let [instant-next-day (.plusDays instant 1)
        day-of-week (.getDayOfWeek instant-next-day)
        time-pair (weekly-schedule day-of-week)]
    (if time-pair
      (.withFields instant-next-day (first time-pair))    ; return the start of the next trading day
      (recur instant-next-day weekly-schedule))))         ; no trading hours for the day of instant-next-day, so try subsequent day

; assumes that the instant is not within the schedule; behaves as if the instant is not within the schedule, even if it is.
(defn next-scheduled-time [#^DateTime instant weekly-schedule]
  (if-let [time-pair (weekly-schedule (.getDayOfWeek instant))]
    (let [start (.withFields instant (first time-pair))
          end (.withFields instant (second time-pair))]
      (cond (before? instant start)     ; instant is before start of trading session -> return start
              start
            (or (= instant end)         ; instant is equal to or after end of trading session -> return the start of the next trading day
                (after? instant end))
              (start-of-subsequent-trading-day instant weekly-schedule)
            :default                    ; perhaps instant represents a weekend or holiday -> return the start of the next trading day
              (start-of-subsequent-trading-day instant weekly-schedule)))
    (start-of-subsequent-trading-day instant weekly-schedule)))

; assumes that the instant is not within the schedule; behaves as if the instant is not within the schedule, even if it is.
(defn previous-scheduled-time [#^DateTime instant weekly-schedule]
  (if-let [time-pair (weekly-schedule (.getDayOfWeek instant))]
    (let [start (.withFields instant (first time-pair))
          end (.withFields instant (second time-pair))]
      (cond (before? instant start)     ; instant is before start of trading session -> return end of previous day
              (end-of-previous-trading-day instant weekly-schedule)
            (or (= instant end)         ; instant is equal to or after end of trading session so, return the datetime occuring
                (after? instant end))   ; 1 second before the end of the current trading day
              (.minusSeconds end 1)
            :default                    ; perhaps instant represents a weekend or holiday -> end of previous day
              (end-of-previous-trading-day instant weekly-schedule)))
    (end-of-previous-trading-day instant weekly-schedule)))

; Returns the soonest (earliest) within-schedule datetime >= instant
(defn soonest-scheduled-time [#^DateTime instant weekly-schedule]
  (if (within-schedule? instant weekly-schedule)
    instant
    (next-scheduled-time instant weekly-schedule)))

; Returns the most recent (latest) within-schedule datetime <= instant
(defn most-recent-scheduled-time [#^DateTime instant weekly-schedule]
  (if (within-schedule? instant weekly-schedule)
    instant
    (previous-scheduled-time instant weekly-schedule)))

; returns a time series consisting of DateTimes within the trading schedule (*weekly-trading-hours*)
; occuring no closer together than the span of time represented by time-increment, starting with the
; DateTime start-time. Each subsequent element of the sequence represents a point in time occuring
; later than the last.
; Example: (take 5 (scheduled-time-series (datetime) (org.joda.time.Period/hours 5) *weekly-trading-hours*))
(defn scheduled-time-series [start-time time-increment weekly-schedule]
  (let [next-time-fn #(soonest-scheduled-time (increment-time % time-increment) weekly-schedule)]
    (time-series (soonest-scheduled-time start-time weekly-schedule) next-time-fn)))

; returns a time series consisting of DateTimes within the trading schedule (*weekly-trading-hours*)
; occuring no closer together than the span of time represented by time-decrement, starting with the
; DateTime start-time. Each subsequent element of the sequence represents a point in time occuring
; earlier than the last.
; Example: (take 5 (reverse-scheduled-time-series (datetime) (org.joda.time.Period/hours 5) *weekly-trading-hours*))
(defn reverse-scheduled-time-series [start-time time-decrement weekly-schedule]
  (let [next-time-fn #(most-recent-scheduled-time (decrement-time % time-decrement) weekly-schedule)]
    (time-series (most-recent-scheduled-time start-time weekly-schedule) next-time-fn)))

; returns a time series consisting of DateTimes within the trading schedule (*weekly-trading-hours*)
; occuring no closer together than the span of time represented by time-increment, starting with the
; point in time occuring "n-past-dates" points in time before "start-time", then continuing forward
; in time at time, starting with "start-time" and increasing from then on.
; Example: (doseq [d (take 10 (mixed-scheduled-time-series (datetime "20100427090000") (org.joda.time.Period/days 1) 3 *weekly-trading-hours*))] (prn d))
;          -> #<DateTime 2010-04-22T14:59:59.000-05:00>
;             #<DateTime 2010-04-23T14:59:59.000-05:00>
;             #<DateTime 2010-04-26T09:00:00.000-05:00>
;             #<DateTime 2010-04-27T09:00:00.000-05:00>
;             #<DateTime 2010-04-28T09:00:00.000-05:00>
;             #<DateTime 2010-04-29T09:00:00.000-05:00>
;             #<DateTime 2010-04-30T09:00:00.000-05:00>
;             #<DateTime 2010-05-03T08:30:00.000-05:00>
;             #<DateTime 2010-05-04T08:30:00.000-05:00>
;             #<DateTime 2010-05-05T08:30:00.000-05:00>
(defn mixed-scheduled-time-series [start-time time-increment n-past-dates weekly-schedule]
  (concat (reverse (take n-past-dates (rest (reverse-scheduled-time-series start-time time-increment weekly-schedule))))
          (scheduled-time-series start-time time-increment weekly-schedule)))

; This function returns the datetime occuring n periods, each of length period-duration, prior to reference-datetime.
; The absolute length of time between reference-datetime and the datetime that this function returns is dependent
;   upon the weekly-schedule and the reference-datetime.
; The absolute length of time equal to the duration of the n periods can be estimated by fixing the
;   reference-datetime and weekly schedule, then computing the difference between the reference-datetime and the
;   function's return value. To be on the safe side, you could add some small buffer period to the duration
;   represented by the difference.
(defn n-periods-prior [n period-duration reference-datetime weekly-schedule]
  (nth (reverse-scheduled-time-series reference-datetime period-duration weekly-schedule) n))

; Returns a org.joda.time.Period of time that estimates the amount of time n periods, each of length period-duration, is equal to.
(defn n-periods-prior-absolute-estimate [n period-duration reference-datetime weekly-schedule]
  (period-between (n-periods-prior n period-duration reference-datetime weekly-schedule)
                  reference-datetime))
