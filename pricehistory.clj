(ns pricehistory
  (:use schedule
        dke.contrib.csv
        [dke.contrib.core :only (str-to-float)]
        [dke.contrib.datetime :only (timestamp datetime interval-between instant-between-inclusive? after? before? offset-date-pair decrement-time)]
        [dke.contrib.statistics :only (mean)]
        [dke.contrib.filesystem :only (head tail)])
  (:import [java.util TreeMap]
           [org.joda.time Interval DateTime Period]))  ; The java.util.NavigableMap interface over java.util.TreeMap requires Java 6.

; The convert-to-price-quote function takes a vector containing 6 string objects and returns a vector of the form:
; [string-date string-time Float-open Float-high Float-low Float-close]
(defn convert-to-price-quote [row]
  (vec (concat (subvec row 0 2)
               (map str-to-float
                    (subvec row 2 6)))))

(defn extract-timestamp [quote-record]
  (apply str (subvec quote-record 0 2)))

(defn read-price-history
  ([filename]
    (vec (map convert-to-price-quote (read-csv filename))))
  ([filename earliest-datetime latest-datetime]
    (if earliest-datetime
      (let [filter-fn (if latest-datetime
                        #(instant-between-inclusive? (datetime (extract-timestamp %)) earliest-datetime latest-datetime)
                        #(not (before? (datetime (extract-timestamp %))
                                       earliest-datetime)))]
        (vec (map convert-to-price-quote
                  (filter filter-fn (read-csv filename)))))
      (read-price-history filename))))

; Returns a java.util.TreeMap holding the records from a price history file, sorted by date then time.
(defn load-price-history
  ([filename]
    (let [records (read-price-history filename)
          tree (TreeMap.)]
      (doseq [quote records]
        (.put tree (extract-timestamp quote) quote))
      tree))
  ([filename earliest-datetime latest-datetime]
    (let [records (read-price-history filename earliest-datetime latest-datetime)
          tree (TreeMap.)]
      (doseq [quote records]
        (.put tree (extract-timestamp quote) quote))
      tree)))

; Returns a map of filename/java.util.TreeMap pairs, of the form:
;   {"AAPL.csv" java.util.TreeMap-holding-AAPL-price-history,
;    "GE.csv" java.util.TreeMap-holding-GE-price-history, ...}
; Each TreeMap is sorted by timestamp (yyyymmddHHMMSS).
(defn load-price-histories
  ([filenames]
    (zipmap filenames (map load-price-history filenames)))
  ([filenames earliest-datetime latest-datetime]
    (zipmap filenames (map #(load-price-history % earliest-datetime latest-datetime) filenames))))

; Returns a vector pair of datetime objects representing the start and end dates of the price history file.
; Usage: (price-history-start-end "intraday_data/AAPL.csv")
;        -> [#<DateTime 1999-04-01T08:32:00.000-06:00> #<DateTime 2009-04-01T13:42:00.000-05:00>]
(defn price-history-start-end [filename]
  (let [start-dt (datetime (extract-timestamp (first (parse-simple-csv (first (head filename 1))))))
        end-dt (datetime (extract-timestamp (first (parse-simple-csv (first (tail filename 1))))))]
    [start-dt end-dt]))

; Returns the interval of time that a price history file covers
; The interval is computed by: end-timestamp - start-timestamp
(defn price-history-interval [filename]
  (apply interval-between (price-history-start-end filename)))

; returns true if the interval of time spanned by the price history file completely contains "interval"; false otherwise.
(defn price-history-contains? [filename #^org.joda.time.Interval interval]
  (try
    (.contains #^Interval (price-history-interval filename) interval)
    (catch Exception _ false)))

(defn enough-price-history? [filename #^Period trading-period-length]
  (try
    (let [[start #^DateTime end] (price-history-start-end filename)
          trading-start (.minus end trading-period-length)]
      (not (after? start trading-start)))
    (catch Exception _ false)))

(defn files-with-enough-price-history [filenames trading-period-length]
  (filter #(enough-price-history? % trading-period-length) filenames))

; is the date pair sequential and perhaps even overlapping/equal? (i.e. start-date <= end-date)
(defn date-pair-sequential? [[start-dt end-dt]]
  (not (before? end-dt start-dt)))

; is the date pair sequential and non-overlapping (i.e. start-date < end-date)
(defn date-pair-strict-sequential? [[start-dt end-dt]]
  (after? end-dt start-dt))

; Returns the earliest and latest start-of-trading-period datetimes that the ticker represented by filename may be traded,
; given that an experiment may last for up to trading-period-length.
; Usage: (trading-period-start-dates "intraday_data/AAPL.csv" (org.joda.time.Period/years 1))
;        -> [#<DateTime 1999-04-01T08:32:00.000-06:00> #<DateTime 2008-04-01T13:42:00.000-05:00>]
; For Reference: (price-history-start-end "intraday_data/AAPL.csv")
;                -> [#<DateTime 1999-04-01T08:32:00.000-06:00> #<DateTime 2009-04-01T13:42:00.000-05:00>]
(defn trading-period-start-dates [filename #^Period trading-period-length]
  (let [[start #^DateTime end] (price-history-start-end filename)
        adjusted-end (.minus end trading-period-length)]
    (if (before? adjusted-end start)
      nil
      [start adjusted-end])))

; Returns the common date range (CDR) of a set of price histories.
; Example, we have the following price history for each of 3 companies.
; Company A:                            |----------------------------------------------------------------------------|
; Company B:                         |------------------------------------------------|
; Company C:                                          |-------------------------------------------------------|
; CDR (common date range):                            |-------------------------------|
;
; This function returns a pair of DateTime objects representing the start and end of the CDR, of the form:
;   [#<DateTime 1999-04-01T08:32:00.000-06:00> #<DateTime 2008-04-01T13:42:00.000-05:00>]
;
; If there is no common overlap among the companies, then the function returns nil.
(defn price-history-cdr [filenames]
  (let [start-end-ranges (map price-history-start-end filenames)    ; [[start1 end2] [start2 end2] ...]
        max-dt #(if (after? %1 %2) %1 %2)
        min-dt #(if (before? %1 %2) %1 %2)
        start (reduce max-dt (map first start-end-ranges))          ; get the latest (max) start date
        end (reduce min-dt (map second start-end-ranges))]          ; get the earliest (min) end date
    (if (before? end start)
      nil
      [start end])))

; Returns a pair of datetimes representing the earliest and latest dates that a trading strategy
; may begin simultaneously trading a group of companies, assuming the trading strategy *may* trade the companies
; for up to trading-period-length.
;
; Example, we have the following price history for each of 3 companies.
; Company A:                            |----------------------------------------------------------------------------|
; Company B:                         |------------------------------------------------|
; Company C:                                          |-------------------------------------------------------|
; CDR (common date range):                            |-------------------------------|
; So, since the CDR (common date range) is the time period that we have price history information for all 3 companies
; we can trade all 3 companies simultaneously during that time period ONLY if the time period is at least as long as
; trading-period-length.
;
; This function returns a pair representing the earliest and latest start-of-trading-period datetimes that all companies
; can be traded simultaneously for a period of trading-period-length.
;
; Usage: (common-trading-period-start-dates ["intraday_data/AAPL.csv" "intraday_data/C.csv"] (org.joda.time.Period/years 1))
;        -> [#<DateTime 1999-04-01T08:32:00.000-06:00> #<DateTime 2008-04-01T13:42:00.000-05:00>]
(defn common-trading-period-start-dates
  ([filenames trading-period-length]
    (let [start-end-ranges (map price-history-start-end filenames)
          max-dt #(if (after? %1 %2) %1 %2)
          min-dt #(if (before? %1 %2) %1 %2)
          start (reduce max-dt (map first start-end-ranges))    ; get the latest (max) start date
          end (reduce min-dt (map second start-end-ranges))    ; get the earliest (min) end date
          adjusted-end (decrement-time end trading-period-length)]
      (if (before? adjusted-end start)
        nil
        [start adjusted-end])))
  ([filenames trading-period-length start-offset-dir start-offset end-offset-dir end-offset]
    (let [start-end-ranges (map (fn [f]
                                  (offset-date-pair (price-history-start-end f)
                                                    start-offset-dir
                                                    start-offset
                                                    end-offset-dir
                                                    end-offset))
                                filenames)
          max-dt #(if (after? %1 %2) %1 %2)
          min-dt #(if (before? %1 %2) %1 %2)
          start (reduce max-dt (map first start-end-ranges))    ; get the latest (max) start date
          end (reduce min-dt (map second start-end-ranges))    ; get the earliest (min) end date
          adjusted-end (decrement-time end trading-period-length)]
      (if (before? adjusted-end start)
        nil
        [start adjusted-end]))))

; timestamp is a string timestamp of the form yyyymmddHHMMSS
; report-tree is a java.util.TreeMap that implements java.util.NavigableMap
; returns a vector pair of the form timestamp/quote-record (i.e. record-key/record-value) if a record was found,
;   or [nil nil] otherwise.
(defn most-recent-report [#^String timestamp #^TreeMap report-tree]
  (let [record (.floorEntry report-tree timestamp)]
    (if record
      (vector (.getKey record) (.getValue record))      ; found a record! Return [timestamp quote-record]
      [nil nil])))                                      ; NO record found. Return [nil nil]

(defn price-report [ticker time price-history-map]
  (let [price-history-tree (price-history-map ticker)]
    (most-recent-report (timestamp time) price-history-tree)))

; returns a map with keys: date, time, open, high, low, close
;   representing a price quote
(defn price-quote [ticker time price-history-map]
  (let [[timestamp report] (price-report ticker time price-history-map)]
    (zipmap [:date :time :open :high :low :close] report)))

(defn price-close [ticker time price-history-map]
  ((price-quote ticker time price-history-map) :close))

; Returns a sequence of historic price quote records, sorted in reverse chronological order (i.e. starting
; with the most recent data, ending with the oldest data)
; The first price quote is the most recent price quote available as of start-time and
; subsequent price quotes are the most recent quotes as of older dates.
; The points in time at which prices are queried are during **scheduled trading hours**.
(defn price-quote-seq [ticker start-time time-increment price-history-map weekly-schedule]
  (map #(price-quote ticker % price-history-map) (reverse-scheduled-time-series start-time time-increment weekly-schedule)))

(defn price-close-seq [ticker start-time time-increment price-history-map weekly-schedule]
  (map #(% :close) (price-quote-seq ticker start-time time-increment price-history-map weekly-schedule)))

(defn price-average
  ([ticker n-periods start-time time-increment price-history-map weekly-trading-schedule]
    (mean (take n-periods (price-close-seq ticker start-time time-increment price-history-map weekly-trading-schedule))))
  ([ticker n-periods start-time time-increment price-history-map weekly-trading-schedule price-filter]
    (mean (take n-periods
                (filter price-filter (price-close-seq ticker start-time time-increment price-history-map weekly-trading-schedule))))))


(defn forward-price-quote-seq [ticker start-time time-increment n-past-quotes price-history-map weekly-schedule]
  (map #(price-quote ticker % price-history-map) (mixed-scheduled-time-series start-time time-increment n-past-quotes weekly-schedule)))

(defn forward-price-close-seq [ticker start-time time-increment n-past-quotes price-history-map weekly-schedule]
  (map #(% :close) (forward-price-quote-seq ticker start-time time-increment n-past-quotes price-history-map weekly-schedule)))
