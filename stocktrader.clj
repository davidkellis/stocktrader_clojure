(ns stocktrader
  (:use averagebands
        buyandhold
        schedule
        pricehistory
        strategyutils
        com.davidsantiago.csv
        dke.contrib.csv
        dke.contrib.datetime
        dke.contrib.string
        dke.contrib.filesystem
        [dke.contrib.dkeutils :only (run-as-script?)])
  (:import java.io.File
           [org.joda.time DateTimeConstants LocalTime Period]))

(defn sort-historical-quote-file [inputfile]
  (let [date-extractor #(str (% 0) (% 1))]
    (loop [sorted-contents (sorted-map)
           quotes (read-csv inputfile)
           line (first quotes)]
      (if (not line)
        (vals sorted-contents)
        (recur (assoc sorted-contents (date-extractor line) line)
               (rest quotes)
               (first (rest quotes)))))))

(defn process-raw-price-history-file []
  (let [filename (nth *command-line-args* 1)
        outfilename (nth *command-line-args* 2)]
    (write-csv-file outfilename (sort-historical-quote-file filename))))


(defn main []
  ; *command-line-args* is of the form: [scriptname arg1 arg2 ... argN]
  ; (println *command-line-args*)
  ; (process-raw-price-history-file)
  ; (run-bah-experiment *weekly-trading-hours*))
  (run-avg-bands-experiment *weekly-trading-hours*))

; Only run the application automatically if run as a script, not if loaded in a REPL with load-file.
(if (run-as-script?) (main))