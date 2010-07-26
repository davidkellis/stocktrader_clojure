(ns portfolio
  (:use [pricehistory :only (price-quote price-close)]
        [clojure.contrib.math :only (floor)]
        [dke.contrib.dkeutils :only (multi-assoc-in)]))

(defn shares-on-hand [portfolio ticker]
  (or (get-in portfolio [:securities ticker]) 0))

(defn hold-shares? [portfolio ticker]
  (> (shares-on-hand portfolio ticker) 0))

(defn owe-shares? [portfolio ticker]
  (< (shares-on-hand portfolio ticker) 0))

; Buy as many shares as possible with the amount of cash on hand.
; takes 5 parameters: strategy-state, commission, ticker, time, price-history-map
; returns a new strategy state
(defn buy-amap [{portfolio :portfolio t-log :transactions :as strategy-state} commission ticker time price-history-map]
  (let [cash (portfolio :cash)
        price ((price-quote ticker time price-history-map) :close)
        max-shares (floor (/ (- cash commission) price))
        cost (* price max-shares)
        shares-on-hand (shares-on-hand portfolio ticker)]
    (if (and (> max-shares 0)
             (>= cash (+ cost commission)))
      (multi-assoc-in strategy-state [[:portfolio :cash] (- cash (+ cost commission))
                                      [:portfolio :securities ticker] (+ shares-on-hand max-shares)
                                      [:transactions] (conj t-log [:buy max-shares ticker time price])])
      strategy-state)))

; takes 7 parameters: strategy-state, commission, ticker, share-count, time, price-history-map, on-margin?
; returns a new strategy state
(defn buy-shares [{portfolio :portfolio t-log :transactions :as strategy-state} commission ticker share-count time price-history-map on-margin?]
  (let [cash (portfolio :cash)
        price ((price-quote ticker time price-history-map) :close)
        cost (* price share-count)
        shares-on-hand (shares-on-hand portfolio ticker)
        post-purchase-cash-balance (- cash (+ cost commission))]
    (if (and (>= cost 0)
             (or (>= post-purchase-cash-balance 0)
                 on-margin?))
      (multi-assoc-in strategy-state [[:portfolio :cash] post-purchase-cash-balance
                                      [:portfolio :securities ticker] (+ shares-on-hand share-count)
                                      [:transactions] (conj t-log [:buy share-count ticker time price])])
      strategy-state)))

; takes 7 parameters: strategy-state, commission, ticker, share-count, time, price-history-map, short-sell?
; returns a new strategy state
(defn sell-shares [{portfolio :portfolio t-log :transactions :as strategy-state} commission ticker share-count time price-history-map short-sell?]
  (let [shares-on-hand (shares-on-hand portfolio ticker)]
    (if (and (> share-count 0)
             (or (<= share-count shares-on-hand)
                 short-sell?))
      (let [cash (portfolio :cash)
            price ((price-quote ticker time price-history-map) :close)
            sale-proceeds (* price share-count)]
        (multi-assoc-in strategy-state [[:portfolio :cash] (- (+ cash sale-proceeds) commission)
                                        [:portfolio :securities ticker] (- shares-on-hand share-count)
                                        [:transactions] (conj t-log [:sell share-count ticker time price])]))
      strategy-state)))

; takes 5 parameters: strategy-state, commission, ticker, time, price-history-map
(defn sell-all [{portfolio :portfolio :as strategy-state} commission ticker time price-history-map]
  (sell-shares strategy-state commission ticker (shares-on-hand portfolio ticker) time price-history-map false))

; Examples:
;   (stock-value ["AAPL" 50] t1 price-histories)     <- we own 50 shares of AAPL (we're long on AAPL)
;   (stock-value ["AAPL" -25] t2 price-histories)    <- we owe 25 shares of AAPL (we're in debt for 25 shares of AAPL; we probably shorted AAPL)
(defn stock-value [[ticker shares-held] time price-history-map]
  (* shares-held (price-close ticker time price-history-map)))

; takes 3 parameters: portfolio, time, price-history-map
; returns the value of the portfolio at the given time
(defn portfolio-value [{:keys [cash securities]} time price-history-map]
  (+ cash                                                                    ; cash
     (reduce + (map #(stock-value % time price-history-map) securities))))   ; + value of securities
