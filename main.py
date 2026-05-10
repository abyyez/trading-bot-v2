'''
V39 — Production-quality crypto trading bot.

Changes from V38 (minimal, data-driven adjustments):
  #A  ATR_SL_MULTIPLIER 1.0 -> 2.0  (V38 stops were inside 4h candle noise; 0% wins on
                                      Trend Following and Mean Reversion confirmed it)
  #B  ATR_TP_MULTIPLIER 4.0 -> 5.0  (preserve 2.5R targets after wider stops)
  #C  MAX_POSITIONS_PER_SECTOR cap (V38 filled all alt slots with same-sector names —
                                    DOGE/SHIB/CRV all crypto-beta = correlated risk)

Changes from V37 (V38 baseline — unchanged):
  #1  Flask async fixed — uses run_coroutine_threadsafe to main loop
  #2  Order intents (PENDING/FILLED/FAILED/ORPHANED) for crash safety
  #3  Defensive ccxt parsing — _safe_float, _parse_order_fill
  #4  Tiered reconciliation (ERROR blocks, WARN alerts only)
  #5  Equity cache protected with lock
  #6  Mark-to-market daily reset
  #7  /pause and /resume endpoints
  #8  Trend Following (Donchian breakout) for TRENDING regime
  #9  Liquidity filter — min 24h volume + max spread
  #10 Dynamic SL multiplier based on volatility + regime
  #11 Scoring approach for Sector Momentum (weight-of-evidence)
  #12 trade_analysis table logs entry context for post-trade learning
'''