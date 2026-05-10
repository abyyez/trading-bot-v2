"""
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
"""

import ccxt.pro as ccxtpro
import ccxt
import asyncio
import pandas as pd
import numpy as np
import time
import requests
import threading
import datetime
import io
import os
import sys
import signal
import sqlite3
import logging
import json
import uuid
from functools import wraps
from dotenv import load_dotenv

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mplfinance as mpf
from flask import Flask
from dashboard import init_dashboard

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SniperBot")

BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')
ADMIN_SECRET = os.getenv('ADMIN_SECRET', '')
INITIAL_EXECUTION_MODE = os.getenv('EXECUTION_MODE', 'PAPER').upper()

if not all([BOT_TOKEN, CHAT_ID, API_KEY, API_SECRET]):
    logger.error("CRITICAL: Missing credentials! Halting.")
    sys.exit(1)

# =============================================================================
# CONFIGURATION
# =============================================================================

DB_FILE = "trading_state.db"
STARTING_CASH = 1000.0
MIN_ORDER_USD = 12.0
COOLDOWN_SECONDS = 1800
MAX_DAILY_DRAWDOWN = 0.04
FEE_RATE = 0.001
SLIPPAGE_ESTIMATE = 0.001
FEE_SLIPPAGE_RATE = FEE_RATE + SLIPPAGE_ESTIMATE

MAX_OPEN_POSITIONS = 4
MAX_POSITIONS_PER_SECTOR = 2  # FIX #C: Cap correlated exposure
MAX_HOLD_TIME_SEC = 48 * 3600
CONSECUTIVE_LOSS_PAUSE_HOURS = 4
MARKET_QUIET_HOURS = 48

# Risk model
RISK_PER_TRADE_PCT = 0.005
MAX_PORTFOLIO_RISK_PCT = 0.02
BTC_RISK_PER_TRADE_PCT = 0.01

# ATR-based SL/TP (base values — modified dynamically by volatility)
# V39: Widened from 1.0/4.0 — V38 stops were getting wicked out inside 4h noise.
# 2 ATR survives normal volatility; 5 ATR keeps 2.5R targets intact.
ATR_SL_MULTIPLIER = 2.0
ATR_TP_MULTIPLIER = 5.0
MIN_TP_R_MULTIPLE = 2.0
TRAILING_ATR_WIDE = 1.5
TRAILING_ATR_TIGHT = 1.2
TRAILING_TIGHTEN_PCT = 0.06
PARTIAL_EXIT_ATR_MULT = 2.0
PARTIAL_EXIT_PCT = 0.30

# Liquidity requirements (FIX #9)
MIN_24H_VOLUME_USD = 5_000_000
MAX_SPREAD_PCT = 0.003

# Signal filters
REQUIRE_CONFIRMATION_CANDLE = True
RANGE_SUPPORT_TOLERANCE = 0.05
RANGE_RSI_THRESHOLD = 48
PENDING_SIGNAL_EXPIRY_SEC = 5 * 3600
MAX_RECENT_PUMP_PCT = 0.30
BTC_ATR_SPIKE_MULTIPLIER = 2.2
REGIME_ADX_TRENDING = 23
REGIME_RANGE_LOOKBACK = 30
REGIME_REDETECT_HOURS = 12
REGIME_REDETECT_PRICE_MOVE = 0.03

# Sector momentum (now scoring-based — FIX #11)
SECTOR_SCORE_THRESHOLD = 8  # out of 12
SECTOR_EMA_TOLERANCE = 0.015
SECTOR_EMA_SLOPE_MIN = 0.0007
SECTOR_ADX_MIN = 14
SECTOR_VOL_MA_RATIO_MIN = 0.60

# Trend following (FIX #8)
DONCHIAN_PERIOD = 20
TREND_VOL_CONFIRM_RATIO = 1.3

SECTOR_TOKENS = {
    'AI':   ['FET/USDT', 'RENDER/USDT', 'TAO/USDT', 'AGIX/USDT', 'OCEAN/USDT', 'AI16Z/USDT'],
    'L1':   ['SOL/USDT', 'AVAX/USDT', 'NEAR/USDT', 'SUI/USDT', 'APT/USDT', 'SEI/USDT'],
    'DEFI': ['UNI/USDT', 'AAVE/USDT', 'MKR/USDT', 'CRV/USDT', 'DYDX/USDT', 'SNX/USDT'],
    'MEME': ['DOGE/USDT', 'SHIB/USDT', 'PEPE/USDT', 'WIF/USDT', 'BONK/USDT', 'FLOKI/USDT'],
}
MAX_SECTOR_SCAN_SYMBOLS = 10

ACCUMULATION_BUY_USD = 25.0
ACCUMULATION_FEAR_THRESHOLD = 15
ACCUMULATION_MAX_ALLOC_PCT = 0.30
ACCUMULATION_PROFIT_EXIT_PCT = 0.20
ACCUMULATION_EMA200_EXIT = True

# Governance
SIGNAL_MIN_TRADES_FOR_EVAL = 10
SIGNAL_DISABLE_EXPECTANCY = -0.3
SIGNAL_REENABLE_COOLDOWN_SEC = 48 * 3600
CIRCUIT_BREAKER_R_THRESHOLD = -1.5

STABLECOIN_BASES = {
    'USDC', 'FDUSD', 'TUSD', 'BUSD', 'USDP', 'DAI', 'USDD', 'PYUSD',
    'GUSD', 'FRAX', 'LUSD', 'SUSD', 'CUSD', 'CEUR', 'MIM', 'DOLA',
    'UST', 'USTC', 'USDJ', 'USDN', 'TRIBE', 'FEI', 'USDE', 'ALUSD',
    'SFRAX', 'SDAI', 'SUSDE', 'USDB', 'USD0', 'GHO', 'CRVUSD',
    'EUSD', 'ZUSD', 'HUSD', 'OUSD', 'RSR',
    'EUR', 'TRY', 'GBP', 'AEUR', 'RUB', 'BRL', 'ARS', 'PLN',
    'UAH', 'RON', 'AUD', 'JPY', 'KRW', 'NGN', 'ZAR', 'IDR',
}

POSITION_UPDATE_INTERVAL_SEC = 4 * 3600
POSITION_AGING_ALERT_SEC = 24 * 3600
SCAN_INTERVAL_ACTIVE = 180
SCAN_INTERVAL_IDLE = 600

# Exit checker — independent fast loop that polls exits even when watch_tickers is quiet
EXIT_CHECK_INTERVAL_SEC = 5       # check every 5 seconds minimum
MAX_TICKER_STALENESS_SEC = 30     # if no tick in 30s, force REST fetch
EQUITY_CACHE_SECONDS = 30
SECTOR_CACHE_HOURS = 4
FUNDING_CACHE_SECONDS = 3600
DAILY_CANDLE_CACHE_SECONDS = 3600
HISTORICAL_SUPPORT_LOOKBACK = 200
MEAN_REVERSION_ZSCORE_ENTRY = -1.8
MEAN_REVERSION_ZSCORE_EXIT = 0.5

# Memory management (for low-RAM environments like 1GB VPS)
MAX_CACHE_ENTRIES = 100         # cap caches to prevent unbounded growth
DISABLE_CHARTS = os.getenv('DISABLE_CHARTS', '0') == '1'   # skip chart rendering to save RAM

chart_lock = threading.Lock()
db_lock = threading.Lock()
entry_lock = asyncio.Lock()
equity_cache_lock = threading.Lock()  # FIX #5
app = Flask(__name__)

# Main event loop reference — set in main() — used by Flask for run_coroutine_threadsafe
_main_loop = None


# FIX #1: Equity cache
_equity_cache = {'equity': None, 'cash': None, 'risk': None, 'ts': 0}

# Other caches
_funding_cache = {}
_daily_candle_cache = {}
_taker_ratio_cache = {}
_last_position_update_time = 0.0

binance = ccxtpro.binance({
    'apiKey': API_KEY, 'secret': API_SECRET,
    'enableRateLimit': True, 'options': {'defaultType': 'spot'}
})
binance_futures_sync = ccxt.binance({
    'apiKey': API_KEY, 'secret': API_SECRET,
    'options': {'defaultType': 'future'}, 'enableRateLimit': True,
})
binance_spot_sync = ccxt.binance({
    'apiKey': API_KEY, 'secret': API_SECRET,
    'options': {'defaultType': 'spot'}, 'enableRateLimit': True,
})


# =============================================================================
# 1. DEFENSIVE PARSING (FIX #3)
# =============================================================================

def _safe_float(value, default=0.0):
    """Parse anything to float without crashing."""
    try:
        if value is None or value == '':
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_order_fill(order, fallback_amount=0.0):
    """Extract (fill_price, filled_amount, fee_cost) from a ccxt order response.
    Handles missing fields, varying structures across exchanges."""
    fill_price = _safe_float(order.get('average'))
    if fill_price == 0:
        fill_price = _safe_float(order.get('price'))
    if fill_price == 0:
        # Last resort: try 'trades' array
        trades = order.get('trades') or []
        if trades:
            fill_price = _safe_float(trades[0].get('price'))

    filled = _safe_float(order.get('filled'), fallback_amount)

    fee_info = order.get('fee')
    fee_cost = 0.0
    if isinstance(fee_info, dict):
        fee_cost = _safe_float(fee_info.get('cost'))
    elif isinstance(fee_info, list) and fee_info:
        # Binance sometimes returns list
        fee_cost = sum(_safe_float(f.get('cost')) for f in fee_info if isinstance(f, dict))
    else:
        # Fallback: sum from trades
        trades = order.get('trades') or []
        for t in trades:
            tf = t.get('fee') if isinstance(t, dict) else None
            if isinstance(tf, dict):
                fee_cost += _safe_float(tf.get('cost'))

    return fill_price, filled, fee_cost


# =============================================================================
# FORMATTING
# =============================================================================

def format_price(price, symbol=None):
    if price is None or price == 0:
        return '0'
    abs_price = abs(price)
    if symbol and symbol in binance.markets:
        try:
            return binance.price_to_precision(symbol, price)
        except Exception:
            pass
    if abs_price >= 1000: return f"{price:.2f}"
    elif abs_price >= 1: return f"{price:.4f}"
    elif abs_price >= 0.01: return f"{price:.6f}"
    elif abs_price >= 0.0001: return f"{price:.8f}"
    else:
        f = f"{price:.12f}".rstrip('0')
        return f + '0' if f.endswith('.') else f


def format_amount(amount, symbol=None):
    if amount is None or amount == 0: return '0'
    if symbol and symbol in binance.markets:
        try:
            return binance.amount_to_precision(symbol, amount)
        except Exception:
            pass
    return f"{amount:.8f}".rstrip('0').rstrip('.')


def is_stablecoin(symbol):
    base = symbol.split('/')[0] if '/' in symbol else symbol
    return base in STABLECOIN_BASES


def filter_tradeable_symbols(symbols_dict):
    filtered = {}
    for k, v in symbols_dict.items():
        if not k.endswith('/USDT'): continue
        base = k.split('/')[0]
        if is_stablecoin(k) or base == 'BTC': continue
        if any(suf in base for suf in ['UP', 'DOWN', 'BULL', 'BEAR', '3L', '3S', '2L', '2S']):
            continue
        # FIX #9: Liquidity gate at filter time
        qvol = _safe_float(v.get('quoteVolume'))
        if qvol < MIN_24H_VOLUME_USD:
            continue
        filtered[k] = v
    return filtered


async def check_spread(symbol, ticker=None):
    """FIX #9: Check bid-ask spread is tight enough to trade."""
    try:
        if ticker is None:
            ticker = await binance.fetch_ticker(symbol)
        bid = _safe_float(ticker.get('bid'))
        ask = _safe_float(ticker.get('ask'))
        if bid <= 0 or ask <= 0: return False
        spread = (ask - bid) / bid
        return spread <= MAX_SPREAD_PCT
    except Exception:
        return False


# =============================================================================
# FIX #C: SECTOR CONCENTRATION CONTROL
# =============================================================================

def get_sector_for_symbol(symbol):
    """Return sector name for a symbol, or None if uncategorized."""
    for sector, tokens in SECTOR_TOKENS.items():
        if symbol in tokens:
            return sector
    return None


def count_positions_in_sector(sector):
    """Count current open trades belonging to the given sector."""
    if sector is None:
        return 0
    trades = db_query('SELECT symbol FROM trades', fetchall=True) or []
    count = 0
    for t in trades:
        if get_sector_for_symbol(t['symbol']) == sector:
            count += 1
    return count


# =============================================================================
# 2. BROKER LAYER (FIX #2 order intents, FIX #3 defensive parsing)
# =============================================================================

class Broker:
    def __init__(self, mode='PAPER'):
        self.mode = mode
        self._mode_lock = threading.Lock()
        self._last_sync_time = 0
        self._exchange_balances = {}
        logger.info(f"Broker initialized in {mode} mode")

    def set_mode(self, new_mode):
        new_mode = new_mode.upper()
        if new_mode not in ('PAPER', 'LIVE'):
            raise ValueError(f"Invalid mode: {new_mode}")
        with self._mode_lock:
            old = self.mode
            self.mode = new_mode
            logger.warning(f"BROKER MODE: {old} -> {new_mode}")
            send_telegram(f"⚠️ *BROKER MODE CHANGED*\n`{old}` → `{new_mode}`")
        return new_mode

    async def sync_balances(self):
        try:
            balance = await asyncio.to_thread(binance_spot_sync.fetch_balance)
            self._exchange_balances = balance
            self._last_sync_time = time.time()
            usdt_free = _safe_float(balance.get('USDT', {}).get('free'))
            logger.info(f"Balance sync: USDT free={usdt_free:.2f}")
            return balance
        except Exception as e:
            logger.error(f"Balance sync: {e}")
            return None

    # FIX #4: Tiered reconciliation — ERROR blocks, WARN alerts only
    async def reconcile_positions(self):
        """Returns {'errors': [...], 'warnings': [...], 'info': [...]}"""
        result = {'errors': [], 'warnings': [], 'info': []}
        try:
            balance = await self.sync_balances()
            if not balance:
                result['errors'].append({'type': 'sync_failed'})
                return result

            trades = db_query('SELECT * FROM trades', fetchall=True) or []

            if self.mode != 'LIVE':
                result['info'].append({'type': 'paper_mode', 'trade_count': len(trades)})
                return result

            # Check SQLite trades exist on exchange
            for t in trades:
                base = t['symbol'].split('/')[0]
                asset_info = balance.get(base, {}) if isinstance(balance.get(base), dict) else {}
                exchange_qty = _safe_float(asset_info.get('total'))
                expected = t['amount']
                if exchange_qty < expected * 0.95:
                    result['errors'].append({
                        'type': 'missing_on_exchange', 'symbol': t['symbol'],
                        'expected': expected, 'actual': exchange_qty,
                    })

            # Check for orphaned order intents (placed but not reconciled)
            pending_intents = db_query("SELECT * FROM order_intents WHERE status = 'PENDING'",
                                       fetchall=True) or []
            for intent in pending_intents:
                # More than 10 min old = definitely stale
                if time.time() - intent['created_at'] > 600:
                    result['errors'].append({
                        'type': 'orphaned_intent',
                        'client_id': intent['client_order_id'],
                        'symbol': intent['symbol'],
                    })

            # Untracked exchange holdings (informational unless large)
            tracked_bases = {t['symbol'].split('/')[0] for t in trades}
            tracked_bases.add('USDT')
            # Also include accumulation holdings
            accum = db_query('SELECT symbol FROM accumulation', fetchall=True) or []
            for a in accum:
                tracked_bases.add(a['symbol'].split('/')[0])

            for asset, info in balance.items():
                if asset in ('info', 'free', 'used', 'total'): continue
                if asset in tracked_bases: continue
                if not isinstance(info, dict): continue
                total = _safe_float(info.get('total'))
                if total <= 0: continue

                try:
                    ticker = await binance.fetch_ticker(f"{asset}/USDT")
                    value = total * _safe_float(ticker.get('last'))
                except Exception:
                    continue

                if value > 500:
                    result['warnings'].append({
                        'type': 'large_untracked_holding',
                        'asset': asset, 'qty': total, 'usd_value': round(value, 2),
                    })
                elif value > 50:
                    result['info'].append({
                        'type': 'small_untracked_holding',
                        'asset': asset, 'usd_value': round(value, 2),
                    })
                # < $50: dust, ignore entirely

        except Exception as e:
            logger.error(f"Reconciliation: {e}", exc_info=True)
            result['errors'].append({'type': 'reconcile_exception', 'detail': str(e)})

        # Summary
        if result['errors']:
            lines = [f"🚫 *RECONCILIATION ERRORS ({len(result['errors'])})*\n"]
            for e in result['errors'][:5]:
                lines.append(f"• {e.get('type')}: {e}")
            lines.append("\n*Trading BLOCKED until resolved.*")
            send_telegram("\n".join(lines))
        elif result['warnings']:
            lines = [f"⚠️ *RECONCILIATION WARNINGS ({len(result['warnings'])})*"]
            for w in result['warnings'][:5]:
                lines.append(f"• {w.get('type')}: {w}")
            lines.append("\n_Trading continues but review advised._")
            send_telegram("\n".join(lines))
        else:
            logger.info(f"Reconciliation clean ({len(result['info'])} info items)")
            if result['info']:
                send_telegram(f"✅ *Reconciliation clean* — {len(result['info'])} dust items logged.")
            else:
                send_telegram("✅ *Reconciliation clean*")

        return result

    async def place_market_buy(self, symbol, amount, reason="", client_order_id=None):
        if self.mode == 'PAPER':
            return await self._paper_buy(symbol, amount, reason, client_order_id)
        return await self._live_buy(symbol, amount, reason, client_order_id)

    async def place_market_sell(self, symbol, amount, reason="", client_order_id=None):
        if self.mode == 'PAPER':
            return await self._paper_sell(symbol, amount, reason, client_order_id)
        return await self._live_sell(symbol, amount, reason, client_order_id)

    async def _paper_buy(self, symbol, amount, reason, client_id):
        try:
            ticker = await binance.fetch_ticker(symbol)
            ask = _safe_float(ticker.get('ask')) or _safe_float(ticker.get('last'))
            if ask <= 0: return None
            fill_price = ask * (1 + SLIPPAGE_ESTIMATE)
            fee = amount * fill_price * FEE_RATE
            return {
                'order_id': client_id or f"PAPER_{int(time.time()*1000)}",
                'symbol': symbol, 'side': 'buy', 'amount': amount,
                'fill_price': fill_price, 'fee': fee, 'fee_currency': 'USDT',
                'cost': amount * fill_price, 'status': 'filled',
                'timestamp': time.time(), 'reason': reason,
            }
        except Exception as e:
            logger.error(f"Paper buy {symbol}: {e}")
            return None

    async def _paper_sell(self, symbol, amount, reason, client_id):
        try:
            ticker = await binance.fetch_ticker(symbol)
            bid = _safe_float(ticker.get('bid')) or _safe_float(ticker.get('last'))
            if bid <= 0: return None
            fill_price = bid * (1 - SLIPPAGE_ESTIMATE)
            fee = amount * fill_price * FEE_RATE
            return {
                'order_id': client_id or f"PAPER_{int(time.time()*1000)}",
                'symbol': symbol, 'side': 'sell', 'amount': amount,
                'fill_price': fill_price, 'fee': fee, 'fee_currency': 'USDT',
                'cost': amount * fill_price, 'status': 'filled',
                'timestamp': time.time(), 'reason': reason,
            }
        except Exception as e:
            logger.error(f"Paper sell {symbol}: {e}")
            return None

    async def _live_buy(self, symbol, amount, reason, client_id):
        try:
            params = {'newClientOrderId': client_id} if client_id else {}
            order = await asyncio.to_thread(
                binance_spot_sync.create_market_buy_order, symbol, amount, params)
            fill_price, filled, fee_cost = _parse_order_fill(order, amount)
            if fill_price <= 0 or filled <= 0:
                logger.error(f"LIVE BUY {symbol}: bad fill data {order}")
                return None
            logger.info(f"LIVE BUY {symbol}: {filled}@{fill_price} fee={fee_cost}")
            return {
                'order_id': order.get('id', client_id or 'unknown'),
                'symbol': symbol, 'side': 'buy', 'amount': filled,
                'fill_price': fill_price, 'fee': fee_cost,
                'fee_currency': 'USDT', 'cost': filled * fill_price,
                'status': order.get('status', 'filled'),
                'timestamp': time.time(), 'reason': reason,
            }
        except ccxt.InsufficientFunds as e:
            send_telegram(f"❌ *INSUFFICIENT FUNDS* `{symbol}`: {e}")
            return None
        except ccxt.BaseError as e:
            send_telegram(f"❌ *ORDER FAILED* `{symbol}` buy: {e}")
            return None
        except Exception as e:
            logger.error(f"Live buy {symbol}: {e}", exc_info=True)
            return None

    async def _live_sell(self, symbol, amount, reason, client_id):
        try:
            params = {'newClientOrderId': client_id} if client_id else {}
            order = await asyncio.to_thread(
                binance_spot_sync.create_market_sell_order, symbol, amount, params)
            fill_price, filled, fee_cost = _parse_order_fill(order, amount)
            if fill_price <= 0 or filled <= 0:
                logger.error(f"LIVE SELL {symbol}: bad fill data {order}")
                return None
            logger.info(f"LIVE SELL {symbol}: {filled}@{fill_price} fee={fee_cost}")
            return {
                'order_id': order.get('id', client_id or 'unknown'),
                'symbol': symbol, 'side': 'sell', 'amount': filled,
                'fill_price': fill_price, 'fee': fee_cost,
                'fee_currency': 'USDT', 'cost': filled * fill_price,
                'status': order.get('status', 'filled'),
                'timestamp': time.time(), 'reason': reason,
            }
        except Exception as e:
            logger.error(f"Live sell {symbol}: {e}", exc_info=True)
            send_telegram(f"❌ *SELL FAILED* `{symbol}`: {e}")
            return None

    async def get_current_price(self, symbol):
        try:
            ticker = await binance.fetch_ticker(symbol)
            bid = _safe_float(ticker.get('bid')) or _safe_float(ticker.get('last'))
            ask = _safe_float(ticker.get('ask')) or _safe_float(ticker.get('last'))
            if bid <= 0 or ask <= 0: return None
            return (bid + ask) / 2
        except Exception:
            return None


broker = Broker(mode=INITIAL_EXECUTION_MODE)


# =============================================================================
# 3. DATABASE
# =============================================================================

conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA busy_timeout=5000")


def init_db():
    with db_lock:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS portfolio
                     (id INTEGER PRIMARY KEY, cash REAL, wins INTEGER, losses INTEGER,
                      current_day TEXT, daily_start_equity REAL, lockout_until REAL,
                      trading_paused INTEGER DEFAULT 0)''')

        c.execute('''CREATE TABLE IF NOT EXISTS trades
                     (symbol TEXT PRIMARY KEY, entry REAL, tp REAL, sl REAL,
                      cost REAL, amount REAL, original_amount REAL DEFAULT 0,
                      highest_price REAL, entry_time REAL, atr REAL,
                      original_sl REAL, signal_type TEXT, partial_exited INTEGER DEFAULT 0,
                      remaining_cost REAL DEFAULT 0, original_cost REAL DEFAULT 0,
                      entry_range_low REAL DEFAULT 0, entry_range_high REAL DEFAULT 0,
                      last_notified REAL DEFAULT 0, exchange_order_id TEXT DEFAULT '',
                      fill_price REAL DEFAULT 0, realized_fee REAL DEFAULT 0,
                      risk_dollars REAL DEFAULT 0, realized_partial_r REAL DEFAULT 0,
                      realized_partial_pnl REAL DEFAULT 0)''')

        c.execute('''CREATE TABLE IF NOT EXISTS cooldowns
                     (symbol TEXT PRIMARY KEY, cooldown_until REAL)''')

        c.execute('''CREATE TABLE IF NOT EXISTS trade_history
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, signal_type TEXT,
                      entry REAL, exit_price REAL, pnl_pct REAL, cost REAL,
                      entry_time REAL, exit_time REAL, exit_reason TEXT,
                      original_cost REAL DEFAULT 0, entry_order_id TEXT DEFAULT '',
                      exit_order_id TEXT DEFAULT '', total_fees REAL DEFAULT 0,
                      r_multiple REAL DEFAULT 0, risk_dollars REAL DEFAULT 0,
                      regime TEXT DEFAULT '')''')

        # FIX #12: Trade analysis — entry context for post-trade learning
        c.execute('''CREATE TABLE IF NOT EXISTS trade_analysis
                     (trade_history_id INTEGER PRIMARY KEY,
                      hour_of_day INTEGER, day_of_week INTEGER,
                      btc_atr_ratio REAL, entry_rsi REAL, entry_adx REAL,
                      entry_vol_ratio REAL, regime_at_entry TEXT,
                      sector_leader TEXT, fear_index INTEGER)''')

        # FIX #2: Order intents table for crash safety
        c.execute('''CREATE TABLE IF NOT EXISTS order_intents
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      client_order_id TEXT UNIQUE, symbol TEXT, side TEXT,
                      amount REAL, signal_type TEXT,
                      status TEXT DEFAULT 'PENDING',
                      exchange_order_id TEXT DEFAULT '',
                      fill_price REAL DEFAULT 0, fee REAL DEFAULT 0,
                      created_at REAL, completed_at REAL,
                      mode TEXT DEFAULT '')''')

        c.execute('''CREATE TABLE IF NOT EXISTS pending_signals
                     (symbol TEXT PRIMARY KEY, signal_type TEXT, trigger_price REAL,
                      atr REAL, created_at REAL, candle_ts REAL,
                      ema20 REAL, bb_lower REAL, vol_ratio REAL)''')

        c.execute('''CREATE TABLE IF NOT EXISTS regime_history
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, regime TEXT,
                      btc_adx REAL, btc_price REAL, range_low REAL, range_high REAL,
                      fear_index INTEGER, leading_sector TEXT, timestamp REAL)''')

        c.execute('''CREATE TABLE IF NOT EXISTS accumulation
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT,
                      amount REAL, cost REAL, entry_price REAL, entry_time REAL,
                      exchange_order_id TEXT DEFAULT '', realized_fee REAL DEFAULT 0)''')

        c.execute('''CREATE TABLE IF NOT EXISTS event_calendar
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, event_name TEXT,
                      event_date TEXT, impact TEXT DEFAULT 'HIGH')''')

        c.execute('''CREATE TABLE IF NOT EXISTS signal_status
                     (signal_type TEXT PRIMARY KEY, enabled INTEGER DEFAULT 1,
                      disabled_at REAL DEFAULT 0, expectancy REAL DEFAULT 0,
                      last_toggled REAL DEFAULT 0,
                      enabled_in_range INTEGER DEFAULT 1,
                      enabled_in_trend INTEGER DEFAULT 1,
                      enabled_in_defensive INTEGER DEFAULT 1)''')

        c.execute('''CREATE TABLE IF NOT EXISTS signal_rejections
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT,
                      signal_type TEXT, rejection_reason TEXT, timestamp REAL)''')

        c.execute('''CREATE TABLE IF NOT EXISTS bot_state
                     (key TEXT PRIMARY KEY, value TEXT, updated_at REAL)''')

        c.execute('''CREATE TABLE IF NOT EXISTS order_log
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      exchange_order_id TEXT, symbol TEXT, side TEXT,
                      amount REAL, fill_price REAL, fee REAL,
                      status TEXT, timestamp REAL, reason TEXT, mode TEXT DEFAULT '')''')

        c.execute('SELECT * FROM portfolio WHERE id=1')
        if not c.fetchone():
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            c.execute('INSERT INTO portfolio VALUES (1, ?, 0, 0, ?, ?, 0.0, 0)',
                      (STARTING_CASH, today, STARTING_CASH))

        # FIX #8: TREND FOLLOWING added
        for sig in ['SQUEEZE BREAKOUT', 'SECTOR MOMENTUM', 'BTC RANGE BUY',
                    'MEAN REVERSION', 'TREND FOLLOWING']:
            c.execute('INSERT OR IGNORE INTO signal_status (signal_type) VALUES (?)', (sig,))

        for key, val in [('last_signal_time', str(time.time())),
                         ('quiet_mode_multiplier', '1.0'),
                         ('reconcile_blocked', '0')]:
            c.execute('INSERT OR IGNORE INTO bot_state (key, value, updated_at) VALUES (?, ?, ?)',
                      (key, val, time.time()))

        # Schema-complete migration: define expected columns for each table.
        # Any table missing any column gets it added via ALTER TABLE.
        # This prevents the kind of silent schema drift that caused the V38 order_log bug.
        expected_schema = {
            'trades': [
                ('original_amount', 'REAL DEFAULT 0'),
                ('realized_partial_r', 'REAL DEFAULT 0'),
                ('realized_partial_pnl', 'REAL DEFAULT 0'),
                ('exchange_order_id', "TEXT DEFAULT ''"),
                ('fill_price', 'REAL DEFAULT 0'),
                ('realized_fee', 'REAL DEFAULT 0'),
                ('risk_dollars', 'REAL DEFAULT 0'),
                ('last_notified', 'REAL DEFAULT 0'),
                ('entry_range_low', 'REAL DEFAULT 0'),
                ('entry_range_high', 'REAL DEFAULT 0'),
            ],
            'portfolio': [
                ('trading_paused', 'INTEGER DEFAULT 0'),
            ],
            'order_log': [
                ('mode', "TEXT DEFAULT ''"),
            ],
            'trade_history': [
                ('original_cost', 'REAL DEFAULT 0'),
                ('entry_order_id', "TEXT DEFAULT ''"),
                ('exit_order_id', "TEXT DEFAULT ''"),
                ('total_fees', 'REAL DEFAULT 0'),
                ('r_multiple', 'REAL DEFAULT 0'),
                ('risk_dollars', 'REAL DEFAULT 0'),
                ('regime', "TEXT DEFAULT ''"),
            ],
            'accumulation': [
                ('exchange_order_id', "TEXT DEFAULT ''"),
                ('realized_fee', 'REAL DEFAULT 0'),
            ],
            'signal_status': [
                ('expectancy', 'REAL DEFAULT 0'),
                ('last_toggled', 'REAL DEFAULT 0'),
                ('enabled_in_range', 'INTEGER DEFAULT 1'),
                ('enabled_in_trend', 'INTEGER DEFAULT 1'),
                ('enabled_in_defensive', 'INTEGER DEFAULT 1'),
            ],
        }
        for table, cols in expected_schema.items():
            # Get existing columns for this table
            try:
                c.execute(f"PRAGMA table_info({table})")
                existing = {row[1] for row in c.fetchall()}
            except sqlite3.OperationalError:
                # Table doesn't exist yet — CREATE TABLE IF NOT EXISTS above handles it
                continue
            for col, coldef in cols:
                if col not in existing:
                    try:
                        c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coldef}")
                        logger.info(f"Migrated: {table}.{col}")
                    except sqlite3.OperationalError as e:
                        logger.warning(f"Migration skipped {table}.{col}: {e}")

        c.execute('SELECT COUNT(*) FROM event_calendar')
        if c.fetchone()[0] == 0:
            _seed_event_calendar(c)

        conn.commit()


def _seed_event_calendar(cursor):
    fomc = [('2026-01-28', 'FOMC'), ('2026-03-18', 'FOMC'), ('2026-05-06', 'FOMC'),
            ('2026-06-17', 'FOMC'), ('2026-07-29', 'FOMC'), ('2026-09-16', 'FOMC'),
            ('2026-11-04', 'FOMC'), ('2026-12-16', 'FOMC')]
    cpi = [(f'2026-{m:02d}-{d}', 'CPI') for m, d in
           [(1, '14'), (2, '11'), (3, '11'), (4, '10'), (5, '13'),
            (6, '10'), (7, '15'), (8, '12'), (9, '10'), (10, '14'),
            (11, '12'), (12, '10')]]
    cursor.executemany(
        'INSERT INTO event_calendar (event_date, event_name, impact) VALUES (?, ?, ?)',
        [(d, n, 'HIGH') for d, n in fomc + cpi])


init_db()


# =============================================================================
# 4. ATOMIC TRANSACTIONS
# =============================================================================

def atomic_transaction(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with db_lock:
            c = conn.cursor()
            try:
                c.execute('BEGIN IMMEDIATE')
                result = func(c, *args, **kwargs)
                conn.commit()
                return result
            except Exception as e:
                conn.rollback()
                # Log full traceback — was previously just logger.error without exc_info
                logger.error(f"TX {func.__name__} ROLLED BACK: {e}", exc_info=True)
                # For critical transactions, also alert on Telegram
                if func.__name__ in ('close_position_tx', 'partial_exit_tx'):
                    try:
                        args_preview = str(args[1:3])[:100] if len(args) > 1 else ""
                        send_telegram(
                            f"🚨 *DB TRANSACTION FAILED*\n"
                            f"Function: `{func.__name__}`\n"
                            f"Error: `{type(e).__name__}: {e}`\n"
                            f"Args: `{args_preview}`\n"
                            f"**Exits/partials may not be recorded. Check logs.**")
                    except Exception:
                        pass
                raise
    return wrapper


# FIX #2: Order intent lifecycle
@atomic_transaction
def create_intent_tx(cursor, client_id, symbol, side, amount, signal_type, mode):
    cursor.execute('''INSERT INTO order_intents
        (client_order_id, symbol, side, amount, signal_type, status, created_at, mode)
        VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)''',
        (client_id, symbol, side, amount, signal_type, time.time(), mode))


@atomic_transaction
def mark_intent_filled_tx(cursor, client_id, exchange_order_id, fill_price, fee):
    cursor.execute('''UPDATE order_intents SET status='FILLED',
        exchange_order_id=?, fill_price=?, fee=?, completed_at=?
        WHERE client_order_id=?''',
        (exchange_order_id, fill_price, fee, time.time(), client_id))


@atomic_transaction
def mark_intent_failed_tx(cursor, client_id):
    cursor.execute('''UPDATE order_intents SET status='FAILED', completed_at=?
        WHERE client_order_id=?''', (time.time(), client_id))


@atomic_transaction
def open_position_tx(cursor, symbol, entry, tp, sl, cost, amount, atr,
                      signal_type, risk_dollars, order_id, fill_price, fee,
                      range_low=0, range_high=0, mode='PAPER'):
    cursor.execute('''INSERT OR REPLACE INTO trades
        (symbol, entry, tp, sl, cost, amount, original_amount, highest_price,
         entry_time, atr, original_sl, signal_type, partial_exited,
         remaining_cost, original_cost, entry_range_low, entry_range_high,
         last_notified, exchange_order_id, fill_price, realized_fee,
         risk_dollars, realized_partial_r, realized_partial_pnl)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, 0, ?, ?, ?, ?, 0, 0)''',
        (symbol, entry, tp, sl, cost, amount, amount, entry, time.time(), atr,
         sl, signal_type, cost, range_low, range_high,
         order_id, fill_price, fee, risk_dollars))
    cursor.execute('UPDATE portfolio SET cash = cash - ? WHERE id=1', (cost,))
    cursor.execute('''INSERT INTO order_log
        (exchange_order_id, symbol, side, amount, fill_price, fee, status, timestamp, reason, mode)
        VALUES (?, ?, 'buy', ?, ?, ?, 'filled', ?, ?, ?)''',
        (order_id, symbol, amount, fill_price, fee, time.time(), signal_type, mode))


@atomic_transaction
def partial_exit_tx(cursor, symbol, partial_amount, remaining_amount,
                     new_cost, new_sl, partial_value, fee,
                     new_risk_dollars, partial_r, partial_pnl,
                     order_id, fill_price, mode='PAPER'):
    cursor.execute('''UPDATE trades SET partial_exited = 1, amount = ?,
        cost = ?, remaining_cost = ?, sl = ?,
        realized_fee = realized_fee + ?, risk_dollars = ?,
        realized_partial_r = realized_partial_r + ?,
        realized_partial_pnl = realized_partial_pnl + ?
        WHERE symbol = ?''',
        (remaining_amount, new_cost, new_cost, new_sl, fee,
         new_risk_dollars, partial_r, partial_pnl, symbol))
    cursor.execute('UPDATE portfolio SET cash = cash + ? WHERE id=1', (partial_value - fee,))
    cursor.execute('''INSERT INTO order_log
        (exchange_order_id, symbol, side, amount, fill_price, fee, status, timestamp, reason, mode)
        VALUES (?, ?, 'sell', ?, ?, ?, 'filled', ?, 'PARTIAL', ?)''',
        (order_id, symbol, partial_amount, fill_price, fee, time.time(), mode))


@atomic_transaction
def close_position_tx(cursor, symbol, exit_price, pnl_pct, exit_reason,
                       trade_data, proceeds, order_id, fee, final_r_multiple,
                       regime='', mode='PAPER'):
    # CRITICAL: Delete the trade row FIRST so even if later inserts fail,
    # the position is out of the active list and can't be sold again.
    cursor.execute('DELETE FROM trades WHERE symbol = ?', (symbol,))
    
    blended_r = trade_data.get('realized_partial_r', 0) + final_r_multiple
    is_win = blended_r > 0
    wl_col = 'wins' if is_win else 'losses'
    cursor.execute(f'UPDATE portfolio SET cash = cash + ?, {wl_col} = {wl_col} + 1 WHERE id=1',
                   (proceeds,))
    cursor.execute('INSERT OR REPLACE INTO cooldowns VALUES (?, ?)',
                   (symbol, time.time() + COOLDOWN_SECONDS))
    total_fees = trade_data.get('realized_fee', 0) + fee
    total_pnl_pct = pnl_pct + trade_data.get('realized_partial_pnl', 0)

    # Defensive: coerce all values to safe types to avoid sqlite type errors
    cursor.execute('''INSERT INTO trade_history
        (symbol, signal_type, entry, exit_price, pnl_pct, cost, entry_time, exit_time,
         exit_reason, original_cost, entry_order_id, exit_order_id, total_fees,
         r_multiple, risk_dollars, regime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (str(symbol), str(trade_data.get('signal_type', '?')),
         float(trade_data['entry']), float(exit_price),
         round(float(total_pnl_pct), 2), float(trade_data['cost']),
         float(trade_data['entry_time']), time.time(),
         str(exit_reason), float(trade_data.get('original_cost', trade_data['cost'])),
         str(trade_data.get('exchange_order_id', '')), str(order_id),
         float(total_fees), round(float(blended_r), 2),
         float(trade_data.get('risk_dollars', 0)), str(regime)))

    trade_history_id = cursor.lastrowid
    cursor.execute('''INSERT INTO order_log
        (exchange_order_id, symbol, side, amount, fill_price, fee, status, timestamp, reason, mode)
        VALUES (?, ?, 'sell', ?, ?, ?, 'filled', ?, ?, ?)''',
        (str(order_id), str(symbol), float(trade_data['amount']),
         float(exit_price), float(fee), time.time(),
         str(exit_reason), str(mode)))
    return trade_history_id


@atomic_transaction
def log_trade_analysis_tx(cursor, trade_history_id, context):
    cursor.execute('''INSERT OR REPLACE INTO trade_analysis
        (trade_history_id, hour_of_day, day_of_week, btc_atr_ratio,
         entry_rsi, entry_adx, entry_vol_ratio, regime_at_entry,
         sector_leader, fear_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (trade_history_id, context.get('hour'), context.get('day_of_week'),
         context.get('btc_atr_ratio'), context.get('rsi'), context.get('adx'),
         context.get('vol_ratio'), context.get('regime'),
         context.get('sector_leader'), context.get('fear_index')))


@atomic_transaction
def accumulation_buy_tx(cursor, symbol, amount, cost, price, order_id, fee, mode='PAPER'):
    cursor.execute('''INSERT INTO accumulation
        (symbol, amount, cost, entry_price, entry_time, exchange_order_id, realized_fee)
        VALUES (?, ?, ?, ?, ?, ?, ?)''',
        (symbol, amount, cost, price, time.time(), order_id, fee))
    cursor.execute('UPDATE portfolio SET cash = cash - ? WHERE id=1', (cost,))
    cursor.execute('''INSERT INTO order_log
        (exchange_order_id, symbol, side, amount, fill_price, fee, status, timestamp, reason, mode)
        VALUES (?, ?, 'buy', ?, ?, ?, 'filled', ?, 'ACCUM', ?)''',
        (order_id, symbol, amount, price, fee, time.time(), mode))


@atomic_transaction
def accumulation_sell_tx(cursor, proceeds, order_ids_and_fees, symbol='BTC/USDT', mode='PAPER'):
    cursor.execute('DELETE FROM accumulation')
    cursor.execute('UPDATE portfolio SET cash = cash + ? WHERE id=1', (proceeds,))
    for oid, fee, amt, price in order_ids_and_fees:
        cursor.execute('''INSERT INTO order_log
            (exchange_order_id, symbol, side, amount, fill_price, fee, status, timestamp, reason, mode)
            VALUES (?, ?, 'sell', ?, ?, ?, 'filled', ?, 'ACCUM_EXIT', ?)''',
            (oid, symbol, amt, price, fee, time.time(), mode))


def db_query(query, args=(), fetchone=False, fetchall=False, commit=False):
    with db_lock:
        c = conn.cursor()
        try:
            c.execute(query, args)
            if commit: conn.commit()
            if fetchone:
                row = c.fetchone()
                return dict(row) if row else None
            if fetchall:
                return [dict(row) for row in c.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"DB [{query[:80]}]: {e}")
            return [] if fetchall else None


def get_portfolio():
    return db_query('SELECT * FROM portfolio WHERE id=1', fetchone=True)


def log_signal_rejection(symbol, signal_type, reason):
    db_query('INSERT INTO signal_rejections (symbol, signal_type, rejection_reason, timestamp) VALUES (?, ?, ?, ?)',
             (symbol, signal_type, reason, time.time()), commit=True)


def set_bot_state(key, value):
    db_query('INSERT OR REPLACE INTO bot_state (key, value, updated_at) VALUES (?, ?, ?)',
             (key, str(value), time.time()), commit=True)


def get_bot_state(key, default=None):
    row = db_query('SELECT value FROM bot_state WHERE key = ?', (key,), fetchone=True)
    return row['value'] if row else default


def mark_signal_activity():
    set_bot_state('last_signal_time', time.time())


def is_reconcile_blocked():
    return get_bot_state('reconcile_blocked', '0') == '1'


def is_trading_paused():
    p = get_portfolio()
    return bool(p and p.get('trading_paused'))


# =============================================================================
# 5. ORDER INTENT HELPERS (FIX #2)
# =============================================================================

def create_order_intent(symbol, side, amount, signal_type):
    """Call BEFORE broker.place_market_*. Returns client_order_id."""
    client_id = f"bot-{uuid.uuid4().hex[:16]}"
    create_intent_tx(client_id, symbol, side, amount, signal_type, broker.mode)
    return client_id


async def execute_order_with_intent(symbol, side, amount, signal_type):
    """Place an order with intent tracking. Returns (client_id, fill_or_None)."""
    client_id = create_order_intent(symbol, side, amount, signal_type)
    try:
        if side == 'buy':
            fill = await broker.place_market_buy(symbol, amount, signal_type, client_id)
        else:
            fill = await broker.place_market_sell(symbol, amount, signal_type, client_id)

        if fill:
            mark_intent_filled_tx(client_id, fill['order_id'], fill['fill_price'], fill['fee'])
        else:
            mark_intent_failed_tx(client_id)
        return client_id, fill
    except Exception as e:
        mark_intent_failed_tx(client_id)
        logger.error(f"execute_order_with_intent {symbol}: {e}", exc_info=True)
        return client_id, None


# =============================================================================
# 6. EQUITY CALCULATION (FIX #5: locked, FIX #6: async for mark-to-market reset)
# =============================================================================

async def calculate_equity(price_overrides=None, use_cache=True):
    """Returns (equity, cash, total_risk). Returns (None, cash, None) if stale."""
    global _equity_cache
    now = time.time()

    with equity_cache_lock:
        if use_cache and _equity_cache['equity'] is not None and (now - _equity_cache['ts']) < EQUITY_CACHE_SECONDS:
            return _equity_cache['equity'], _equity_cache['cash'], _equity_cache['risk']

    p = get_portfolio()
    if not p:
        return STARTING_CASH, STARTING_CASH, 0
    cash = p['cash']

    trades = db_query('SELECT * FROM trades', fetchall=True)
    if not trades:
        with equity_cache_lock:
            _equity_cache.update({'equity': cash, 'cash': cash, 'risk': 0, 'ts': now})
        return cash, cash, 0

    market_value = 0
    total_risk = 0
    price_overrides = price_overrides or {}
    stale = False

    for t in trades:
        price = price_overrides.get(t['symbol'])
        if price is None:
            price = await broker.get_current_price(t['symbol'])
        if price is None:
            logger.warning(f"Equity: no price for {t['symbol']} — stale")
            stale = True
            continue
        position_value = t['amount'] * price
        position_value -= position_value * FEE_RATE
        market_value += position_value
        total_risk += max(0, (price - t['sl']) * t['amount'])

    if stale:
        return None, cash, None

    equity = cash + market_value
    with equity_cache_lock:
        _equity_cache.update({'equity': equity, 'cash': cash, 'risk': total_risk, 'ts': now})
    return equity, cash, total_risk


def invalidate_equity_cache():
    with equity_cache_lock:
        _equity_cache['ts'] = 0


async def check_daily_reset():
    """FIX #6: Async + mark-to-market."""
    p = get_portfolio()
    if not p: return
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    if p['current_day'] != today_str:
        equity, _, _ = await calculate_equity(use_cache=False)
        if equity is None:
            # Fallback at reset time — acceptable because MAX_HOLD_TIME_SEC ensures positions roll forward
            trades = db_query('SELECT cost FROM trades', fetchall=True)
            equity = p['cash'] + sum(t['cost'] for t in trades) if trades else p['cash']
        db_query('UPDATE portfolio SET current_day = ?, daily_start_equity = ? WHERE id=1',
                 (today_str, equity), commit=True)


# =============================================================================
# 7. MATH UTILS
# =============================================================================

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def calculate_atr(df, period=14):
    hl = df['high'] - df['low']
    hc = (df['high'] - df['close'].shift()).abs()
    lc = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def calculate_adx(df, period=14):
    plus_dm_raw = df['high'].diff()
    minus_dm_raw = -df['low'].diff()
    plus_dm = plus_dm_raw.where((plus_dm_raw > minus_dm_raw) & (plus_dm_raw > 0), 0.0)
    minus_dm = minus_dm_raw.where((minus_dm_raw > plus_dm_raw) & (minus_dm_raw > 0), 0.0)
    hl = df['high'] - df['low']
    hc = (df['high'] - df['close'].shift()).abs()
    lc = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    atr_s = tr.rolling(window=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr_s)
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr_s)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    return dx.rolling(window=period).mean()


def calculate_recent_pump(df, lookback=18):
    if len(df) < lookback + 1: return 0.0
    low = df['low'].iloc[-lookback:].min()
    curr = df['close'].iloc[-1]
    return (curr - low) / low if low > 0 else 0.0


def is_hammer_candle(c):
    body = abs(c['close'] - c['open'])
    full = c['high'] - c['low']
    if full <= 0: return False
    lw = min(c['open'], c['close']) - c['low']
    uw = c['high'] - max(c['open'], c['close'])
    return lw > body * 1.5 and uw < full * 0.35


def find_swing_levels(df, lookback=30, w=2):
    r = df.tail(lookback)
    sh, sl = [], []
    for i in range(w, len(r) - w):
        if r['high'].iloc[i] == r['high'].iloc[i-w:i+w+1].max():
            sh.append(r['high'].iloc[i])
        if r['low'].iloc[i] == r['low'].iloc[i-w:i+w+1].min():
            sl.append(r['low'].iloc[i])
    if not sh: sh = [r['high'].max()]
    if not sl: sl = [r['low'].min()]
    return np.mean(sorted(sl)[:3]), np.mean(sorted(sh, reverse=True)[:3])


def find_historical_sr_levels(df, lookback=200):
    r = df.tail(lookback)
    if len(r) < 50: return [], []
    sh, sl = [], []
    w = 5
    for i in range(w, len(r) - w):
        if r['high'].iloc[i] == r['high'].iloc[i-w:i+w+1].max():
            sh.append(r['high'].iloc[i])
        if r['low'].iloc[i] == r['low'].iloc[i-w:i+w+1].min():
            sl.append(r['low'].iloc[i])

    def cluster(lvls, pct=0.015):
        if not lvls: return []
        s = sorted(lvls)
        cs = [[s[0]]]
        for lvl in s[1:]:
            if (lvl - cs[-1][-1]) / cs[-1][-1] <= pct:
                cs[-1].append(lvl)
            else:
                cs.append([lvl])
        return sorted([(np.mean(c), len(c)) for c in cs], key=lambda x: x[1], reverse=True)

    return [c[0] for c in cluster(sl)[:3]], [c[0] for c in cluster(sh)[:3]]


def calculate_zscore(series, lookback=50):
    if len(series) < lookback: return 0.0
    m = series.rolling(lookback).mean().iloc[-1]
    s = series.rolling(lookback).std().iloc[-1]
    if s <= 0 or np.isnan(s): return 0.0
    return (series.iloc[-1] - m) / s


def is_candle_fresh(df, max_age_seconds=7200):
    if df is None or len(df) == 0: return False
    last_ts = df['ts'].iloc[-1]
    if isinstance(last_ts, (int, float)):
        last_ts_sec = last_ts / 1000 if last_ts > 1e12 else last_ts
    else:
        return True
    return (time.time() - last_ts_sec) < max_age_seconds


# =============================================================================
# FIX #10: Dynamic SL multiplier based on volatility + regime
# =============================================================================

def get_dynamic_sl_multiplier(regime, btc_atr_ratio):
    """Return the ATR multiplier to use for stops, adjusted for current conditions."""
    base = ATR_SL_MULTIPLIER
    # Volatility adjustment
    if btc_atr_ratio > 1.5:
        base *= 1.25
    elif btc_atr_ratio < 0.8:
        base *= 0.90
    # Regime adjustment
    if regime == 'TRENDING':
        base *= 1.15  # trends need room
    elif regime == 'DEFENSIVE':
        base *= 1.10
    return base


# =============================================================================
# 8. REGIME DETECTOR
# =============================================================================

class RegimeDetector:
    def __init__(self):
        self.current_regime = 'RANGE'
        self.range_low = 0.0
        self.range_high = 0.0
        self.btc_adx = 0.0
        self.fear_index = 50
        self.leading_sector = 'NONE'
        self.btc_atr_ratio = 1.0
        self._last_detect_time = 0.0
        self._last_detect_price = 0.0
        self._regime_change_range_low = 0.0
        self._regime_change_range_high = 0.0

    def needs_redetection(self, current_price):
        elapsed = time.time() - self._last_detect_time
        if elapsed > REGIME_REDETECT_HOURS * 3600: return True
        if self._last_detect_price > 0:
            if abs(current_price - self._last_detect_price) / self._last_detect_price >= REGIME_REDETECT_PRICE_MOVE:
                return True
        return False

    async def detect(self, df_daily):
        df = df_daily.iloc[:-1].copy() if len(df_daily) > 2 else df_daily.copy()
        df['adx'] = calculate_adx(df)
        df['ema50'] = df['close'].ewm(span=50).mean()
        df['atr'] = calculate_atr(df)

        last = df.iloc[-1]
        self.btc_adx = last['adx'] if not np.isnan(last['adx']) else 0
        self.range_low, self.range_high = find_swing_levels(df, REGIME_RANGE_LOOKBACK)
        price = last['close']
        ema50 = last['ema50']
        old = self.current_regime
        defensive_ref = self._regime_change_range_low if self._regime_change_range_low > 0 else self.range_low

        if price < defensive_ref * 0.97 or price < ema50 * 0.95:
            self.current_regime = 'DEFENSIVE'
        elif self.btc_adx >= REGIME_ADX_TRENDING and price > self.range_high:
            self.current_regime = 'TRENDING'
        else:
            self.current_regime = 'RANGE'

        if self.current_regime != old:
            self._regime_change_range_low = self.range_low
            self._regime_change_range_high = self.range_high
            logger.info(f"REGIME: {old} -> {self.current_regime}")

        await self._fetch_fear_index()
        self._last_detect_time = time.time()
        self._last_detect_price = price

        db_query('''INSERT INTO regime_history (regime, btc_adx, btc_price, range_low, range_high,
                    fear_index, leading_sector, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                 (self.current_regime, round(self.btc_adx, 1), round(price, 2),
                  round(self.range_low, 2), round(self.range_high, 2),
                  self.fear_index, self.leading_sector, time.time()), commit=True)
        return self.current_regime

    async def _fetch_fear_index(self):
        try:
            def _f():
                return int(requests.get("https://api.alternative.me/fng/?limit=1", timeout=10).json()['data'][0]['value'])
            self.fear_index = await asyncio.to_thread(_f)
        except Exception as e:
            logger.warning(f"Fear: {e}")

    def is_near_range_support(self, price, tol=0.02):
        return price <= self.range_low * (1 + tol)

    def range_broke_down_for_trade(self, curr, trade_low):
        ref = trade_low if trade_low > 0 else self.range_low
        return curr < ref * 0.98


# =============================================================================
# 9. SECTOR TRACKER
# =============================================================================

class SectorTracker:
    def __init__(self):
        self.sector_performance = {}
        self.sector_absolute = {}
        self.leader = 'NONE'
        self._last_update = 0.0

    def needs_update(self):
        return time.time() - self._last_update > SECTOR_CACHE_HOURS * 3600

    async def update(self, bc):
        if not self.needs_update(): return
        try:
            btc_bars = await bc.fetch_ohlcv('BTC/USDT', '1d', limit=8)
            btc_df = pd.DataFrame(btc_bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            btc_7d = (btc_df['close'].iloc[-1] - btc_df['close'].iloc[-7]) / btc_df['close'].iloc[-7]
            self.sector_performance, self.sector_absolute = {}, {}
            for sec, toks in SECTOR_TOKENS.items():
                rets = []
                for tok in toks:
                    try:
                        if tok not in bc.markets: continue
                        bars = await bc.fetch_ohlcv(tok, '1d', limit=8)
                        d = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                        if len(d) >= 7:
                            rets.append((d['close'].iloc[-1] - d['close'].iloc[-7]) / d['close'].iloc[-7])
                    except Exception:
                        continue
                if rets:
                    avg = sum(rets) / len(rets)
                    self.sector_absolute[sec] = round(avg * 100, 2)
                    self.sector_performance[sec] = round((avg - btc_7d) * 100, 2)
                else:
                    self.sector_absolute[sec] = -999.0
                    self.sector_performance[sec] = 0.0
            cands = {k: v for k, v in self.sector_performance.items()
                     if self.sector_absolute.get(k, -999) >= -5.0}
            self.leader = max(cands, key=cands.get) if cands else 'NONE'
            self._last_update = time.time()
        except Exception as e:
            logger.error(f"Sector: {e}")

    def get_scan_symbols(self):
        if self.leader == 'NONE' or self.leader not in SECTOR_TOKENS:
            return []
        return SECTOR_TOKENS[self.leader][:MAX_SECTOR_SCAN_SYMBOLS]


# =============================================================================
# 10. CACHED FETCHERS
# =============================================================================

async def fetch_funding_rate(symbol):
    now = time.time()
    if symbol in _funding_cache and now - _funding_cache[symbol][1] < FUNDING_CACHE_SECONDS:
        return _funding_cache[symbol][0]
    try:
        perp = symbol.replace('/USDT', '/USDT:USDT')
        def _f():
            return _safe_float(binance_futures_sync.fetch_funding_rate(perp).get('fundingRate'))
        rate = await asyncio.to_thread(_f)
        _funding_cache[symbol] = (rate, now)
        _prune_cache(_funding_cache)
        return rate
    except Exception:
        _funding_cache[symbol] = (0, now)
        _prune_cache(_funding_cache)
        return 0


async def fetch_daily_candles_cached(symbol):
    now = time.time()
    if symbol in _daily_candle_cache and now - _daily_candle_cache[symbol][1] < DAILY_CANDLE_CACHE_SECONDS:
        return _daily_candle_cache[symbol][0]
    try:
        bars = await binance.fetch_ohlcv(symbol, '1d', limit=210)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        _daily_candle_cache[symbol] = (df, now)
        _prune_cache(_daily_candle_cache)
        return df
    except Exception:
        return None


def _prune_cache(cache_dict):
    """Evict oldest entries if cache exceeds MAX_CACHE_ENTRIES."""
    if len(cache_dict) <= MAX_CACHE_ENTRIES:
        return
    # Sort by timestamp (second element of tuple), keep most recent
    sorted_items = sorted(cache_dict.items(), key=lambda x: x[1][1], reverse=True)
    keep = dict(sorted_items[:MAX_CACHE_ENTRIES])
    cache_dict.clear()
    cache_dict.update(keep)


# =============================================================================
# 11. CHARTS & TELEGRAM
# =============================================================================

def generate_chart(df, symbol, title):
    if DISABLE_CHARTS:
        return None  # Low-RAM mode: skip chart rendering entirely
    with chart_lock:
        try:
            df_c = df.copy().tail(40)
            df_c['ts'] = pd.to_datetime(df_c['ts'], unit='ms')
            df_c.set_index('ts', inplace=True)
            df_c.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                                 'close': 'Close', 'vol': 'Volume'}, inplace=True)
            ap = []
            if 'ema20' in df_c.columns:
                ap.append(mpf.make_addplot(df_c['ema20'], color='blue', width=1.2))
            if 'ema50' in df_c.columns:
                ap.append(mpf.make_addplot(df_c['ema50'], color='orange', width=1.2))
            mc = mpf.make_marketcolors(up='green', down='red', edge='inherit', wick='inherit', volume='in')
            s = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc)
            buf = io.BytesIO()
            kwargs = dict(type='candle', style=s, volume=True,
                          savefig=dict(fname=buf, dpi=100), figsize=(8, 5),
                          title=f"\n{symbol} {title}")
            if ap: kwargs['addplot'] = ap
            mpf.plot(df_c, **kwargs)
            buf.seek(0)
            plt.close('all')
            return buf
        except Exception as e:
            logger.error(f"Chart: {e}")
            return None


def send_telegram(message, image_buf=None):
    photo = image_buf.getvalue() if image_buf else None
    def _send():
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/"
        for a in range(3):
            try:
                if photo:
                    r = requests.post(url + "sendPhoto",
                        data={"chat_id": CHAT_ID, "caption": message, "parse_mode": "Markdown"},
                        files={'photo': ('chart.png', photo, 'image/png')}, timeout=20)
                else:
                    r = requests.post(url + "sendMessage",
                        data={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=20)
                if r.status_code == 200: return
            except Exception as e:
                logger.warning(f"TG {a+1}: {e}")
            if a < 2: time.sleep(2 ** (a + 1))
        try:
            with open("telegram_failed.log", "a") as f:
                f.write(f"[{datetime.datetime.now().isoformat()}] {message}\n")
        except Exception: pass
    threading.Thread(target=_send, daemon=True).start()


# =============================================================================
# 12. GOVERNANCE
# =============================================================================

async def check_signal_performance(regime):
    for sig_type in ['SQUEEZE BREAKOUT', 'SECTOR MOMENTUM', 'BTC RANGE BUY',
                     'MEAN REVERSION', 'TREND FOLLOWING']:
        history = db_query('SELECT r_multiple FROM trade_history WHERE signal_type = ? ORDER BY id DESC LIMIT 40',
                           (sig_type,), fetchall=True)
        if len(history) < SIGNAL_MIN_TRADES_FOR_EVAL: continue
        recent = history[:20]
        r_mults = [_safe_float(h.get('r_multiple'), 0.0) for h in recent]
        if not r_mults: continue
        expectancy = sum(r_mults) / len(r_mults)
        win_rate = sum(1 for r in r_mults if r > 0) / len(r_mults)

        status = db_query('SELECT * FROM signal_status WHERE signal_type = ?', (sig_type,), fetchone=True)
        if not status: continue
        if time.time() - (status.get('last_toggled') or 0) < SIGNAL_REENABLE_COOLDOWN_SEC:
            continue

        db_query('UPDATE signal_status SET expectancy = ? WHERE signal_type = ?',
                 (round(expectancy, 2), sig_type), commit=True)

        if expectancy < SIGNAL_DISABLE_EXPECTANCY:
            if status['enabled']:
                db_query('UPDATE signal_status SET enabled = 0, disabled_at = ?, last_toggled = ? WHERE signal_type = ?',
                         (time.time(), time.time(), sig_type), commit=True)
                send_telegram(f"⚠️ *SIGNAL DISABLED: {sig_type}*\n"
                              f"Exp: `{expectancy:.2f}R` | WR: `{win_rate*100:.0f}%`")
        else:
            if not status['enabled']:
                db_query('UPDATE signal_status SET enabled = 1, last_toggled = ? WHERE signal_type = ?',
                         (time.time(), sig_type), commit=True)
                send_telegram(f"✅ *RE-ENABLED: {sig_type}* | Exp: `{expectancy:.2f}R`")


async def is_signal_enabled(signal_type, regime='RANGE'):
    status = db_query('SELECT * FROM signal_status WHERE signal_type = ?', (signal_type,), fetchone=True)
    if not status: return True
    if not status['enabled']: return False
    return bool(status.get(f"enabled_in_{regime.lower()}", 1))


async def is_event_day():
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    e = db_query('SELECT * FROM event_calendar WHERE event_date IN (?, ?) AND impact = ?',
                 (today, tomorrow, 'HIGH'), fetchone=True)
    return e is not None


def get_quiet_multiplier():
    try:
        return max(1.0, float(get_bot_state('quiet_mode_multiplier', '1.0') or '1.0'))
    except Exception:
        return 1.0


async def evaluate_market_quiet_mode():
    now = time.time()
    last = float(get_bot_state('last_signal_time', str(now)) or now)
    mult = get_quiet_multiplier()
    if now - last > MARKET_QUIET_HOURS * 3600 and mult <= 1.0:
        set_bot_state('quiet_mode_multiplier', 1.15)
        send_telegram("⚠️ *QUIET MARKET* — Thresholds +15%.")
    elif now - last <= MARKET_QUIET_HOURS * 3600 and mult > 1.0:
        set_bot_state('quiet_mode_multiplier', 1.0)


async def get_daily_conviction(symbol):
    try:
        df = await fetch_daily_candles_cached(symbol)
        if df is not None and len(df) > 20:
            ema = df['close'].ewm(span=20).mean().iloc[-1]
            return 1.0 if df.iloc[-1]['close'] >= ema else 0.5
    except Exception:
        pass
    return 1.0


# =============================================================================
# 13. STRATEGY: BTC RANGE BUY
# =============================================================================

async def strategy_btc_range(regime_detector, df_btc_4h, regime):
    if not await is_signal_enabled('BTC RANGE BUY', regime): return
    df = df_btc_4h.copy()
    df['rsi'] = calculate_rsi(df['close'])
    df['atr'] = calculate_atr(df)
    df['ema20'] = df['close'].ewm(span=20).mean()
    last = df.iloc[-2]

    if db_query('SELECT * FROM trades WHERE symbol = ?', ('BTC/USDT',), fetchone=True):
        return

    if REQUIRE_CONFIRMATION_CANDLE:
        confirm = df.iloc[-1]
        pending = db_query('SELECT * FROM pending_signals WHERE symbol = ?', ('BTC/USDT',), fetchone=True)
        if pending:
            if (confirm['close'] >= pending['trigger_price'] * 0.995
                    and regime_detector.is_near_range_support(confirm['close'], RANGE_SUPPORT_TOLERANCE)):
                db_query('DELETE FROM pending_signals WHERE symbol = ?', ('BTC/USDT',), commit=True)
                await execute_entry_btc(df, confirm, pending['atr'], regime_detector, regime)
            elif time.time() - pending['created_at'] > PENDING_SIGNAL_EXPIRY_SEC:
                db_query('DELETE FROM pending_signals WHERE symbol = ?', ('BTC/USDT',), commit=True)
            return

    quiet = get_quiet_multiplier()
    near = regime_detector.is_near_range_support(last['close'], RANGE_SUPPORT_TOLERANCE * quiet)
    bullish = last['close'] > last['open'] or is_hammer_candle(last)
    if near and last['rsi'] < (RANGE_RSI_THRESHOLD * quiet) and bullish:
        db_query('''INSERT OR REPLACE INTO pending_signals
            (symbol, signal_type, trigger_price, atr, created_at, candle_ts, ema20, bb_lower, vol_ratio)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)''',
            ('BTC/USDT', 'BTC RANGE BUY', last['close'], last['atr'],
             time.time(), last['ts'], last['ema20']), commit=True)


async def execute_entry_btc(df, candle, atr, regime_detector, regime):
    async with entry_lock:
        try:
            if is_reconcile_blocked() or is_trading_paused():
                return

            # FIX #10: Dynamic SL
            sl_mult = get_dynamic_sl_multiplier(regime, regime_detector.btc_atr_ratio)
            sl_atr = atr * sl_mult
            # Range-based SL overrides if tighter than ATR-based (tighter = better)
            range_sl = regime_detector.range_low * 0.98
            sl = max(range_sl, candle['close'] * 1.001 - sl_atr)
            tp = regime_detector.range_high * 0.99

            equity, cash, open_risk = await calculate_equity(use_cache=False)
            if equity is None: return

            ct = db_query('SELECT COUNT(*) as c FROM trades', fetchone=True)
            if ct and ct['c'] >= MAX_OPEN_POSITIONS: return

            entry_est = candle['close'] * 1.001
            risk_per_unit = entry_est - sl
            if risk_per_unit <= 0: return

            # Min 2R TP
            tp_r = (tp - entry_est) / risk_per_unit
            if tp_r < MIN_TP_R_MULTIPLE:
                log_signal_rejection('BTC/USDT', 'BTC RANGE BUY', f'tp_only_{tp_r:.1f}R')
                return

            risk_budget = equity * BTC_RISK_PER_TRADE_PCT
            if open_risk + risk_budget > equity * MAX_PORTFOLIO_RISK_PCT:
                risk_budget = max(0, equity * MAX_PORTFOLIO_RISK_PCT - open_risk)
            if risk_budget < 1: return

            amount = risk_budget / risk_per_unit
            cost = amount * entry_est
            if cost < MIN_ORDER_USD or cost > cash: return

            # FIX #2: Order intent flow
            client_id, fill = await execute_order_with_intent('BTC/USDT', 'buy', amount, 'BTC RANGE BUY')
            if not fill: return

            actual_entry = fill['fill_price']
            actual_amount = fill['amount']
            actual_cost = fill['cost']
            actual_risk = max(0, actual_amount * (actual_entry - sl))

            await asyncio.to_thread(open_position_tx,
                'BTC/USDT', actual_entry, tp, sl, actual_cost, actual_amount, atr,
                'BTC RANGE BUY', actual_risk, fill['order_id'], actual_entry, fill['fee'],
                regime_detector.range_low, regime_detector.range_high, broker.mode)
            invalidate_equity_cache()

            rr = round((tp - actual_entry) / (actual_entry - sl), 1) if actual_entry > sl else 0
            msg = (f"💰 *BTC RANGE BUY* [{broker.mode}]\n"
                   f"Entry: `{format_price(actual_entry, 'BTC/USDT')}` | "
                   f"Size: `{format_amount(actual_amount, 'BTC/USDT')}` (${actual_cost:.2f})\n"
                   f"TP: `{format_price(tp, 'BTC/USDT')}` | SL: `{format_price(sl, 'BTC/USDT')}` | "
                   f"R:R `{rr}:1` (SL mult `{sl_mult:.2f}x`)\n"
                   f"Risk: `${actual_risk:.2f}` ({actual_risk/equity*100:.2f}%)")
            chart_buf = await asyncio.to_thread(generate_chart, df, 'BTC/USDT', 'RANGE BUY')
            send_telegram(msg, chart_buf)
            mark_signal_activity()
        except Exception as e:
            logger.error(f"BTC entry: {e}", exc_info=True)


# =============================================================================
# 14. STRATEGY: SECTOR MOMENTUM (FIX #11: scoring approach)
# =============================================================================

def score_sector_entry(last, quiet_mult):
    """Weight-of-evidence scoring. Max = 12. Threshold = 8."""
    score = 0
    reasons = []
    if last['ema20'] > last['ema50']:
        score += 2; reasons.append('uptrend')
    if last['ema20_slope'] > SECTOR_EMA_SLOPE_MIN / quiet_mult:
        score += 2; reasons.append('rising')
    if 35 < last['rsi'] < 68:
        score += 2; reasons.append('rsi_ok')
    if last['vol_ma'] > 0 and last['vol'] / last['vol_ma'] >= SECTOR_VOL_MA_RATIO_MIN / quiet_mult:
        score += 2; reasons.append('volume')
    if last['adx'] > SECTOR_ADX_MIN / quiet_mult:
        score += 2; reasons.append('adx')
    if abs(last['close'] - last['ema20']) / last['ema20'] <= SECTOR_EMA_TOLERANCE * quiet_mult:
        score += 1; reasons.append('near_ema')
    if last['close'] > last['open']:
        score += 1; reasons.append('green')
    return score, reasons


async def strategy_sector_momentum(sector_tracker, btc_atr_ratio, regime):
    if sector_tracker.leader == 'NONE': return
    if not await is_signal_enabled('SECTOR MOMENTUM', regime): return
    scan = [s for s in sector_tracker.get_scan_symbols()
            if s in binance.markets and not is_stablecoin(s)]
    active_data = db_query('SELECT symbol FROM trades', fetchall=True)
    active = [r['symbol'] for r in active_data] if active_data else []
    cds = db_query('SELECT * FROM cooldowns', fetchall=True)
    cd_map = {r['symbol']: r['cooldown_until'] for r in cds} if cds else {}
    to_scan = [s for s in scan if s not in active and (s not in cd_map or time.time() >= cd_map[s])]

    for s in to_scan:
        ct = db_query('SELECT COUNT(*) as c FROM trades', fetchone=True)
        if ct and ct['c'] >= MAX_OPEN_POSITIONS: break
        await _process_sector(s, btc_atr_ratio, regime)
        await asyncio.sleep(0.2)


async def _process_sector(s, btc_atr_ratio, regime):
    try:
        bars = await binance.fetch_ohlcv(s, '4h', limit=500)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        if not is_candle_fresh(df): return
        df['ema20'] = df['close'].ewm(span=20).mean()
        df['ema50'] = df['close'].ewm(span=50).mean()
        df['rsi'] = calculate_rsi(df['close'])
        df['atr'] = calculate_atr(df)
        df['vol_ma'] = df['vol'].rolling(20).mean()
        df['adx'] = calculate_adx(df)
        df['ema20_slope'] = df['ema20'].pct_change(periods=3)
        last = df.iloc[-2]

        if calculate_recent_pump(df) > MAX_RECENT_PUMP_PCT: return
        quiet = get_quiet_multiplier()

        score, reasons = score_sector_entry(last, quiet)
        if score < SECTOR_SCORE_THRESHOLD:
            log_signal_rejection(s, 'SECTOR MOMENTUM', f'score_{score}_reasons_{len(reasons)}')
            return

        if btc_atr_ratio > BTC_ATR_SPIKE_MULTIPLIER: return
        funding = await fetch_funding_rate(s)
        if funding > 0.0008: return
        if not await check_spread(s): return

        conv = await get_daily_conviction(s)

        if REQUIRE_CONFIRMATION_CANDLE:
            confirm = df.iloc[-1]
            pending = db_query('SELECT * FROM pending_signals WHERE symbol = ?', (s,), fetchone=True)
            if pending and pending['signal_type'] == 'SECTOR MOMENTUM':
                if confirm['close'] > confirm['ema20']:
                    db_query('DELETE FROM pending_signals WHERE symbol = ?', (s,), commit=True)
                    await execute_entry_alt(s, 'SECTOR MOMENTUM', df, confirm, pending['atr'],
                                            conv, regime, btc_atr_ratio)
                elif time.time() - pending['created_at'] > PENDING_SIGNAL_EXPIRY_SEC:
                    db_query('DELETE FROM pending_signals WHERE symbol = ?', (s,), commit=True)
                return
            db_query('''INSERT OR REPLACE INTO pending_signals
                (symbol, signal_type, trigger_price, atr, created_at, candle_ts, ema20, bb_lower, vol_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)''',
                (s, 'SECTOR MOMENTUM', last['close'], last['atr'], time.time(),
                 last['ts'], last['ema20'], last['vol'] / last['vol_ma'] if last['vol_ma'] > 0 else 0),
                commit=True)
        else:
            await execute_entry_alt(s, 'SECTOR MOMENTUM', df, last, last['atr'],
                                    conv, regime, btc_atr_ratio)
    except Exception as e:
        logger.warning(f"Sector {s}: {e}")


# =============================================================================
# 15. STRATEGY: SQUEEZE BREAKOUT
# =============================================================================

async def strategy_squeeze_breakout(btc_atr_ratio, regime):
    if not await is_signal_enabled('SQUEEZE BREAKOUT', regime): return
    tickers = await binance.fetch_tickers()
    valid = filter_tradeable_symbols(tickers)
    top = sorted(valid.keys(), key=lambda x: _safe_float(valid[x].get('quoteVolume')), reverse=True)[:80]
    active_data = db_query('SELECT symbol FROM trades', fetchall=True)
    active = [r['symbol'] for r in active_data] if active_data else []
    cds = db_query('SELECT * FROM cooldowns', fetchall=True)
    cd_map = {r['symbol']: r['cooldown_until'] for r in cds} if cds else {}
    symbols = [s for s in top if s not in active and (s not in cd_map or time.time() >= cd_map[s])]

    for i in range(0, len(symbols), 12):
        ct = db_query('SELECT COUNT(*) as c FROM trades', fetchone=True)
        if ct and ct['c'] >= MAX_OPEN_POSITIONS: break
        batch = symbols[i:i + 12]
        await asyncio.gather(*[_process_squeeze(s, btc_atr_ratio, regime) for s in batch])
        await asyncio.sleep(0.3)


async def _process_squeeze(s, btc_atr_ratio, regime):
    try:
        bars = await binance.fetch_ohlcv(s, '4h', limit=500)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        if not is_candle_fresh(df): return
        df['ema20'] = df['close'].ewm(span=20).mean()
        df['ema50'] = df['close'].ewm(span=50).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        df['std'] = df['close'].rolling(20).std()
        df['upper'] = df['ma20'] + df['std'] * 2
        df['lower'] = df['ma20'] - df['std'] * 2
        df['rsi'] = calculate_rsi(df['close'])
        df['atr'] = calculate_atr(df)
        df['vol_ma'] = df['vol'].rolling(20).mean()
        df['adx'] = calculate_adx(df)
        df['bb_width'] = (df['upper'] - df['lower']) / df['ma20']
        df['bb_width_ma'] = df['bb_width'].rolling(20).mean()
        last = df.iloc[-2]
        prev = df.iloc[-3]

        if calculate_recent_pump(df) > MAX_RECENT_PUMP_PCT: return

        if not (prev['bb_width'] < prev['bb_width_ma']
                and last['vol'] > last['vol_ma'] * 1.5
                and last['adx'] > REGIME_ADX_TRENDING - 3
                and last['close'] > df['ema20'].iloc[-2]
                and prev['close'] <= prev['ema20']
                and last['ema20'] > last['ema50']):
            return
        if btc_atr_ratio > BTC_ATR_SPIKE_MULTIPLIER: return
        funding = await fetch_funding_rate(s)
        if funding > 0.0008: return
        if not await check_spread(s): return

        conv = await get_daily_conviction(s)

        if REQUIRE_CONFIRMATION_CANDLE:
            confirm = df.iloc[-1]
            if confirm['close'] > confirm['ema20']:
                await execute_entry_alt(s, 'SQUEEZE BREAKOUT', df, confirm, last['atr'],
                                        conv, regime, btc_atr_ratio, disable_hard_tp=True)
        else:
            await execute_entry_alt(s, 'SQUEEZE BREAKOUT', df, last, last['atr'],
                                    conv, regime, btc_atr_ratio, disable_hard_tp=True)
    except Exception as e:
        logger.warning(f"Squeeze {s}: {e}")


# =============================================================================
# 16. STRATEGY: MEAN REVERSION
# =============================================================================

async def strategy_mean_reversion(btc_atr_ratio, regime):
    # Change 1: Mean Reversion only runs in RANGE regime.
    # Mean reversion in TRENDING markets means buying dips that aren't dips —
    # they're consolidations before continuation. Performance data confirmed: 0% wins
    # across 4 trades during TRENDING regime. Locked here regardless of signal_status flags.
    if regime != 'RANGE':
        return
    if not await is_signal_enabled('MEAN REVERSION', regime): return
    if btc_atr_ratio > BTC_ATR_SPIKE_MULTIPLIER: return
    tickers = await binance.fetch_tickers()
    valid = filter_tradeable_symbols(tickers)
    top = sorted(valid.keys(), key=lambda x: _safe_float(valid[x].get('quoteVolume')), reverse=True)[:50]
    active_data = db_query('SELECT symbol FROM trades', fetchall=True)
    active = [r['symbol'] for r in active_data] if active_data else []
    cds = db_query('SELECT * FROM cooldowns', fetchall=True)
    cd_map = {r['symbol']: r['cooldown_until'] for r in cds} if cds else {}

    for s in top[:30]:
        if s in active or (s in cd_map and time.time() < cd_map[s]): continue
        ct = db_query('SELECT COUNT(*) as c FROM trades', fetchone=True)
        if ct and ct['c'] >= MAX_OPEN_POSITIONS: break
        try:
            bars = await binance.fetch_ohlcv(s, '4h', limit=500)
            df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            if len(df) < 100 or not is_candle_fresh(df): continue
            df['ema20'] = df['close'].ewm(span=20).mean()
            df['rsi'] = calculate_rsi(df['close'])
            df['atr'] = calculate_atr(df)
            df['vol_ma'] = df['vol'].rolling(20).mean()
            last = df.iloc[-2]

            zscore = calculate_zscore(df['close'], 50)
            if zscore > MEAN_REVERSION_ZSCORE_ENTRY or last['rsi'] > 35: continue
            if not (last['close'] > last['open'] or is_hammer_candle(last)): continue

            supports, _ = find_historical_sr_levels(df, 200)
            near = any(abs(last['close'] - sup) / sup < 0.03 for sup in supports if sup > 0)
            if not near: continue
            if last['vol_ma'] > 0 and last['vol'] < last['vol_ma'] * 1.2: continue

            if not await check_spread(s): continue

            mean_50 = df['close'].rolling(50).mean().iloc[-1]
            std_50 = df['close'].rolling(50).std().iloc[-1]
            mr_tp_raw = mean_50 + MEAN_REVERSION_ZSCORE_EXIT * std_50

            entry_est = last['close'] * 1.002
            sl_mult = get_dynamic_sl_multiplier(regime, btc_atr_ratio)
            sl = entry_est - last['atr'] * sl_mult
            risk_per_unit = entry_est - sl
            if risk_per_unit <= 0: continue
            min_tp = entry_est + MIN_TP_R_MULTIPLE * risk_per_unit
            mr_tp = max(mr_tp_raw, min_tp)

            await execute_entry_alt(s, 'MEAN REVERSION', df, last, last['atr'],
                                    0.8, regime, btc_atr_ratio, custom_tp=mr_tp)
        except Exception as e:
            logger.warning(f"MR {s}: {e}")
        await asyncio.sleep(0.2)


# =============================================================================
# 17. NEW STRATEGY: TREND FOLLOWING (FIX #8)
# =============================================================================

async def strategy_trend_following(btc_atr_ratio, regime):
    """Donchian breakout — buy new 20-period high with volume + trend confirmation.
    Only activates in TRENDING regime. Fills the gap for 'buy strength' trades."""
    if regime != 'TRENDING': return
    if not await is_signal_enabled('TREND FOLLOWING', regime): return
    if btc_atr_ratio > BTC_ATR_SPIKE_MULTIPLIER: return

    tickers = await binance.fetch_tickers()
    valid = filter_tradeable_symbols(tickers)
    # Prefer high-liquidity names — trend strategies need tight spreads
    top = sorted(valid.keys(), key=lambda x: _safe_float(valid[x].get('quoteVolume')), reverse=True)[:40]
    active_data = db_query('SELECT symbol FROM trades', fetchall=True)
    active = [r['symbol'] for r in active_data] if active_data else []
    cds = db_query('SELECT * FROM cooldowns', fetchall=True)
    cd_map = {r['symbol']: r['cooldown_until'] for r in cds} if cds else {}

    for s in top[:30]:
        if s in active or (s in cd_map and time.time() < cd_map[s]): continue
        ct = db_query('SELECT COUNT(*) as c FROM trades', fetchone=True)
        if ct and ct['c'] >= MAX_OPEN_POSITIONS: break
        try:
            bars = await binance.fetch_ohlcv(s, '4h', limit=150)
            df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            if len(df) < 130 or not is_candle_fresh(df): continue

            df['atr'] = calculate_atr(df)
            df['vol_ma'] = df['vol'].rolling(20).mean()
            df['ema20'] = df['close'].ewm(span=20).mean()
            df['ema50'] = df['close'].ewm(span=50).mean()

            last = df.iloc[-2]
            # Donchian high: prior 20 periods (exclusive of current)
            donchian_high = df['high'].iloc[-(DONCHIAN_PERIOD + 2):-2].max()
            if np.isnan(donchian_high): continue

            # Close must break prior high
            if last['close'] <= donchian_high * 1.001: continue

            # Volume confirmation
            if last['vol_ma'] > 0 and last['vol'] < last['vol_ma'] * TREND_VOL_CONFIRM_RATIO:
                continue

            # Trend structure: EMA20 above EMA50
            if last['ema20'] <= last['ema50']: continue

            # Don't chase — if already pumped 30%+ in last 18 candles, skip
            if calculate_recent_pump(df) > MAX_RECENT_PUMP_PCT: continue

            # Spread check (liquidity filter already applied upstream)
            if not await check_spread(s): continue

            # Funding check
            funding = await fetch_funding_rate(s)
            if funding > 0.001:  # More permissive than other strategies — trends eat funding
                continue

            conv = await get_daily_conviction(s)
            logger.info(f"TREND FOLLOWING signal: {s} | Break: {last['close']:.6f} > {donchian_high:.6f}")

            await execute_entry_alt(s, 'TREND FOLLOWING', df, last, last['atr'],
                                    conv, regime, btc_atr_ratio, disable_hard_tp=True)
        except Exception as e:
            logger.warning(f"Trend {s}: {e}")
        await asyncio.sleep(0.2)


# =============================================================================
# 18. UNIFIED ALT ENTRY
# =============================================================================

async def execute_entry_alt(s, signal, df, candle, atr, conv_mult, regime,
                              btc_atr_ratio, disable_hard_tp=False, custom_tp=None):
    async with entry_lock:
        try:
            if is_reconcile_blocked() or is_trading_paused():
                return

            # FIX #C: Sector concentration check
            sector = get_sector_for_symbol(s)
            if sector is not None:
                sector_count = count_positions_in_sector(sector)
                if sector_count >= MAX_POSITIONS_PER_SECTOR:
                    log_signal_rejection(s, signal, f'sector_full_{sector}_{sector_count}')
                    return

            entry_est = candle['close'] * 1.002
            # FIX #10: Dynamic SL
            sl_mult = get_dynamic_sl_multiplier(regime, btc_atr_ratio)
            sl = entry_est - atr * sl_mult
            risk_per_unit = entry_est - sl
            if risk_per_unit <= 0: return

            if custom_tp:
                min_tp = entry_est + MIN_TP_R_MULTIPLE * risk_per_unit
                tp = max(custom_tp, min_tp)
            elif disable_hard_tp:
                # No hard TP — use 0 as sentinel. position_manager checks for this.
                tp = 0.0
            else:
                tp = entry_est + atr * ATR_TP_MULTIPLIER

            # Only enforce min-R check when a real TP is set
            if tp > 0:
                tp_r = (tp - entry_est) / risk_per_unit
                if tp_r < MIN_TP_R_MULTIPLE and not disable_hard_tp:
                    log_signal_rejection(s, signal, f'tp_{tp_r:.1f}R')
                    return

            equity, cash, open_risk = await calculate_equity(use_cache=False)
            if equity is None: return

            ct = db_query('SELECT COUNT(*) as c FROM trades', fetchone=True)
            if ct and ct['c'] >= MAX_OPEN_POSITIONS: return

            risk_budget = equity * RISK_PER_TRADE_PCT * max(0.4, conv_mult)
            if open_risk + risk_budget > equity * MAX_PORTFOLIO_RISK_PCT:
                risk_budget = max(0, equity * MAX_PORTFOLIO_RISK_PCT - open_risk)
            if risk_budget < 0.50: return

            amount = risk_budget / risk_per_unit
            cost = amount * entry_est
            if cost < MIN_ORDER_USD or cost > cash: return

            # FIX #2: Order intent flow
            client_id, fill = await execute_order_with_intent(s, 'buy', amount, signal)
            if not fill: return

            actual_entry = fill['fill_price']
            actual_amount = fill['amount']
            actual_cost = fill['cost']
            actual_risk = max(0, actual_amount * (actual_entry - sl))

            await asyncio.to_thread(open_position_tx,
                s, actual_entry, tp, sl, actual_cost, actual_amount, atr,
                signal, actual_risk, fill['order_id'], actual_entry, fill['fee'],
                0, 0, broker.mode)
            invalidate_equity_cache()

            if tp > 0:
                rr_val = (tp - actual_entry) / (actual_entry - sl) if actual_entry > sl else 0
                rr_str = f"`{rr_val:.1f}:1`"
                tp_str = format_price(tp, s)
            else:
                rr_str = "`trail-only`"
                tp_str = "trailing"
            sector_tag = f" | Sec `{sector}`" if sector else ""
            msg = (f"💰 *{signal}* [{broker.mode}]\n"
                   f"`{s}` | Entry: `{format_price(actual_entry, s)}`{sector_tag}\n"
                   f"Size: `{format_amount(actual_amount, s)}` (${actual_cost:.2f})\n"
                   f"TP: {tp_str} | SL: `{format_price(sl, s)}` | R:R {rr_str}\n"
                   f"Risk: `${actual_risk:.2f}` ({actual_risk/equity*100:.2f}%) | SL×`{sl_mult:.2f}`")
            chart_buf = await asyncio.to_thread(generate_chart, df, s, signal)
            send_telegram(msg, chart_buf)
            mark_signal_activity()
        except Exception as e:
            logger.error(f"Entry {s}: {e}", exc_info=True)


# =============================================================================
# 19. POSITION MANAGER
# =============================================================================

async def send_position_updates(cached_prices=None):
    global _last_position_update_time
    now = time.time()
    if now - _last_position_update_time < POSITION_UPDATE_INTERVAL_SEC: return
    trades = db_query('SELECT * FROM trades', fetchall=True)
    if not trades: return
    _last_position_update_time = now
    lines = ["📋 *OPEN POSITIONS*\n"]
    for t in trades:
        price = (cached_prices or {}).get(t['symbol']) if cached_prices else None
        if price is None:
            price = await broker.get_current_price(t['symbol'])
        if not price: continue
        pnl = ((price - t['entry']) / t['entry'] - FEE_SLIPPAGE_RATE) * 100
        hrs = (now - t['entry_time']) / 3600
        risk_per_unit = t['entry'] - t['original_sl']
        r_mult = (price - t['entry']) / risk_per_unit if risk_per_unit > 0 else 0
        emoji = "🟢" if pnl > 0 else "🔴"
        lines.append(f"{emoji} `{t['symbol']}` ({t.get('signal_type', '?')})\n"
                     f"  `{format_price(t['entry'], t['symbol'])}` → `{format_price(price, t['symbol'])}` "
                     f"| `{pnl:+.1f}%` ({r_mult:+.1f}R) | {hrs:.0f}h")
        if hrs > POSITION_AGING_ALERT_SEC / 3600:
            lines.append(f"  ⚠️ Aging")
    if len(lines) > 1:
        send_telegram("\n".join(lines))


async def exit_checker(regime_detector):
    """FAST loop: checks SL/TP/time-stop exits every EXIT_CHECK_INTERVAL_SEC.
    
    Uses batch fetch_tickers() call (1 API request for all positions) and falls back
    to individual fetch_ticker if batch fails. Alerts on Telegram if a position can't
    be priced for more than 5 minutes — because "silent failure + 50-hour hold" is
    the worst-case scenario.
    """
    logger.info(f"Exit checker started (interval={EXIT_CHECK_INTERVAL_SEC}s)")
    last_prices = {}  # {symbol: (price, timestamp)}
    stale_alerts_sent = {}  # {symbol: last_alert_time} — don't spam

    while True:
        try:
            trades = db_query('SELECT * FROM trades', fetchall=True)
            if not trades:
                await asyncio.sleep(EXIT_CHECK_INTERVAL_SEC)
                continue

            now = time.time()
            symbols = [t['symbol'] for t in trades]

            # Batch fetch all positions in one REST call — this is the key fix.
            # fetch_tickers({symbols}) returns bid/ask/last for all in one request.
            prices_batch = {}
            try:
                batch = await binance.fetch_tickers(symbols)
                for s in symbols:
                    if s in batch:
                        t_data = batch[s]
                        bid = _safe_float(t_data.get('bid')) or _safe_float(t_data.get('last'))
                        if bid > 0:
                            prices_batch[s] = bid
            except Exception as e:
                logger.warning(f"Exit checker: batch ticker fetch failed: {e}")

            # Individual fallback for anything missing from batch
            for s in symbols:
                if s in prices_batch:
                    continue
                try:
                    ticker = await binance.fetch_ticker(s)
                    bid = _safe_float(ticker.get('bid')) or _safe_float(ticker.get('last'))
                    if bid > 0:
                        prices_batch[s] = bid
                except Exception as e:
                    logger.warning(f"Exit checker: fetch_ticker {s} failed: {e}")

            # Process each trade — now we have prices or we know we don't
            for t in trades:
                s = t['symbol']
                curr_bid = prices_batch.get(s)

                # Detect stale pricing and alert if it's been going on too long
                if not curr_bid:
                    cached_price, cached_ts = last_prices.get(s, (None, 0))
                    stale_duration = now - cached_ts if cached_ts > 0 else now - t['entry_time']
                    # Alert once every 30 minutes if we can't price a position for >5min
                    last_alert = stale_alerts_sent.get(s, 0)
                    if stale_duration > 300 and now - last_alert > 1800:
                        send_telegram(
                            f"🚨 *CANNOT PRICE POSITION*\n"
                            f"`{s}` ({t.get('signal_type', '?')}) has no fresh price for "
                            f"{stale_duration/60:.0f} minutes. Exit checks SKIPPED.\n"
                            f"Entry: `{format_price(t['entry'], s)}` | SL: `{format_price(t['sl'], s)}`\n"
                            f"Held: {(now - t['entry_time'])/3600:.1f}h")
                        stale_alerts_sent[s] = now
                    continue

                last_prices[s] = (curr_bid, now)
                stale_alerts_sent.pop(s, None)  # clear stale alert state if price resumes

                sl = t['sl']
                tp = t['tp']
                duration = now - t['entry_time']

                exit_reason = None
                if curr_bid <= sl:
                    fee_entry = t['entry'] * (1 + FEE_SLIPPAGE_RATE)
                    exit_reason = "TRAILING_PROFIT" if sl > fee_entry else "SL_HIT"
                elif tp > 0 and curr_bid >= tp and t.get('signal_type') not in ('SQUEEZE BREAKOUT', 'TREND FOLLOWING'):
                    exit_reason = "TP_HIT"
                elif (t.get('signal_type') == 'MEAN REVERSION' and tp > 0 and curr_bid >= tp):
                    exit_reason = "MR_TARGET"
                elif duration > MAX_HOLD_TIME_SEC:
                    exit_reason = "TIME_STOP"
                elif (s == 'BTC/USDT' and t['signal_type'] == 'BTC RANGE BUY'
                      and regime_detector.range_broke_down_for_trade(curr_bid, t.get('entry_range_low', 0))):
                    exit_reason = "RANGE_BREAKDOWN"

                if exit_reason:
                    try:
                        success = await _execute_exit(t, exit_reason, curr_bid, regime_detector)
                        if not success:
                            # Exit execution failed — loudly notify so user can intervene
                            send_telegram(
                                f"🚨 *EXIT FAILED: {s}*\n"
                                f"Reason: `{exit_reason}` | Bid: `{format_price(curr_bid, s)}`\n"
                                f"Held: {duration/3600:.1f}h. Will retry every {EXIT_CHECK_INTERVAL_SEC}s.\n"
                                f"If this persists, check exchange manually.")
                    except Exception as e:
                        logger.error(f"_execute_exit {s}: {e}", exc_info=True)
                        send_telegram(f"🚨 *EXIT CRASHED: {s}* reason `{exit_reason}`: {e}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Exit checker loop: {e}", exc_info=True)
            # Loop errors mean exits aren't happening — this is critical
            send_telegram(f"🚨 *EXIT CHECKER ERROR*\n`{e}`\nLoop continues but positions may not exit.")

        await asyncio.sleep(EXIT_CHECK_INTERVAL_SEC)


async def _execute_exit(t, exit_reason, trigger_price, regime_detector):
    """Shared exit execution — places order, records results, sends telegram."""
    s = t['symbol']
    
    # Defensive: coerce all numeric fields. SQLite + ALTER TABLE can produce
    # mixed types (e.g. string "0" instead of float 0.0) which causes
    # "unsupported operand type(s) for +: 'int' and 'str'" errors.
    t = dict(t)  # ensure mutable copy
    for field in ('entry', 'original_sl', 'sl', 'tp', 'cost', 'amount',
                   'original_amount', 'original_cost', 'atr', 'highest_price',
                   'entry_time', 'realized_fee', 'risk_dollars',
                   'realized_partial_r', 'realized_partial_pnl',
                   'fill_price', 'entry_range_low', 'entry_range_high'):
        t[field] = _safe_float(t.get(field), 0.0)
    
    risk_per_unit = t['entry'] - t['original_sl']
    duration = time.time() - t['entry_time']
    
    client_id, fill = await execute_order_with_intent(s, 'sell', t['amount'], exit_reason)
    if not fill:
        logger.error(f"EXIT SELL FAILED {s} (reason {exit_reason}) — will retry")
        return False
    
    exit_price = fill['fill_price']
    proceeds = fill['cost'] - fill['fee']
    actual_pnl = ((exit_price - t['entry']) / t['entry'] - FEE_SLIPPAGE_RATE) * 100
    actual_r_raw = (exit_price - t['entry']) / risk_per_unit if risk_per_unit > 0 else 0
    
    original_amt = t.get('original_amount', t['amount'])
    remaining_portion = t['amount'] / original_amt if original_amt > 0 else 1.0
    actual_r_weighted = actual_r_raw * remaining_portion
    actual_pnl_weighted = actual_pnl * remaining_portion
    
    # Critical: if this DB call fails, the sell already happened on the exchange but
    # the position row is still in our DB. We MUST know about this immediately.
    try:
        th_id = await asyncio.to_thread(close_position_tx,
            s, exit_price, actual_pnl_weighted, exit_reason, dict(t), proceeds,
            fill['order_id'], fill['fee'], actual_r_weighted,
            regime_detector.current_regime, broker.mode)
    except Exception as db_error:
        # Sell fired but DB close failed — send_telegram already alerted via decorator.
        # Now try an emergency manual DELETE so the position doesn't get sold again.
        logger.error(f"close_position_tx failed for {s} AFTER sell fill: {db_error}", exc_info=True)
        try:
            db_query('DELETE FROM trades WHERE symbol = ?', (s,), commit=True)
            logger.warning(f"Emergency DELETE of {s} trade row succeeded — accounting may be inconsistent")
            send_telegram(
                f"⚠️ *EMERGENCY TRADE DELETE: {s}*\n"
                f"Sell fired @ `{format_price(exit_price, s)}` but DB close failed.\n"
                f"Position row deleted to prevent re-sell. Accounting inconsistent — review manually.")
        except Exception as delete_error:
            logger.error(f"Emergency DELETE also failed for {s}: {delete_error}")
            send_telegram(
                f"🚨🚨 *CRITICAL: {s}*\n"
                f"Sell fired but cannot delete DB row. Bot will KEEP SELLING THIS POSITION.\n"
                f"STOP THE BOT IMMEDIATELY.")
        return False
    
    if th_id:
        dt = datetime.datetime.fromtimestamp(t['entry_time'])
        context = {
            'hour': dt.hour, 'day_of_week': dt.weekday(),
            'btc_atr_ratio': regime_detector.btc_atr_ratio,
            'regime': regime_detector.current_regime,
            'sector_leader': regime_detector.leading_sector,
            'fear_index': regime_detector.fear_index,
        }
        try:
            await asyncio.to_thread(log_trade_analysis_tx, th_id, context)
        except Exception:
            pass
    
    invalidate_equity_cache()
    blended_r = t.get('realized_partial_r', 0) + actual_r_weighted
    emoji = "✅" if blended_r > 0 else "🛑"
    
    slippage = abs(exit_price - trigger_price) / trigger_price * 100 if trigger_price > 0 else 0
    
    msg = (f"{emoji} *{exit_reason}: {s}* [{broker.mode}]\n"
           f"Entry `{format_price(t['entry'], s)}` → Exit `{format_price(exit_price, s)}`\n"
           f"Trigger: `{format_price(trigger_price, s)}` (slippage: `{slippage:.2f}%`)\n"
           f"Blended R: `{blended_r:+.2f}R` | Final chunk: `{actual_r_raw:+.1f}R`\n"
           f"Held `{duration/3600:.1f}h` | Fee `${fill['fee']:.4f}`")
    
    # Circuit breaker check
    recent_3 = db_query('SELECT r_multiple FROM trade_history ORDER BY id DESC LIMIT 3', fetchall=True)
    if len(recent_3) == 3:
        recent_rs = [_safe_float(x.get('r_multiple'), 0) for x in recent_3]
        total_r = sum(recent_rs)
        if all(r <= 0 for r in recent_rs) and total_r <= CIRCUIT_BREAKER_R_THRESHOLD:
            db_query('UPDATE portfolio SET lockout_until = ? WHERE id=1',
                     (time.time() + CONSECUTIVE_LOSS_PAUSE_HOURS * 3600,), commit=True)
            msg += f"\n🧯 Circuit — `{total_r:.2f}R` over 3 trades. Paused {CONSECUTIVE_LOSS_PAUSE_HOURS}h"
    
    equity, _, _ = await calculate_equity(use_cache=False)
    if equity is not None:
        p = get_portfolio()
        dd = (p['daily_start_equity'] - equity) / p['daily_start_equity'] if p['daily_start_equity'] > 0 else 0
        if dd >= MAX_DAILY_DRAWDOWN:
            db_query('UPDATE portfolio SET lockout_until = ? WHERE id=1',
                     (time.time() + 86400,), commit=True)
            msg += f"\n🚨 KILL SWITCH `-{dd*100:.1f}%`"
        msg += f"\n💰 Equity: `${equity:.2f}`"
    
    send_telegram(msg)
    return True


async def position_manager(regime_detector):
    """SLOW loop: handles partial exits and trailing stop updates.
    Exit checks (SL/TP/time stop) are handled by exit_checker for fast response."""
    dynamic_atr_cache = {}
    logger.info("Position Manager started (partials + trails only).")

    while True:
        try:
            trades = db_query('SELECT * FROM trades', fetchall=True)
            if not trades:
                await asyncio.sleep(5)
                continue

            symbols = [t['symbol'] for t in trades]
            try:
                tickers = await asyncio.wait_for(binance.watch_tickers(symbols), timeout=30)
            except asyncio.TimeoutError:
                tickers = {}
                for s in symbols:
                    try:
                        tickers[s] = await binance.fetch_ticker(s)
                    except Exception:
                        pass

            prices = {s: _safe_float(tickers[s].get('last')) for s in symbols if s in tickers}
            now = time.time()

            for t in trades:
                s = t['symbol']
                if s not in prices or prices[s] <= 0: continue
                # Defensive type coercion — same reason as in _execute_exit
                t = dict(t)
                for field in ('entry', 'original_sl', 'sl', 'tp', 'cost', 'amount',
                               'original_amount', 'original_cost', 'atr', 'highest_price',
                               'entry_time', 'realized_fee', 'risk_dollars',
                               'realized_partial_r', 'realized_partial_pnl'):
                    t[field] = _safe_float(t.get(field), 0.0)
                
                curr = prices[s]
                highest = max(t['highest_price'], curr)
                current_atr = t['atr']

                if s not in dynamic_atr_cache or now - dynamic_atr_cache[s] > 3600:
                    try:
                        recent = await binance.fetch_ohlcv(s, '4h', limit=20)
                        df_a = pd.DataFrame(recent, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                        current_atr = calculate_atr(df_a).iloc[-1]
                        dynamic_atr_cache[s] = now
                        db_query('UPDATE trades SET atr = ? WHERE symbol = ?', (current_atr, s), commit=True)
                    except Exception:
                        pass

                gain = (curr - t['entry']) / t['entry']
                risk_per_unit = t['entry'] - t['original_sl']

                # --- PARTIAL EXIT ---
                if not t['partial_exited'] and curr >= t['entry'] + current_atr * PARTIAL_EXIT_ATR_MULT:
                    original_amt = t.get('original_amount', t['amount'])
                    partial_amount = original_amt * PARTIAL_EXIT_PCT

                    client_id, fill = await execute_order_with_intent(s, 'sell', partial_amount, 'PARTIAL_EXIT')
                    if fill:
                        remaining_amount = t['amount'] - fill['amount']
                        remaining_cost = t.get('original_cost', t['cost']) * (1 - PARTIAL_EXIT_PCT)
                        be_sl = t['entry'] * (1 + FEE_SLIPPAGE_RATE + 0.001)
                        new_sl = max(t['sl'], be_sl)
                        new_risk = max(0, remaining_amount * (curr - new_sl))

                        partial_r_raw = (fill['fill_price'] - t['entry']) / risk_per_unit if risk_per_unit > 0 else 0
                        partial_r_weighted = partial_r_raw * PARTIAL_EXIT_PCT
                        partial_pnl = ((fill['fill_price'] - t['entry']) / t['entry'] - FEE_SLIPPAGE_RATE) * 100
                        partial_pnl_weighted = partial_pnl * PARTIAL_EXIT_PCT

                        await asyncio.to_thread(partial_exit_tx,
                            s, fill['amount'], remaining_amount, remaining_cost, new_sl,
                            fill['cost'], fill['fee'], new_risk,
                            partial_r_weighted, partial_pnl_weighted,
                            fill['order_id'], fill['fill_price'], broker.mode)
                        invalidate_equity_cache()
                        send_telegram(
                            f"📤 *PARTIAL ({int(PARTIAL_EXIT_PCT*100)}%): {s}* [{broker.mode}]\n"
                            f"Sold @ `{format_price(fill['fill_price'], s)}` | "
                            f"+{partial_r_raw:.1f}R on chunk | SL → `{format_price(new_sl, s)}`")
                    continue

                # --- TRAILING STOP ---
                # Change 3: Only trail AFTER partial exit fires. Before partial,
                # the original SL stays put to give trades room to reach the partial target.
                # Once 30% is sold and stop is at breakeven, trailing becomes a profit-lock mechanism.
                current_sl = t['sl']
                if t['partial_exited'] and highest >= t['entry'] * 1.01:
                    be_sl = t['entry'] * (1 + FEE_SLIPPAGE_RATE + 0.001)
                    trail_mult = TRAILING_ATR_TIGHT if gain >= TRAILING_TIGHTEN_PCT else TRAILING_ATR_WIDE
                    proposed = max(t['sl'], be_sl, highest - trail_mult * current_atr)
                    if proposed > current_sl:
                        current_sl = proposed

                if current_sl > t['sl'] or highest > t['highest_price']:
                    new_risk = max(0, t['amount'] * (curr - current_sl))
                    db_query('UPDATE trades SET sl = ?, highest_price = ?, risk_dollars = ? WHERE symbol = ?',
                             (current_sl, highest, new_risk, s), commit=True)
                    if current_sl > t['sl']:
                        tt = "TIGHT" if gain >= TRAILING_TIGHTEN_PCT else "WIDE"
                        send_telegram(f"🛡️ *Trail [{tt}]: {s}* | SL `{format_price(current_sl, s)}`")

        except asyncio.CancelledError:
            break
        except ccxt.BaseError as e:
            logger.warning(f"PM API: {e}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"PM: {e}", exc_info=True)
            await asyncio.sleep(2)


# =============================================================================
# 20. ACCUMULATION
# =============================================================================

async def run_accumulation(regime_detector):
    if is_reconcile_blocked() or is_trading_paused(): return
    rows = db_query('SELECT * FROM accumulation', fetchall=True)

    if rows:
        try:
            price = await broker.get_current_price('BTC/USDT')
            if not price: return
            total_cost = sum(r['cost'] for r in rows)
            total_amount = sum(r['amount'] for r in rows)
            should_exit = False
            if ACCUMULATION_EMA200_EXIT:
                df_d = await fetch_daily_candles_cached('BTC/USDT')
                if df_d is not None and len(df_d) > 200:
                    if price > df_d['close'].ewm(span=200).mean().iloc[-1]:
                        should_exit = True
            value_est = total_amount * price
            if total_cost > 0 and (value_est - total_cost) / total_cost >= ACCUMULATION_PROFIT_EXIT_PCT:
                should_exit = True

            if should_exit:
                client_id, fill = await execute_order_with_intent('BTC/USDT', 'sell', total_amount, 'ACCUM_EXIT')
                if not fill:
                    logger.error("Accum exit failed — retry next cycle")
                    return
                proceeds = fill['cost'] - fill['fee']
                order_logs = [(fill['order_id'], fill['fee'] * (r['amount'] / total_amount),
                               r['amount'], fill['fill_price']) for r in rows]
                await asyncio.to_thread(accumulation_sell_tx, proceeds, order_logs, 'BTC/USDT', broker.mode)
                invalidate_equity_cache()
                send_telegram(f"🏦 *ACCUM SOLD* [{broker.mode}]\n"
                              f"Proceeds `${proceeds:.2f}` | PnL `${proceeds - total_cost:+.2f}`")
                return
        except Exception as e:
            logger.error(f"Accum exit: {e}", exc_info=True)

    if regime_detector.fear_index > ACCUMULATION_FEAR_THRESHOLD: return
    p = get_portfolio()
    accum = db_query('SELECT SUM(cost) as total FROM accumulation', fetchone=True)
    accum_total = accum['total'] if accum and accum['total'] else 0
    if accum_total >= (p['cash'] + accum_total) * ACCUMULATION_MAX_ALLOC_PCT: return
    if p['cash'] < ACCUMULATION_BUY_USD: return

    try:
        price = await broker.get_current_price('BTC/USDT')
        if not price or not regime_detector.is_near_range_support(price, 0.05): return
        target_amount = ACCUMULATION_BUY_USD / price

        client_id, fill = await execute_order_with_intent('BTC/USDT', 'buy', target_amount, 'ACCUMULATION')
        if not fill:
            logger.error("Accum buy failed")
            return

        await asyncio.to_thread(accumulation_buy_tx,
            'BTC/USDT', fill['amount'], fill['cost'], fill['fill_price'],
            fill['order_id'], fill['fee'], broker.mode)
        invalidate_equity_cache()
        send_telegram(f"🏦 *ACCUM* [{broker.mode}] Fear `{regime_detector.fear_index}` | "
                      f"Bought `{format_amount(fill['amount'], 'BTC/USDT')}` BTC")
    except Exception as e:
        logger.error(f"Accum buy: {e}", exc_info=True)


# =============================================================================
# 21. HEARTBEAT
# =============================================================================

async def execute_heartbeat(regime_detector, sector_tracker):
    try:
        bars = await binance.fetch_ohlcv('BTC/USDT', '4h', limit=60)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        df['ema20'] = df['close'].ewm(span=20).mean()
        df['ema50'] = df['close'].ewm(span=50).mean()
        df['rsi'] = calculate_rsi(df['close'])
        df['atr'] = calculate_atr(df)
        last = df.iloc[-1]
        trend = ("🟢 Up" if last['close'] > last['ema20'] > last['ema50']
                 else "🟡 Pull" if last['ema20'] > last['ema50'] else "🔴 Bear")

        equity, cash, risk = await calculate_equity(use_cache=True)
        equity_str = f"${equity:.2f}" if equity is not None else "STALE"
        risk_str = f"${risk:.2f}" if risk is not None else "?"

        trades = db_query('SELECT * FROM trades', fetchall=True)
        accum = db_query('SELECT SUM(cost) as total FROM accumulation', fetchone=True)
        accum_t = _safe_float(accum['total'] if accum else 0, 0.0)
        history = db_query('SELECT r_multiple FROM trade_history ORDER BY id DESC LIMIT 20', fetchall=True)
        if history:
            r_vals = [_safe_float(h.get('r_multiple'), 0.0) for h in history]
            avg_r = round(sum(r_vals) / len(r_vals), 2)
        else:
            avg_r = 0

        re = {"RANGE": "📊", "TRENDING": "🚀", "DEFENSIVE": "🛡️"}.get(regime_detector.current_regime, "❓")
        pos = ""
        if trades:
            pos = "\n📌 *Positions:*\n"
            for t in trades:
                pos += f"  `{t['symbol']}` {t.get('signal_type','?')} — {(time.time()-t['entry_time'])/3600:.0f}h\n"

        flags = []
        if is_reconcile_blocked(): flags.append("🚫 RECONCILE")
        if is_trading_paused(): flags.append("⏸ PAUSED")
        flag_str = "\n" + " ".join(flags) if flags else ""

        msg = (f"🌕 *V39 HEARTBEAT* [{broker.mode}]{flag_str}\n"
               f"BTC `${last['close']:,.0f}` {trend} | RSI `{last['rsi']:.0f}`\n"
               f"{re} `{regime_detector.current_regime}` | "
               f"Range `${regime_detector.range_low:,.0f}`–`${regime_detector.range_high:,.0f}`\n"
               f"Fear `{regime_detector.fear_index}` | Sector `{sector_tracker.leader}`\n"
               f"BTC ATR ratio `{regime_detector.btc_atr_ratio:.2f}`\n"
               f"---\n"
               f"Equity `{equity_str}` | Cash `${cash:.2f}` | Risk `{risk_str}`\n"
               f"Accum `${accum_t:.2f}` | Avg R(20) `{avg_r}R` | "
               f"Trades `{len(trades) if trades else 0}/{MAX_OPEN_POSITIONS}`"
               f"{pos}")
        chart_buf = await asyncio.to_thread(generate_chart, df, 'BTC/USDT', 'Heartbeat')
        send_telegram(msg, chart_buf)
    except Exception as e:
        logger.error(f"HB: {e}", exc_info=True)


async def heartbeat_loop(regime_detector, sector_tracker):
    """Heartbeat fires every 4 hours, aligned to UTC 4h candle close boundaries
    (00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC). Fires shortly after each
    candle closes so the heartbeat reflects the latest closed 4h bar."""
    while True:
        try:
            now = datetime.datetime.now()
            # Align to next 4-hour boundary (local time, but matches UTC if server is UTC)
            nxt_hour = ((now.hour // 4) + 1) * 4
            if nxt_hour >= 24:
                # Next run is tomorrow at 00:00
                nxt_run = (now.replace(hour=0, minute=0, second=0, microsecond=0)
                           + datetime.timedelta(days=1))
            else:
                nxt_run = now.replace(hour=nxt_hour, minute=0, second=0, microsecond=0)
            sleep_seconds = max((nxt_run - now).total_seconds(), 60)
            await asyncio.sleep(sleep_seconds)
            await execute_heartbeat(regime_detector, sector_tracker)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"HB: {e}")
            await asyncio.sleep(60)


# =============================================================================
# 22. SCANNER
# =============================================================================

async def scanner_loop(regime_detector, sector_tracker):
    while True:
        try:
            await check_daily_reset()
            await evaluate_market_quiet_mode()
            await send_position_updates()

            p = get_portfolio()
            if not p or time.time() < p['lockout_until']:
                await asyncio.sleep(600)
                continue

            if is_reconcile_blocked() or is_trading_paused():
                await asyncio.sleep(300)
                continue

            btc_daily = await binance.fetch_ohlcv('BTC/USDT', '1d', limit=100)
            df_btc = pd.DataFrame(btc_daily, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            btc_price = df_btc.iloc[-1]['close']

            if regime_detector.needs_redetection(btc_price):
                regime = await regime_detector.detect(df_btc)
            else:
                regime = regime_detector.current_regime

            await sector_tracker.update(binance)
            regime_detector.leading_sector = sector_tracker.leader
            await check_signal_performance(regime)

            btc_atr = calculate_atr(df_btc)
            btc_atr_avg = btc_atr.rolling(20).mean().iloc[-1]
            btc_atr_ratio = btc_atr.iloc[-1] / btc_atr_avg if btc_atr_avg > 0 else 1.0
            regime_detector.btc_atr_ratio = btc_atr_ratio  # Expose for entries

            event_active = await is_event_day()
            ct = db_query('SELECT COUNT(*) as c FROM trades', fetchone=True)
            slots = MAX_OPEN_POSITIONS - (ct['c'] if ct else 0)
            logger.info(f"Scan | {regime} | ATR ratio {btc_atr_ratio:.2f} | Slots:{slots} | [{broker.mode}]")

            if slots <= 0:
                await asyncio.sleep(SCAN_INTERVAL_IDLE)
                continue

            if regime == 'DEFENSIVE':
                await run_accumulation(regime_detector)
                # Mean Reversion no longer runs in DEFENSIVE — it's RANGE-only now.
            elif regime == 'RANGE':
                btc_4h = await binance.fetch_ohlcv('BTC/USDT', '4h', limit=500)
                df_4h = pd.DataFrame(btc_4h, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                df_4h['ema20'] = df_4h['close'].ewm(span=20).mean()
                df_4h['ema50'] = df_4h['close'].ewm(span=50).mean()
                await strategy_btc_range(regime_detector, df_4h, regime)
                if not event_active:
                    await strategy_sector_momentum(sector_tracker, btc_atr_ratio, regime)
                    await strategy_mean_reversion(btc_atr_ratio, regime)
                await run_accumulation(regime_detector)
            elif regime == 'TRENDING':
                if not event_active:
                    await strategy_trend_following(btc_atr_ratio, regime)  # FIX #8: NEW
                    await strategy_squeeze_breakout(btc_atr_ratio, regime)
                    await strategy_sector_momentum(sector_tracker, btc_atr_ratio, regime)

            await asyncio.sleep(SCAN_INTERVAL_ACTIVE)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Scanner: {e}", exc_info=True)
            await asyncio.sleep(60)


# =============================================================================
# 24. MAIN
# =============================================================================

shutdown_event = None  # will be created in main


def handle_exit(signum, frame):
    logger.info("Shutdown...")
    if shutdown_event and _main_loop:
        _main_loop.call_soon_threadsafe(shutdown_event.set)


async def main():
    global _main_loop, shutdown_event
    _main_loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    logger.info(f"Booting V39 [{broker.mode}]")
    await binance.load_markets()
    try:
        await asyncio.to_thread(binance_futures_sync.load_markets)
    except Exception as e:
        logger.warning(f"Futures load: {e}")
    try:
        await asyncio.to_thread(binance_spot_sync.load_markets)
    except Exception as e:
        logger.warning(f"Spot sync load: {e}")

    # FIX #5: Startup reconciliation
    logger.info("Startup reconciliation...")
    recon = await broker.reconcile_positions()
    if recon['errors']:
        set_bot_state('reconcile_blocked', '1')
        logger.error(f"Reconcile: {len(recon['errors'])} errors — BLOCKED")
    else:
        set_bot_state('reconcile_blocked', '0')

    regime_detector = RegimeDetector()
    sector_tracker = SectorTracker()


    # Bind dashboard to bot internals and register HTTP routes.
    # Must be called BEFORE the Flask thread starts so routes are registered.
    init_dashboard(
        app,
        db_query=db_query,
        db_lock=db_lock,
        db_conn=conn,
        safe_float=_safe_float,
        is_reconcile_blocked=is_reconcile_blocked,
        is_trading_paused=is_trading_paused,
        get_quiet_multiplier=get_quiet_multiplier,
        get_sector_for_symbol=get_sector_for_symbol,
        set_bot_state=set_bot_state,
        send_telegram=send_telegram,
        init_db_func=init_db,
        execute_exit=_execute_exit,
        broker=broker,
        get_main_loop=lambda: _main_loop,
        get_regime_detector=lambda: regime_detector,
        admin_secret=ADMIN_SECRET,
        starting_cash=STARTING_CASH,
        max_open_positions=MAX_OPEN_POSITIONS,
        max_hold_time_sec=MAX_HOLD_TIME_SEC,
        max_positions_per_sector=MAX_POSITIONS_PER_SECTOR,
        version_string="V39",
    )

    host = os.getenv('FLASK_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_PORT', '10000'))
    threading.Thread(target=lambda: app.run(host=host, port=port, use_reloader=False),
                      daemon=True).start()

    btc_daily = await binance.fetch_ohlcv('BTC/USDT', '1d', limit=100)
    df_btc = pd.DataFrame(btc_daily, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
    regime = await regime_detector.detect(df_btc)
    btc_atr = calculate_atr(df_btc)
    btc_atr_avg = btc_atr.rolling(20).mean().iloc[-1]
    regime_detector.btc_atr_ratio = btc_atr.iloc[-1] / btc_atr_avg if btc_atr_avg > 0 else 1.0
    await sector_tracker.update(binance)
    regime_detector.leading_sector = sector_tracker.leader

    send_telegram(
        f"⚙️ *V39 DEPLOYED* [{broker.mode}]\n"
        f"Regime `{regime}` | ATR ratio `{regime_detector.btc_atr_ratio:.2f}` | "
        f"Fear `{regime_detector.fear_index}`\n"
        f"Sector `{sector_tracker.leader}`\n"
        f"Reconcile: `{'BLOCKED' if is_reconcile_blocked() else 'CLEAN'}`\n"
        f"Strategies: BTC Range, Sector Mom (scored), Squeeze, Mean Reversion, Trend Following\n"
        f"V39 changes: SL `{ATR_SL_MULTIPLIER}×ATR`, TP `{ATR_TP_MULTIPLIER}×ATR`, "
        f"max `{MAX_POSITIONS_PER_SECTOR}` per sector")

    await execute_heartbeat(regime_detector, sector_tracker)

    # STARTUP: Sweep any already-aged positions BEFORE loops start.
    # If the bot crashed or was paused for hours, positions could be over MAX_HOLD_TIME.
    aged_trades = db_query('SELECT * FROM trades', fetchall=True) or []
    aged_to_exit = []
    for t in aged_trades:
        age = time.time() - t['entry_time']
        if age > MAX_HOLD_TIME_SEC:
            aged_to_exit.append((t, age))
    if aged_to_exit:
        logger.warning(f"Startup found {len(aged_to_exit)} aged positions — exiting now")
        send_telegram(f"🕐 *STARTUP SWEEP*\nFound {len(aged_to_exit)} aged position(s). Force-exiting...")
        for t, age in aged_to_exit:
            try:
                price = await broker.get_current_price(t['symbol'])
                if price is None:
                    logger.error(f"Startup sweep: cannot price {t['symbol']} — leaving for exit_checker")
                    continue
                await _execute_exit(t, "STARTUP_TIME_STOP", price, regime_detector)
            except Exception as e:
                logger.error(f"Startup sweep {t['symbol']}: {e}", exc_info=True)

    tasks = [
        asyncio.create_task(heartbeat_loop(regime_detector, sector_tracker)),
        asyncio.create_task(position_manager(regime_detector)),
        asyncio.create_task(exit_checker(regime_detector)),
        asyncio.create_task(scanner_loop(regime_detector, sector_tracker)),
    ]
    await shutdown_event.wait()
    await binance.close()
    conn.close()
    for t in tasks: t.cancel()
    logger.info("Shutdown complete.")


if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

