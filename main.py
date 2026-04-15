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
from dotenv import load_dotenv

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mplfinance as mpf
from flask import Flask, jsonify, request as flask_request

load_dotenv()

# --- 1. CONFIGURATION & LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SniperBot")

BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')

if not all([BOT_TOKEN, CHAT_ID, API_KEY, API_SECRET]):
    logger.error("CRITICAL: Missing credentials in .env file! Halting bot.")
    sys.exit(1)

DB_FILE = "trading_state.db"
STARTING_CASH = 1000.0
MIN_ORDER_USD = 12.0
COOLDOWN_SECONDS = 14400
MAX_DAILY_DRAWDOWN = 0.04
FEE_SLIPPAGE_RATE = 0.004

MAX_OPEN_POSITIONS = 4
MAX_HOLD_TIME_SEC = 48 * 3600

ATR_SL_MULTIPLIER = 1.2
ATR_TP_MULTIPLIER = 2.5

TRAILING_ATR_WIDE = 1.5
TRAILING_ATR_TIGHT = 1.0
TRAILING_TIGHTEN_PCT = 0.03

PARTIAL_EXIT_ATR_MULT = 1.5
PARTIAL_EXIT_PCT = 0.40

REQUIRE_CONFIRMATION_CANDLE = True
PENDING_SIGNAL_EXPIRY_SEC = 5 * 3600
MAX_RECENT_PUMP_PCT = 0.30

BTC_ATR_SPIKE_MULTIPLIER = 2.0

REGIME_ADX_TRENDING = 25
REGIME_RANGE_LOOKBACK = 30
REGIME_REDETECT_HOURS = 12          # FIX #4: Only re-detect regime every 12h
REGIME_REDETECT_PRICE_MOVE = 0.03   # ...or if BTC moved 3% since last check

SECTOR_TOKENS = {
    'AI':      ['FET/USDT', 'RENDER/USDT', 'TAO/USDT', 'AGIX/USDT', 'OCEAN/USDT', 'AI16Z/USDT'],
    'L1':      ['SOL/USDT', 'AVAX/USDT', 'NEAR/USDT', 'SUI/USDT', 'APT/USDT', 'SEI/USDT'],
    'DEFI':    ['UNI/USDT', 'AAVE/USDT', 'MKR/USDT', 'CRV/USDT', 'DYDX/USDT', 'SNX/USDT'],
    'MEME':    ['DOGE/USDT', 'SHIB/USDT', 'PEPE/USDT', 'WIF/USDT', 'BONK/USDT', 'FLOKI/USDT'],
    'PRIVACY': ['XMR/USDT', 'ZEC/USDT', 'DASH/USDT', 'SCRT/USDT'],
}
MAX_SECTOR_SCAN_SYMBOLS = 10

ACCUMULATION_BUY_USD = 25.0
ACCUMULATION_FEAR_THRESHOLD = 15
ACCUMULATION_MAX_ALLOC_PCT = 0.30
ACCUMULATION_PROFIT_EXIT_PCT = 0.20   # FIX #6: Sell accumulation at 20% profit
ACCUMULATION_EMA200_EXIT = True        # FIX #6: Or when BTC reclaims 200 EMA

SIGNAL_DISABLE_WIN_RATE = 0.30
SIGNAL_EARLY_DISABLE_RATE = 0.25      # FIX #11: Forgiving threshold for first 30 trades
SIGNAL_MIN_TRADES_FOR_EVAL = 20       # FIX #11: Require 20 trades, not 10
SIGNAL_EARLY_TRADE_COUNT = 30         # FIX #11: "early" period

CORRELATION_THRESHOLD = 0.85
CORRELATION_LOOKBACK = 14

IGNORE_BASES = ('USDC', 'FDUSD', 'TUSD', 'BUSD', 'USDP', 'DAI', 'EUR', 'TRY', 'GBP', 'AEUR', 'USDE', 'RUB')

# --- Caching intervals (FIX #8) ---
SECTOR_CACHE_HOURS = 4
FUNDING_CACHE_SECONDS = 3600
DAILY_CANDLE_CACHE_SECONDS = 3600

chart_lock = threading.Lock()
db_lock = threading.Lock()
entry_lock = asyncio.Lock()
app = Flask(__name__)

# Async Binance client (WebSocket + REST)
binance = ccxtpro.binance({
    'apiKey': API_KEY,
    'secret': API_SECRET,
    'enableRateLimit': True,
    'options': {'defaultType': 'spot'}
})

# FIX #2: Single reusable sync futures client for funding rates
binance_futures_sync = ccxt.binance({
    'apiKey': API_KEY,
    'secret': API_SECRET,
    'options': {'defaultType': 'future'},
    'enableRateLimit': True,
})

# FIX #8: Global caches
_funding_cache = {}        # {symbol: (rate, timestamp)}
_daily_candle_cache = {}   # {symbol: (df, timestamp)}
_taker_ratio_cache = {}    # {symbol: (ratio, timestamp)}


# --- 2. SQLITE DATABASE ---
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA busy_timeout=5000")


def init_db():
    with db_lock:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS portfolio 
                     (id INTEGER PRIMARY KEY, cash REAL, wins INTEGER, losses INTEGER, 
                      current_day TEXT, daily_start_equity REAL, lockout_until REAL)''')

        # FIX #3: original_cost to track full deployed capital
        # FIX #9: entry_range_low/high to freeze range at entry time
        c.execute('''CREATE TABLE IF NOT EXISTS trades 
                     (symbol TEXT PRIMARY KEY, entry REAL, tp REAL, sl REAL, 
                      cost REAL, amount REAL, highest_price REAL, entry_time REAL, atr REAL,
                      original_sl REAL, signal_type TEXT, partial_exited INTEGER DEFAULT 0,
                      remaining_cost REAL DEFAULT 0, original_cost REAL DEFAULT 0,
                      entry_range_low REAL DEFAULT 0, entry_range_high REAL DEFAULT 0)''')

        c.execute('''CREATE TABLE IF NOT EXISTS cooldowns 
                     (symbol TEXT PRIMARY KEY, cooldown_until REAL)''')

        c.execute('''CREATE TABLE IF NOT EXISTS trade_history
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, signal_type TEXT,
                      entry REAL, exit_price REAL, pnl_pct REAL, cost REAL,
                      entry_time REAL, exit_time REAL, exit_reason TEXT,
                      original_cost REAL DEFAULT 0)''')

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
                      amount REAL, cost REAL, entry_price REAL, entry_time REAL)''')

        # FIX #7: Event calendar with seed data
        c.execute('''CREATE TABLE IF NOT EXISTS event_calendar
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, event_name TEXT,
                      event_date TEXT, impact TEXT DEFAULT 'HIGH')''')

        c.execute('''CREATE TABLE IF NOT EXISTS signal_status
                     (signal_type TEXT PRIMARY KEY, enabled INTEGER DEFAULT 1,
                      disabled_at REAL DEFAULT 0)''')

        c.execute('SELECT * FROM portfolio WHERE id=1')
        if not c.fetchone():
            today = datetime.datetime.now().strftime('%Y-%m-%d')
            c.execute('INSERT INTO portfolio VALUES (1, ?, 0, 0, ?, ?, 0.0)',
                      (STARTING_CASH, today, STARTING_CASH))

        for sig in ['SQUEEZE BREAKOUT', 'SECTOR MOMENTUM', 'BTC RANGE BUY']:
            c.execute('INSERT OR IGNORE INTO signal_status (signal_type) VALUES (?)', (sig,))

        # FIX #7: Seed known event dates if table is empty
        c.execute('SELECT COUNT(*) FROM event_calendar')
        if c.fetchone()[0] == 0:
            _seed_event_calendar(c)

        conn.commit()


def _seed_event_calendar(cursor):
    """FIX #7: Pre-populate known high-impact dates."""
    # FOMC meetings 2026 (approximate)
    fomc_dates = [
        ('2026-01-28', 'FOMC Decision'), ('2026-03-18', 'FOMC Decision'),
        ('2026-05-06', 'FOMC Decision'), ('2026-06-17', 'FOMC Decision'),
        ('2026-07-29', 'FOMC Decision'), ('2026-09-16', 'FOMC Decision'),
        ('2026-11-04', 'FOMC Decision'), ('2026-12-16', 'FOMC Decision'),
    ]
    # CPI release dates 2026 (approximate, usually 2nd week of month)
    cpi_dates = [
        (f'2026-{m:02d}-{d}', 'US CPI Release')
        for m, d in [(1, '14'), (2, '11'), (3, '11'), (4, '10'), (5, '13'),
                     (6, '10'), (7, '15'), (8, '12'), (9, '10'), (10, '14'),
                     (11, '12'), (12, '10')]
    ]
    # Crypto-specific catalysts
    crypto_dates = [
        ('2026-04-15', 'CLARITY Act Markup (Expected)'),
        ('2026-06-15', 'ETH Glamsterdam Upgrade (Expected)'),
    ]
    all_events = [(d, n, 'HIGH') for d, n in fomc_dates + cpi_dates + crypto_dates]
    cursor.executemany(
        'INSERT INTO event_calendar (event_date, event_name, impact) VALUES (?, ?, ?)',
        all_events)
    logger.info(f"Seeded {len(all_events)} events into calendar.")


init_db()


def db_query(query, args=(), fetchone=False, fetchall=False, commit=False):
    with db_lock:
        c = conn.cursor()
        try:
            c.execute(query, args)
            if commit:
                conn.commit()
            if fetchone:
                row = c.fetchone()
                return dict(row) if row else None
            if fetchall:
                return [dict(row) for row in c.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"DB Error [{query[:80]}]: {e}")
            if fetchall:
                return []
            return None


def get_portfolio():
    return db_query('SELECT * FROM portfolio WHERE id=1', fetchone=True)


def check_daily_reset():
    p = get_portfolio()
    if not p:
        return
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    if p['current_day'] != today_str:
        trades = db_query('SELECT cost FROM trades', fetchall=True)
        equity = p['cash'] + sum(t['cost'] for t in trades) if trades else p['cash']
        db_query('UPDATE portfolio SET current_day = ?, daily_start_equity = ? WHERE id=1',
                 (today_str, equity), commit=True)


# --- 3. MATH UTILS ---

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()


def calculate_adx(df, period=14):
    """FIX #22 from V33: Clean ADX, no dead code."""
    plus_dm_raw = df['high'].diff()
    minus_dm_raw = -df['low'].diff()
    plus_dm = plus_dm_raw.where((plus_dm_raw > minus_dm_raw) & (plus_dm_raw > 0), 0.0)
    minus_dm = minus_dm_raw.where((minus_dm_raw > plus_dm_raw) & (minus_dm_raw > 0), 0.0)
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr_series = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr_smooth = tr_series.rolling(window=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr_smooth)
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr_smooth)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    return dx.rolling(window=period).mean()


def calculate_recent_pump(df, lookback_candles=18):
    if len(df) < lookback_candles + 1:
        return 0.0
    recent_low = df['low'].iloc[-lookback_candles:].min()
    current = df['close'].iloc[-1]
    return (current - recent_low) / recent_low if recent_low > 0 else 0.0


def calculate_correlation(prices_a, prices_b, lookback=14):
    if len(prices_a) < lookback or len(prices_b) < lookback:
        return 0.0
    returns_a = prices_a[-lookback:].pct_change().dropna()
    returns_b = prices_b[-lookback:].pct_change().dropna()
    if len(returns_a) < 5 or len(returns_b) < 5:
        return 0.0
    min_len = min(len(returns_a), len(returns_b))
    corr = returns_a.iloc[-min_len:].reset_index(drop=True).corr(
        returns_b.iloc[-min_len:].reset_index(drop=True))
    return corr if not np.isnan(corr) else 0.0


def find_swing_levels(df, lookback=30, swing_window=2):
    """FIX #5: Proper swing high/low detection instead of rolling median."""
    recent = df.tail(lookback)
    swing_highs = []
    swing_lows = []

    for i in range(swing_window, len(recent) - swing_window):
        window_highs = recent['high'].iloc[i - swing_window:i + swing_window + 1]
        window_lows = recent['low'].iloc[i - swing_window:i + swing_window + 1]

        if recent['high'].iloc[i] == window_highs.max():
            swing_highs.append(recent['high'].iloc[i])
        if recent['low'].iloc[i] == window_lows.min():
            swing_lows.append(recent['low'].iloc[i])

    if not swing_highs:
        swing_highs = [recent['high'].max()]
    if not swing_lows:
        swing_lows = [recent['low'].min()]

    # Use the most recent cluster: average of top 3 swing highs/lows
    range_high = np.mean(sorted(swing_highs, reverse=True)[:3])
    range_low = np.mean(sorted(swing_lows)[:3])

    return range_low, range_high


# --- 4. REGIME DETECTOR (FIX #4, #5, #10) ---

class RegimeDetector:
    def __init__(self):
        self.current_regime = 'RANGE'
        self.range_low = 0.0
        self.range_high = 0.0
        self.btc_adx = 0.0
        self.fear_index = 50
        self.leading_sector = 'NONE'
        self._last_detect_time = 0.0
        self._last_detect_price = 0.0
        # FIX #10: Fixed reference range from last regime change
        self._regime_change_range_low = 0.0
        self._regime_change_range_high = 0.0

    def needs_redetection(self, current_btc_price):
        """FIX #4: Only re-detect if enough time passed or price moved significantly."""
        elapsed = time.time() - self._last_detect_time
        if elapsed > REGIME_REDETECT_HOURS * 3600:
            return True
        if self._last_detect_price > 0:
            price_move = abs(current_btc_price - self._last_detect_price) / self._last_detect_price
            if price_move >= REGIME_REDETECT_PRICE_MOVE:
                return True
        return False

    async def detect(self, df_btc_daily):
        df = df_btc_daily.copy()
        df['adx'] = calculate_adx(df)
        df['ema50'] = df['close'].ewm(span=50).mean()
        df['atr'] = calculate_atr(df)

        last = df.iloc[-1]
        self.btc_adx = last['adx'] if not np.isnan(last['adx']) else 0

        # FIX #5: Proper swing point detection
        self.range_low, self.range_high = find_swing_levels(df, REGIME_RANGE_LOOKBACK)

        price = last['close']
        ema50 = last['ema50']

        old_regime = self.current_regime

        # FIX #10: Use fixed reference for DEFENSIVE check
        defensive_ref_low = self._regime_change_range_low if self._regime_change_range_low > 0 else self.range_low

        if price < defensive_ref_low * 0.97 or price < ema50 * 0.95:
            self.current_regime = 'DEFENSIVE'
        elif self.btc_adx >= REGIME_ADX_TRENDING and price > self.range_high:
            self.current_regime = 'TRENDING'
        else:
            self.current_regime = 'RANGE'

        # FIX #10: Lock range on regime change
        if self.current_regime != old_regime:
            self._regime_change_range_low = self.range_low
            self._regime_change_range_high = self.range_high
            logger.info(f"REGIME CHANGE: {old_regime} → {self.current_regime} | "
                        f"Locked range: ${round(self.range_low, 0)}-${round(self.range_high, 0)}")

        await self._fetch_fear_index()

        self._last_detect_time = time.time()
        self._last_detect_price = price

        await asyncio.to_thread(db_query,
            '''INSERT INTO regime_history (regime, btc_adx, btc_price, range_low, range_high, 
               fear_index, leading_sector, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (self.current_regime, round(self.btc_adx, 1), round(price, 2),
             round(self.range_low, 2), round(self.range_high, 2),
             self.fear_index, self.leading_sector, time.time()), commit=True)

        logger.info(f"Regime: {self.current_regime} | ADX: {round(self.btc_adx, 1)} | "
                    f"Range: ${round(self.range_low, 0)}-${round(self.range_high, 0)} | "
                    f"Fear: {self.fear_index}")

        return self.current_regime

    async def _fetch_fear_index(self):
        try:
            def _fetch():
                resp = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
                data = resp.json()
                return int(data['data'][0]['value'])
            self.fear_index = await asyncio.to_thread(_fetch)
        except Exception as e:
            logger.warning(f"Fear & Greed fetch failed: {e}")

    def is_near_range_support(self, price, tolerance_pct=0.02):
        return price <= self.range_low * (1 + tolerance_pct)

    def is_near_range_resistance(self, price, tolerance_pct=0.02):
        return price >= self.range_high * (1 - tolerance_pct)

    def range_broke_down_for_trade(self, curr_price, trade_range_low):
        """FIX #9: Use trade's frozen range, not current."""
        if trade_range_low <= 0:
            return curr_price < self.range_low * 0.98
        return curr_price < trade_range_low * 0.98


# --- 5. SECTOR TRACKER (FIX #8: Caching) ---

class SectorTracker:
    def __init__(self):
        self.sector_performance = {}
        self.leader = 'NONE'
        self._last_update_time = 0.0

    def needs_update(self):
        """FIX #8: Only update every SECTOR_CACHE_HOURS."""
        return time.time() - self._last_update_time > SECTOR_CACHE_HOURS * 3600

    async def update(self, binance_client):
        if not self.needs_update():
            return
        try:
            btc_bars = await binance_client.fetch_ohlcv('BTC/USDT', '1d', limit=8)
            btc_df = pd.DataFrame(btc_bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            btc_7d_return = (btc_df['close'].iloc[-1] - btc_df['close'].iloc[-7]) / btc_df['close'].iloc[-7]

            self.sector_performance = {}
            for sector, tokens in SECTOR_TOKENS.items():
                returns = []
                for token in tokens:
                    try:
                        if token not in binance_client.markets:
                            continue
                        bars = await binance_client.fetch_ohlcv(token, '1d', limit=8)
                        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                        if len(df) >= 7:
                            ret = (df['close'].iloc[-1] - df['close'].iloc[-7]) / df['close'].iloc[-7]
                            returns.append(ret)
                    except Exception:
                        continue
                if returns:
                    avg_return = sum(returns) / len(returns)
                    self.sector_performance[sector] = round((avg_return - btc_7d_return) * 100, 2)
                else:
                    self.sector_performance[sector] = 0.0

            positive = {k: v for k, v in self.sector_performance.items() if v > 0}
            self.leader = max(positive, key=positive.get) if positive else 'NONE'
            self._last_update_time = time.time()
            logger.info(f"Sectors: {self.sector_performance} | Leader: {self.leader}")
        except Exception as e:
            logger.error(f"Sector tracking error: {e}")

    def get_scan_symbols(self):
        if self.leader == 'NONE' or self.leader not in SECTOR_TOKENS:
            return []
        return SECTOR_TOKENS[self.leader][:MAX_SECTOR_SCAN_SYMBOLS]


# --- 6. CACHED DATA FETCHERS (FIX #2, #8) ---

async def fetch_funding_rate(symbol):
    """FIX #2: Reuse module-level futures client. FIX #8: Cache results."""
    now = time.time()
    if symbol in _funding_cache:
        rate, ts = _funding_cache[symbol]
        if now - ts < FUNDING_CACHE_SECONDS:
            return rate
    try:
        perp_symbol = symbol.replace('/USDT', '/USDT:USDT')
        def _fetch():
            funding = binance_futures_sync.fetch_funding_rate(perp_symbol)
            return funding.get('fundingRate', 0)
        rate = await asyncio.to_thread(_fetch)
        rate = rate if rate is not None else 0
        _funding_cache[symbol] = (rate, now)
        return rate
    except Exception:
        _funding_cache[symbol] = (0, now)
        return 0


async def fetch_taker_buy_ratio(symbol):
    """FIX #8: Cached taker ratio."""
    now = time.time()
    if symbol in _taker_ratio_cache:
        ratio, ts = _taker_ratio_cache[symbol]
        if now - ts < FUNDING_CACHE_SECONDS:
            return ratio
    try:
        raw_symbol = symbol.replace('/', '')
        def _fetch():
            resp = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={'symbol': raw_symbol, 'interval': '4h', 'limit': 1}, timeout=10)
            data = resp.json()
            if data and len(data[0]) >= 10:
                total = float(data[0][5])
                buy = float(data[0][9])
                return buy / total if total > 0 else 0.5
            return 0.5
        ratio = await asyncio.to_thread(_fetch)
        _taker_ratio_cache[symbol] = (ratio, now)
        return ratio
    except Exception:
        return 0.5


async def fetch_daily_candles_cached(symbol):
    """FIX #8: Cached daily candles to avoid redundant API calls."""
    now = time.time()
    if symbol in _daily_candle_cache:
        df, ts = _daily_candle_cache[symbol]
        if now - ts < DAILY_CANDLE_CACHE_SECONDS:
            return df
    try:
        bars = await binance.fetch_ohlcv(symbol, '1d', limit=30)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        _daily_candle_cache[symbol] = (df, now)
        return df
    except Exception:
        return None


# --- 7. CHART & TELEGRAM ---

def generate_chart(df, symbol, title):
    with chart_lock:
        try:
            df_c = df.copy().tail(40)
            df_c['ts'] = pd.to_datetime(df_c['ts'], unit='ms')
            df_c.set_index('ts', inplace=True)
            df_c.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                                 'close': 'Close', 'vol': 'Volume'}, inplace=True)
            ap = [
                mpf.make_addplot(df_c['ema20'], color='blue', width=1.2),
                mpf.make_addplot(df_c['ema50'], color='orange', width=1.2)
            ]
            if 'upper' in df_c.columns:
                ap.append(mpf.make_addplot(df_c['upper'], color='#4287f5', alpha=0.3))
                ap.append(mpf.make_addplot(df_c['lower'], color='#4287f5', alpha=0.3))
            mc = mpf.make_marketcolors(up='green', down='red', edge='inherit',
                                       wick='inherit', volume='in')
            s = mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mc,
                                    gridstyle='solid', gridcolor='#f0f0f0')
            buf = io.BytesIO()
            mpf.plot(df_c, type='candle', style=s, addplot=ap, volume=True,
                     savefig=dict(fname=buf, dpi=100), figsize=(8, 5),
                     title=f"\n{symbol} {title}")
            buf.seek(0)
            plt.close('all')
            return buf
        except Exception as e:
            logger.error(f"Chart Render Error: {e}")
            return None


def send_telegram(message, image_buf=None):
    photo_data = image_buf.getvalue() if image_buf else None
    def _send():
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/"
        for attempt in range(3):
            try:
                if photo_data:
                    resp = requests.post(url + "sendPhoto",
                        data={"chat_id": CHAT_ID, "caption": message, "parse_mode": "Markdown"},
                        files={'photo': ('chart.png', photo_data, 'image/png')}, timeout=20)
                else:
                    resp = requests.post(url + "sendMessage",
                        data={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=20)
                if resp.status_code == 200:
                    return
            except Exception as e:
                logger.warning(f"Telegram attempt {attempt+1}: {e}")
            if attempt < 2:
                time.sleep(2 ** (attempt + 1))
        try:
            with open("telegram_failed.log", "a") as f:
                f.write(f"[{datetime.datetime.now().isoformat()}] {message}\n")
        except Exception:
            pass
    threading.Thread(target=_send, daemon=True).start()


# --- 8. PERFORMANCE FEEDBACK (FIX #11) ---

async def check_signal_performance():
    for sig_type in ['SQUEEZE BREAKOUT', 'SECTOR MOMENTUM', 'BTC RANGE BUY']:
        history = await asyncio.to_thread(db_query,
            'SELECT pnl_pct FROM trade_history WHERE signal_type = ? ORDER BY id DESC LIMIT 40',
            (sig_type,), fetchall=True)

        # FIX #11: Require 20 trades minimum
        if len(history) < SIGNAL_MIN_TRADES_FOR_EVAL:
            continue

        recent = history[:20]
        wins = sum(1 for h in recent if h['pnl_pct'] > 0)
        win_rate = wins / len(recent)

        # FIX #11: Use forgiving threshold for early trades
        total_trades = len(history)
        threshold = SIGNAL_EARLY_DISABLE_RATE if total_trades < SIGNAL_EARLY_TRADE_COUNT else SIGNAL_DISABLE_WIN_RATE

        status = await asyncio.to_thread(db_query,
            'SELECT * FROM signal_status WHERE signal_type = ?', (sig_type,), fetchone=True)

        if win_rate < threshold:
            if status and status['enabled']:
                await asyncio.to_thread(db_query,
                    'UPDATE signal_status SET enabled = 0, disabled_at = ? WHERE signal_type = ?',
                    (time.time(), sig_type), commit=True)
                send_telegram(
                    f"⚠️ **SIGNAL DISABLED: {sig_type}**\n"
                    f"Win rate: `{round(win_rate*100,1)}%` < threshold `{round(threshold*100,1)}%`\n"
                    f"({total_trades} total trades, using {'early' if total_trades < SIGNAL_EARLY_TRADE_COUNT else 'standard'} threshold)")
        else:
            if status and not status['enabled']:
                await asyncio.to_thread(db_query,
                    'UPDATE signal_status SET enabled = 1 WHERE signal_type = ?',
                    (sig_type,), commit=True)
                send_telegram(f"✅ **RE-ENABLED: {sig_type}** | Win rate: `{round(win_rate*100,1)}%`")


async def is_signal_enabled(signal_type):
    status = await asyncio.to_thread(db_query,
        'SELECT enabled FROM signal_status WHERE signal_type = ?',
        (signal_type,), fetchone=True)
    return status['enabled'] if status else True


# --- 9. EVENT CALENDAR (FIX #7) ---

async def is_event_day():
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    event = await asyncio.to_thread(db_query,
        'SELECT * FROM event_calendar WHERE event_date IN (?, ?) AND impact = ?',
        (today, tomorrow, 'HIGH'), fetchone=True)
    return event is not None


# --- 10. STRATEGIES ---

async def strategy_btc_range(regime_detector, df_btc_4h, btc_daily_df):
    if not await is_signal_enabled('BTC RANGE BUY'):
        return
    df = df_btc_4h.copy()
    df['rsi'] = calculate_rsi(df['close'])
    df['atr'] = calculate_atr(df)
    df['ema20'] = df['close'].ewm(span=20).mean()

    last = df.iloc[-2]
    confirm = df.iloc[-1]

    existing = await asyncio.to_thread(db_query,
        'SELECT * FROM trades WHERE symbol = ?', ('BTC/USDT',), fetchone=True)
    if existing:
        return

    pending = await asyncio.to_thread(db_query,
        'SELECT * FROM pending_signals WHERE symbol = ?', ('BTC/USDT',), fetchone=True)
    if pending:
        if (confirm['close'] > pending['trigger_price']
                and confirm['close'] > confirm['open']
                and regime_detector.is_near_range_support(confirm['close'], 0.03)):
            await asyncio.to_thread(db_query,
                'DELETE FROM pending_signals WHERE symbol = ?', ('BTC/USDT',), commit=True)
            await execute_entry_btc_range(df, confirm, pending['atr'], regime_detector)
        elif time.time() - pending['created_at'] > PENDING_SIGNAL_EXPIRY_SEC:
            await asyncio.to_thread(db_query,
                'DELETE FROM pending_signals WHERE symbol = ?', ('BTC/USDT',), commit=True)
        return

    if (regime_detector.is_near_range_support(last['close'])
            and last['rsi'] < 40 and last['close'] > last['open']):
        taker_ratio = await fetch_taker_buy_ratio('BTC/USDT')
        if taker_ratio < 0.48:
            return
        if REQUIRE_CONFIRMATION_CANDLE:
            await asyncio.to_thread(db_query,
                '''INSERT OR REPLACE INTO pending_signals 
                   (symbol, signal_type, trigger_price, atr, created_at, candle_ts, ema20, bb_lower, vol_ratio)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                ('BTC/USDT', 'BTC RANGE BUY', last['close'], last['atr'],
                 time.time(), last['ts'], last['ema20'], 0, taker_ratio), commit=True)
        else:
            await execute_entry_btc_range(df, confirm, last['atr'], regime_detector)


async def execute_entry_btc_range(df, candle_data, atr_value, regime_detector):
    async with entry_lock:
        try:
            real_entry = candle_data['close'] * 1.001
            sl = regime_detector.range_low * 0.98
            tp = regime_detector.range_high * 0.99

            p = await asyncio.to_thread(get_portfolio)
            active = await asyncio.to_thread(db_query,
                'SELECT COUNT(*) as c FROM trades', fetchone=True)
            if active and active['c'] >= MAX_OPEN_POSITIONS:
                return

            pos_size = min(p['cash'] * 0.40, p['cash'] - MIN_ORDER_USD)
            if pos_size < MIN_ORDER_USD:
                return

            raw_amount = pos_size / real_entry
            try:
                amount_str = binance.amount_to_precision('BTC/USDT', raw_amount)
                amount = float(amount_str)
            except Exception:
                amount_str = str(raw_amount)
                amount = raw_amount

            actual_cost = amount * real_entry
            if amount <= 0 or p['cash'] < actual_cost:
                return

            # FIX #3: Store original_cost. FIX #9: Store entry-time range.
            await asyncio.to_thread(db_query, '''
                INSERT INTO trades (symbol, entry, tp, sl, cost, amount, highest_price,
                    entry_time, atr, original_sl, signal_type, partial_exited, remaining_cost,
                    original_cost, entry_range_low, entry_range_high)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?)''',
                ('BTC/USDT', real_entry, tp, sl, actual_cost, amount, real_entry,
                 time.time(), atr_value, sl, 'BTC RANGE BUY',
                 actual_cost, regime_detector.range_low, regime_detector.range_high), commit=True)

            await asyncio.to_thread(db_query,
                'UPDATE portfolio SET cash = cash - ? WHERE id=1', (actual_cost,), commit=True)

            rr = round((tp - real_entry) / (real_entry - sl), 1) if (real_entry - sl) > 0 else 0
            msg = (f"💰 **BTC RANGE BUY**\n"
                   f"Entry: `${round(real_entry, 2)}` | Size: `${round(actual_cost, 2)}`\n"
                   f"TP: `${round(tp, 2)}` | SL: `${round(sl, 2)}` | R:R: `{rr}:1`")
            chart_buf = await asyncio.to_thread(generate_chart, df, "BTC/USDT", "RANGE BUY")
            send_telegram(msg, chart_buf)
        except Exception as e:
            logger.error(f"BTC range entry error: {e}", exc_info=True)


async def strategy_sector_momentum(sector_tracker, btc_atr_ratio):
    if sector_tracker.leader == 'NONE':
        return
    if not await is_signal_enabled('SECTOR MOMENTUM'):
        return

    scan_symbols = [s for s in sector_tracker.get_scan_symbols() if s in binance.markets]
    if not scan_symbols:
        return

    active_data = await asyncio.to_thread(db_query, 'SELECT symbol FROM trades', fetchall=True)
    active = [r['symbol'] for r in active_data] if active_data else []
    cooldowns_data = await asyncio.to_thread(db_query, 'SELECT * FROM cooldowns', fetchall=True)
    cooldowns = {r['symbol']: r['cooldown_until'] for r in cooldowns_data} if cooldowns_data else {}

    to_scan = [s for s in scan_symbols
               if s not in active and (s not in cooldowns or time.time() >= cooldowns[s])]

    for s in to_scan:
        ct = await asyncio.to_thread(db_query, 'SELECT COUNT(*) as c FROM trades', fetchone=True)
        if ct and ct['c'] >= MAX_OPEN_POSITIONS:
            break
        await process_sector_symbol(s, btc_atr_ratio, active)
        await asyncio.sleep(0.3)


async def process_sector_symbol(s, btc_atr_ratio, active_symbols):
    # FIX #1: Initialize df_daily to None at top of function
    df_daily = None
    try:
        bars = await binance.fetch_ohlcv(s, '4h', limit=500)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        df['ema20'] = df['close'].ewm(span=20).mean()
        df['ema50'] = df['close'].ewm(span=50).mean()
        df['rsi'] = calculate_rsi(df['close'])
        df['atr'] = calculate_atr(df)
        df['vol_ma'] = df['vol'].rolling(20).mean()
        df['adx'] = calculate_adx(df)
        df['ema20_slope'] = df['ema20'].pct_change(periods=3)

        last = df.iloc[-2]
        confirm = df.iloc[-1]

        pending = await asyncio.to_thread(db_query,
            'SELECT * FROM pending_signals WHERE symbol = ?', (s,), fetchone=True)
        if pending:
            if pending['signal_type'] == 'SECTOR MOMENTUM':
                if confirm['close'] > pending['trigger_price'] and confirm['close'] > confirm['ema20']:
                    await asyncio.to_thread(db_query,
                        'DELETE FROM pending_signals WHERE symbol = ?', (s,), commit=True)
                    await execute_entry(s, 'SECTOR MOMENTUM', df, confirm, pending['atr'])
                elif time.time() - pending['created_at'] > PENDING_SIGNAL_EXPIRY_SEC:
                    await asyncio.to_thread(db_query,
                        'DELETE FROM pending_signals WHERE symbol = ?', (s,), commit=True)
            return

        if calculate_recent_pump(df) > MAX_RECENT_PUMP_PCT:
            return

        # FIX #8: Use cached daily candles
        df_daily = await fetch_daily_candles_cached(s)
        if df_daily is not None and len(df_daily) > 20:
            daily_ema20 = df_daily['close'].ewm(span=20).mean().iloc[-1]
            if df_daily.iloc[-1]['close'] < daily_ema20:
                return

        is_uptrend = last['ema20'] > last['ema50']
        is_rising = last['ema20_slope'] > 0.002
        touched_ema = last['low'] <= last['ema20']
        bounced = last['close'] > last['ema20'] * 1.003
        green_candle = last['close'] > last['open']
        good_rsi = 40 < last['rsi'] < 65
        good_volume = last['vol'] > last['vol_ma']
        has_trend = last['adx'] > 20

        if (is_uptrend and is_rising and touched_ema and bounced
                and green_candle and good_rsi and good_volume and has_trend):

            if btc_atr_ratio > BTC_ATR_SPIKE_MULTIPLIER:
                return

            base = s.split('/')[0]
            btc_pair = f"{base}/BTC"
            try:
                if btc_pair in binance.markets:
                    btc_bars = await binance.fetch_ohlcv(btc_pair, '4h', limit=50)
                    df_bp = pd.DataFrame(btc_bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                    df_bp['ema20'] = df_bp['close'].ewm(span=20).mean()
                    if df_bp.iloc[-1]['close'] <= df_bp.iloc[-1]['ema20']:
                        return
            except Exception:
                pass

            # FIX #8: Only fetch funding AFTER technical filters pass
            funding = await fetch_funding_rate(s)
            if funding > 0.0005:
                return

            # FIX #1: Use properly scoped df_daily
            corr_rejected = False
            for active_sym in active_symbols:
                try:
                    other_df = await fetch_daily_candles_cached(active_sym)
                    if other_df is None:
                        continue
                    prices_a = df_daily['close'] if df_daily is not None else df['close']
                    corr = calculate_correlation(prices_a, other_df['close'], CORRELATION_LOOKBACK)
                    if corr > CORRELATION_THRESHOLD:
                        logger.info(f"Blocked {s}: correlated {corr:.2f} with {active_sym}")
                        corr_rejected = True
                        break
                except Exception:
                    continue
            if corr_rejected:
                return

            if REQUIRE_CONFIRMATION_CANDLE:
                await asyncio.to_thread(db_query,
                    '''INSERT OR REPLACE INTO pending_signals
                       (symbol, signal_type, trigger_price, atr, created_at, candle_ts, ema20, bb_lower, vol_ratio)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (s, 'SECTOR MOMENTUM', last['close'], last['atr'], time.time(),
                     last['ts'], last['ema20'], 0,
                     last['vol'] / last['vol_ma'] if last['vol_ma'] > 0 else 0), commit=True)
            else:
                await execute_entry(s, 'SECTOR MOMENTUM', df, confirm, last['atr'])

    except ccxt.BaseError as e:
        logger.warning(f"Sector error {s}: {e}")
    except Exception as e:
        logger.error(f"Sector error {s}: {e}")


async def strategy_squeeze_breakout(btc_atr_ratio):
    if not await is_signal_enabled('SQUEEZE BREAKOUT'):
        return

    # FIX #8: Fetch tickers once, pass down
    tickers = await binance.fetch_tickers()
    valid = {
        k: v for k, v in tickers.items()
        if k.endswith('/USDT') and 'BTC' not in k
        and k.split('/')[0] not in IGNORE_BASES
    }
    top_symbols = sorted(valid.keys(), key=lambda x: valid[x].get('quoteVolume', 0), reverse=True)[:75]

    active_data = await asyncio.to_thread(db_query, 'SELECT symbol FROM trades', fetchall=True)
    active = [r['symbol'] for r in active_data] if active_data else []
    cooldowns_data = await asyncio.to_thread(db_query, 'SELECT * FROM cooldowns', fetchall=True)
    cooldowns = {r['symbol']: r['cooldown_until'] for r in cooldowns_data} if cooldowns_data else {}

    symbols = [s for s in top_symbols
               if s not in active and (s not in cooldowns or time.time() >= cooldowns[s])]

    for i in range(0, len(symbols), 10):
        ct = await asyncio.to_thread(db_query, 'SELECT COUNT(*) as c FROM trades', fetchone=True)
        if ct and ct['c'] >= MAX_OPEN_POSITIONS:
            break
        batch = symbols[i:i + 10]
        tasks = [process_squeeze_symbol(s, btc_atr_ratio, active) for s in batch]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.5)


async def process_squeeze_symbol(s, btc_atr_ratio, active_symbols):
    try:
        bars = await binance.fetch_ohlcv(s, '4h', limit=500)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        df['ema20'] = df['close'].ewm(span=20).mean()
        df['ema50'] = df['close'].ewm(span=50).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        df['std'] = df['close'].rolling(20).std()
        df['upper'] = df['ma20'] + (df['std'] * 2)
        df['lower'] = df['ma20'] - (df['std'] * 2)
        df['rsi'] = calculate_rsi(df['close'])
        df['atr'] = calculate_atr(df)
        df['vol_ma'] = df['vol'].rolling(20).mean()
        df['adx'] = calculate_adx(df)
        df['bb_width'] = (df['upper'] - df['lower']) / df['ma20']
        df['bb_width_ma'] = df['bb_width'].rolling(20).mean()

        last = df.iloc[-2]
        confirm = df.iloc[-1]
        prev = df.iloc[-3]

        pending = await asyncio.to_thread(db_query,
            'SELECT * FROM pending_signals WHERE symbol = ?', (s,), fetchone=True)
        if pending:
            if pending['signal_type'] == 'SQUEEZE BREAKOUT':
                if confirm['close'] > confirm['ema20'] and confirm['low'] > pending['ema20'] * 0.995:
                    await asyncio.to_thread(db_query,
                        'DELETE FROM pending_signals WHERE symbol = ?', (s,), commit=True)
                    await execute_entry(s, 'SQUEEZE BREAKOUT', df, confirm, pending['atr'])
                elif time.time() - pending['created_at'] > PENDING_SIGNAL_EXPIRY_SEC:
                    await asyncio.to_thread(db_query,
                        'DELETE FROM pending_signals WHERE symbol = ?', (s,), commit=True)
            return

        if calculate_recent_pump(df) > MAX_RECENT_PUMP_PCT:
            return

        # FIX #8: Cached daily overlay
        df_d = await fetch_daily_candles_cached(s)
        if df_d is not None and len(df_d) > 20:
            d_ema20 = df_d['close'].ewm(span=20).mean().iloc[-1]
            if df_d.iloc[-1]['close'] < d_ema20:
                return

        is_squeeze = prev['bb_width'] < prev['bb_width_ma']
        is_high_vol = last['vol'] > (last['vol_ma'] * 2.0)
        has_trend = last['adx'] > REGIME_ADX_TRENDING

        if (last['close'] > df['ema20'].iloc[-2]
                and prev['close'] <= prev['ema20']
                and is_high_vol and is_squeeze and has_trend
                and last['ema20'] > last['ema50']):

            if btc_atr_ratio > BTC_ATR_SPIKE_MULTIPLIER:
                return

            # FIX #8: Funding/taker ONLY after technical filters pass
            funding = await fetch_funding_rate(s)
            if funding > 0.0005:
                return
            taker = await fetch_taker_buy_ratio(s)
            if taker < 0.50:
                return

            if REQUIRE_CONFIRMATION_CANDLE:
                await asyncio.to_thread(db_query,
                    '''INSERT OR REPLACE INTO pending_signals
                       (symbol, signal_type, trigger_price, atr, created_at, candle_ts, ema20, bb_lower, vol_ratio)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (s, 'SQUEEZE BREAKOUT', last['close'], last['atr'], time.time(),
                     last['ts'], last['ema20'], last['lower'], taker), commit=True)
            else:
                await execute_entry(s, 'SQUEEZE BREAKOUT', df, confirm, last['atr'])
    except Exception as e:
        logger.warning(f"Squeeze error {s}: {e}")


# --- 11. ENTRY EXECUTION (altcoins) ---

async def execute_entry(s, signal, df, candle_data, atr_value):
    async with entry_lock:
        try:
            real_entry = candle_data['close'] * 1.002
            sl = real_entry - (atr_value * ATR_SL_MULTIPLIER)
            tp = real_entry + (atr_value * ATR_TP_MULTIPLIER)

            p = await asyncio.to_thread(get_portfolio)
            sl_pct = (real_entry - sl) / real_entry
            if sl_pct <= 0:
                return

            pos_size = min(p['cash'] * 0.02 / sl_pct, p['cash'] * 0.20)
            if pos_size < MIN_ORDER_USD:
                return

            raw_amount = pos_size / real_entry
            try:
                amount_str = binance.amount_to_precision(s, raw_amount)
                amount = float(amount_str)
            except Exception:
                amount_str = str(raw_amount)
                amount = raw_amount

            actual_cost = amount * real_entry
            if amount <= 0:
                return

            cur_p = await asyncio.to_thread(get_portfolio)
            ct = await asyncio.to_thread(db_query, 'SELECT COUNT(*) as c FROM trades', fetchone=True)

            if ct and ct['c'] < MAX_OPEN_POSITIONS and cur_p['cash'] >= actual_cost:
                # FIX #3: original_cost. FIX #9: entry_range fields.
                await asyncio.to_thread(db_query, '''
                    INSERT INTO trades (symbol, entry, tp, sl, cost, amount, highest_price,
                        entry_time, atr, original_sl, signal_type, partial_exited, remaining_cost,
                        original_cost, entry_range_low, entry_range_high)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, 0)''',
                    (s, real_entry, tp, sl, actual_cost, amount, real_entry,
                     time.time(), atr_value, sl, signal, actual_cost), commit=True)

                await asyncio.to_thread(db_query,
                    'UPDATE portfolio SET cash = cash - ? WHERE id=1', (actual_cost,), commit=True)

                rr = round(ATR_TP_MULTIPLIER / ATR_SL_MULTIPLIER, 1)
                msg = (f"💰 **EXECUTED: {s}**\n"
                       f"⚡ `{signal}` | Entry: `{round(real_entry, 4)}`\n"
                       f"Size: `{amount_str}` (${round(actual_cost, 2)})\n"
                       f"TP: `{round(tp, 4)}` | SL: `{round(sl, 4)}` | R:R: `{rr}:1`")
                chart_buf = await asyncio.to_thread(generate_chart, df, s, signal)
                send_telegram(msg, chart_buf)
        except Exception as e:
            logger.error(f"Entry error {s}: {e}", exc_info=True)


# --- 12. POSITION MANAGER (FIX #3, #9, #16) ---

async def position_manager(regime_detector):
    dynamic_atr_cache = {}
    logger.info("Position Manager Initialized.")

    while True:
        try:
            trades = await asyncio.to_thread(db_query, 'SELECT * FROM trades', fetchall=True)
            if not trades:
                await asyncio.sleep(5)
                continue

            symbols = [t['symbol'] for t in trades]
            tickers = await binance.watch_tickers(symbols)
            p = await asyncio.to_thread(get_portfolio)
            cash = p['cash']
            now = time.time()

            for t in trades:
                s = t['symbol']
                if s not in tickers:
                    continue

                curr = tickers[s]['last']
                highest_price = max(t['highest_price'], curr)
                current_atr = t['atr']

                if s not in dynamic_atr_cache or (now - dynamic_atr_cache[s] > 3600):
                    try:
                        recent = await binance.fetch_ohlcv(s, '4h', limit=20)
                        df_a = pd.DataFrame(recent, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                        current_atr = calculate_atr(df_a).iloc[-1]
                        dynamic_atr_cache[s] = now
                        await asyncio.to_thread(db_query,
                            'UPDATE trades SET atr = ? WHERE symbol = ?', (current_atr, s), commit=True)
                    except Exception:
                        pass

                effective_pnl_pct = (((curr - t['entry']) / t['entry']) - FEE_SLIPPAGE_RATE) * 100
                unrealized_gain = (curr - t['entry']) / t['entry']

                # --- PARTIAL EXIT (FIX #3: correct accounting) ---
                if (not t['partial_exited']
                        and curr >= t['entry'] + (current_atr * PARTIAL_EXIT_ATR_MULT)):
                    partial_amount = t['amount'] * PARTIAL_EXIT_PCT
                    partial_value = partial_amount * curr
                    partial_pnl = (((curr - t['entry']) / t['entry']) - FEE_SLIPPAGE_RATE) * 100

                    remaining_amount = t['amount'] - partial_amount
                    # FIX #3: remaining_cost is proportional to remaining amount
                    remaining_cost = t['cost'] * (1 - PARTIAL_EXIT_PCT)

                    cash += partial_value
                    await asyncio.to_thread(db_query,
                        '''UPDATE trades SET partial_exited = 1, amount = ?, cost = ?,
                           remaining_cost = ? WHERE symbol = ?''',
                        (remaining_amount, remaining_cost, remaining_cost, s), commit=True)
                    await asyncio.to_thread(db_query,
                        'UPDATE portfolio SET cash = ? WHERE id=1', (cash,), commit=True)

                    be_sl = t['entry'] * (1 + FEE_SLIPPAGE_RATE + 0.001)
                    if be_sl > t['sl']:
                        await asyncio.to_thread(db_query,
                            'UPDATE trades SET sl = ? WHERE symbol = ?', (be_sl, s), commit=True)

                    send_telegram(
                        f"📤 **PARTIAL EXIT (40%): {s}**\n"
                        f"Sold at `{round(curr, 4)}` | PnL: `+{round(partial_pnl, 2)}%`\n"
                        f"Stop → breakeven `{round(be_sl, 4)}`")
                    continue

                # --- TRAILING STOP ---
                current_sl = t['sl']
                if highest_price >= t['entry'] * 1.01:
                    be_sl = t['entry'] * (1 + FEE_SLIPPAGE_RATE + 0.001)
                    trail = TRAILING_ATR_TIGHT if unrealized_gain >= TRAILING_TIGHTEN_PCT else TRAILING_ATR_WIDE
                    atr_trail = highest_price - (trail * current_atr)
                    proposed = max(t['sl'], be_sl, atr_trail)
                    if proposed > current_sl:
                        current_sl = proposed

                if current_sl > t['sl'] or highest_price > t['highest_price']:
                    await asyncio.to_thread(db_query,
                        'UPDATE trades SET sl = ?, highest_price = ? WHERE symbol = ?',
                        (current_sl, highest_price, s), commit=True)
                    if current_sl > t['sl']:
                        tt = "TIGHT" if unrealized_gain >= TRAILING_TIGHTEN_PCT else "WIDE"
                        send_telegram(f"🛡️ **Trail [{tt}]: {s}** | SL: `{round(current_sl, 4)}`")

                # --- EXIT CONDITIONS ---
                closed = False
                exit_reason = ""
                duration = now - t['entry_time']

                # FIX #9: Use trade's frozen range for breakdown check
                if (s == 'BTC/USDT' and t['signal_type'] == 'BTC RANGE BUY'
                        and regime_detector.range_broke_down_for_trade(curr, t.get('entry_range_low', 0))):
                    exit_reason = "RANGE_BREAKDOWN"
                    cash += (t['cost'] * (1 + (effective_pnl_pct / 100)))
                    await asyncio.to_thread(db_query,
                        'UPDATE portfolio SET losses = losses + 1 WHERE id=1', commit=True)
                    msg = (f"🚨 **RANGE BREAKDOWN: {s}**\n"
                           f"Exit: `{curr}` | PnL: `{round(effective_pnl_pct, 2)}%`")
                    closed = True

                elif duration > MAX_HOLD_TIME_SEC:
                    exit_reason = "TIME_STOP"
                    cash += (t['cost'] * (1 + (effective_pnl_pct / 100)))
                    key = 'wins' if effective_pnl_pct > 0 else 'losses'
                    await asyncio.to_thread(db_query,
                        f'UPDATE portfolio SET {key} = {key} + 1 WHERE id=1', commit=True)
                    msg = f"⏰ **TIME STOP: {s}** | PnL: `{round(effective_pnl_pct, 2)}%`"
                    closed = True

                elif curr >= t['tp']:
                    exit_reason = "TP_HIT"
                    cash += (t['cost'] * (1 + (effective_pnl_pct / 100)))
                    await asyncio.to_thread(db_query,
                        'UPDATE portfolio SET wins = wins + 1 WHERE id=1', commit=True)
                    msg = f"✅ **TP HIT: {s}** | PnL: `+{round(effective_pnl_pct, 2)}%`"
                    closed = True

                elif curr <= current_sl:
                    # FIX #16: Fee-adjusted win/loss
                    fee_entry = t['entry'] * (1 + FEE_SLIPPAGE_RATE)
                    if current_sl > fee_entry:
                        exit_reason = "TRAILING_PROFIT"
                        await asyncio.to_thread(db_query,
                            'UPDATE portfolio SET wins = wins + 1 WHERE id=1', commit=True)
                        emoji = "🟢"
                    else:
                        exit_reason = "SL_HIT"
                        await asyncio.to_thread(db_query,
                            'UPDATE portfolio SET losses = losses + 1 WHERE id=1', commit=True)
                        emoji = "🛑"
                    cash += (t['cost'] * (1 + (effective_pnl_pct / 100)))
                    msg = f"{emoji} **{exit_reason}: {s}** | PnL: `{round(effective_pnl_pct, 2)}%`"
                    closed = True

                if closed:
                    await asyncio.to_thread(db_query,
                        'UPDATE portfolio SET cash = ? WHERE id=1', (cash,), commit=True)
                    await asyncio.to_thread(db_query,
                        'INSERT OR REPLACE INTO cooldowns VALUES (?, ?)',
                        (s, now + COOLDOWN_SECONDS), commit=True)

                    # FIX #3: Log original_cost in trade history
                    original_cost = t.get('original_cost', t['cost'])
                    await asyncio.to_thread(db_query,
                        '''INSERT INTO trade_history 
                           (symbol, signal_type, entry, exit_price, pnl_pct, cost, 
                            entry_time, exit_time, exit_reason, original_cost)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (s, t.get('signal_type', 'UNKNOWN'), t['entry'], curr,
                         round(effective_pnl_pct, 2), t['cost'], t['entry_time'],
                         now, exit_reason, original_cost), commit=True)

                    await asyncio.to_thread(db_query,
                        'DELETE FROM trades WHERE symbol = ?', (s,), commit=True)

                    left = await asyncio.to_thread(db_query, 'SELECT cost FROM trades', fetchall=True)
                    equity = cash + sum(x['cost'] for x in left) if left else cash
                    dd = (p['daily_start_equity'] - equity) / p['daily_start_equity'] if p['daily_start_equity'] > 0 else 0
                    if dd >= MAX_DAILY_DRAWDOWN:
                        await asyncio.to_thread(db_query,
                            'UPDATE portfolio SET lockout_until = ? WHERE id=1',
                            (now + 86400,), commit=True)
                        send_telegram(f"🚨 **KILL SWITCH** | Drawdown: `-{dd*100:.2f}%` | Paused 24h.")
                    send_telegram(msg + f"\nCash: `${round(cash, 2)}`")

        except asyncio.CancelledError:
            break
        except ccxt.BaseError as e:
            logger.warning(f"PM API: {e}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"PM: {e}", exc_info=True)
            await asyncio.sleep(2)


# --- 13. ACCUMULATION (FIX #6: Exit logic) ---

async def run_accumulation(regime_detector):
    """Buy during extreme fear. Sell when BTC reclaims 200 EMA or 20%+ profit."""
    # --- EXIT CHECK FIRST ---
    accum_rows = await asyncio.to_thread(db_query,
        'SELECT * FROM accumulation', fetchall=True)

    if accum_rows:
        try:
            ticker = await binance.fetch_ticker('BTC/USDT')
            btc_price = ticker['last']

            # FIX #6: Check 200 EMA exit
            should_exit = False
            if ACCUMULATION_EMA200_EXIT:
                df_d = await fetch_daily_candles_cached('BTC/USDT')
                if df_d is not None and len(df_d) > 200:
                    ema200 = df_d['close'].ewm(span=200).mean().iloc[-1]
                    if btc_price > ema200:
                        should_exit = True
                        logger.info("Accumulation exit: BTC reclaimed 200 EMA")

            # FIX #6: Check profit exit
            total_cost = sum(r['cost'] for r in accum_rows)
            total_amount = sum(r['amount'] for r in accum_rows)
            current_value = total_amount * btc_price
            if total_cost > 0 and (current_value - total_cost) / total_cost >= ACCUMULATION_PROFIT_EXIT_PCT:
                should_exit = True
                logger.info(f"Accumulation exit: +{round((current_value/total_cost - 1)*100, 1)}% profit")

            if should_exit and accum_rows:
                profit = current_value - total_cost
                await asyncio.to_thread(db_query, 'DELETE FROM accumulation', commit=True)
                await asyncio.to_thread(db_query,
                    'UPDATE portfolio SET cash = cash + ? WHERE id=1', (current_value,), commit=True)
                send_telegram(
                    f"🏦 **ACCUMULATION SOLD**\n"
                    f"Total: `${round(current_value, 2)}` | Profit: `${round(profit, 2)}`\n"
                    f"Returned to trading capital.")
                return
        except Exception as e:
            logger.error(f"Accumulation exit check error: {e}")

    # --- BUY CHECK ---
    if regime_detector.fear_index > ACCUMULATION_FEAR_THRESHOLD:
        return

    p = await asyncio.to_thread(get_portfolio)
    accum = await asyncio.to_thread(db_query,
        'SELECT SUM(cost) as total FROM accumulation', fetchone=True)
    accum_total = accum['total'] if accum and accum['total'] else 0.0

    total_equity = p['cash'] + accum_total
    if accum_total >= total_equity * ACCUMULATION_MAX_ALLOC_PCT:
        return
    if p['cash'] < ACCUMULATION_BUY_USD:
        return

    try:
        ticker = await binance.fetch_ticker('BTC/USDT')
        btc_price = ticker['last']
        if not regime_detector.is_near_range_support(btc_price, 0.05):
            return

        amount = ACCUMULATION_BUY_USD / btc_price
        await asyncio.to_thread(db_query,
            'INSERT INTO accumulation (symbol, amount, cost, entry_price, entry_time) VALUES (?, ?, ?, ?, ?)',
            ('BTC/USDT', amount, ACCUMULATION_BUY_USD, btc_price, time.time()), commit=True)
        await asyncio.to_thread(db_query,
            'UPDATE portfolio SET cash = cash - ? WHERE id=1', (ACCUMULATION_BUY_USD,), commit=True)
        send_telegram(
            f"🏦 **ACCUMULATION BUY**\nFear: `{regime_detector.fear_index}` | "
            f"BTC: `${round(btc_price, 2)}` | Bought: `${ACCUMULATION_BUY_USD}`")
    except Exception as e:
        logger.error(f"Accumulation buy error: {e}")


# --- 14. HEARTBEAT ---

async def execute_heartbeat(regime_detector, sector_tracker):
    try:
        bars = await binance.fetch_ohlcv('BTC/USDT', '4h', limit=500)
        df = pd.DataFrame(bars, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        df['ema20'] = df['close'].ewm(span=20).mean()
        df['ema50'] = df['close'].ewm(span=50).mean()
        df['ma20'] = df['close'].rolling(20).mean()
        df['std'] = df['close'].rolling(20).std()
        df['upper'] = df['ma20'] + (df['std'] * 2)
        df['lower'] = df['ma20'] - (df['std'] * 2)
        df['rsi'] = calculate_rsi(df['close'])
        df['atr'] = calculate_atr(df)
        df['adx'] = calculate_adx(df)
        last = df.iloc[-1]

        trend = ("🟢 Uptrend" if last['close'] > last['ema20'] > last['ema50']
                 else "🟡 Pullback" if last['close'] < last['ema20'] and last['ema20'] > last['ema50']
                 else "🔴 Bearish")

        p = await asyncio.to_thread(get_portfolio)
        trades = await asyncio.to_thread(db_query, 'SELECT * FROM trades', fetchall=True)
        equity = p['cash'] + sum(t['cost'] for t in trades) if trades else p['cash']
        accum = await asyncio.to_thread(db_query, 'SELECT SUM(cost) as total FROM accumulation', fetchone=True)
        accum_total = accum['total'] if accum and accum['total'] else 0.0

        history = await asyncio.to_thread(db_query,
            'SELECT pnl_pct FROM trade_history ORDER BY id DESC LIMIT 20', fetchall=True)
        avg_pnl = round(sum(h['pnl_pct'] for h in history) / len(history), 2) if history else 0.0

        sectors = " | ".join([f"{k}:{v:+.1f}%" for k, v in sector_tracker.sector_performance.items()])
        re = {"RANGE": "📊", "TRENDING": "🚀", "DEFENSIVE": "🛡️"}.get(regime_detector.current_regime, "❓")

        msg = (f"🌕 **V34 ANALYTICS**\n"
               f"BTC: `${last['close']:,.2f}` {trend}\n"
               f"ADX: `{round(last['adx'], 1)}` | RSI: `{round(last['rsi'], 1)}` | ATR: `{round(last['atr'], 1)}`\n"
               f"---\n"
               f"{re} Regime: `{regime_detector.current_regime}` | "
               f"Range: `${round(regime_detector.range_low, 0)}`–`${round(regime_detector.range_high, 0)}`\n"
               f"Fear: `{regime_detector.fear_index}` | Sector: `{sector_tracker.leader}`\n"
               f"{sectors}\n---\n"
               f"Equity: `${round(equity, 2)}` | Cash: `${round(p['cash'], 2)}` | "
               f"Accum: `${round(accum_total, 2)}`\n"
               f"PnL(20): `{avg_pnl}%` | {p['wins']}W-{p['losses']}L | "
               f"Trades: `{len(trades) if trades else 0}/{MAX_OPEN_POSITIONS}`")

        chart_buf = await asyncio.to_thread(generate_chart, df, "BTC/USDT", "Analytics")
        send_telegram(msg, chart_buf)
    except Exception as e:
        logger.error(f"Heartbeat: {e}", exc_info=True)


async def heartbeat_loop(regime_detector, sector_tracker):
    while True:
        try:
            now = datetime.datetime.now()
            nxt = (now.hour // 2 * 2) + 2
            nxt_run = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(
                hours=(nxt - now.hour) if nxt < 24 else 2)
            await asyncio.sleep(max((nxt_run - now).total_seconds(), 60))
            await execute_heartbeat(regime_detector, sector_tracker)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Heartbeat loop: {e}")
            await asyncio.sleep(60)


# --- 15. SCANNER / STRATEGY MANAGER ---

async def scanner_loop(regime_detector, sector_tracker):
    while True:
        try:
            await asyncio.to_thread(check_daily_reset)
            p = await asyncio.to_thread(get_portfolio)
            if not p or time.time() < p['lockout_until']:
                await asyncio.sleep(900)
                continue

            # FIX #4: Only re-detect regime when needed
            btc_daily = await binance.fetch_ohlcv('BTC/USDT', '1d', limit=100)
            df_btc_daily = pd.DataFrame(btc_daily, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
            btc_price = df_btc_daily.iloc[-1]['close']

            if regime_detector.needs_redetection(btc_price):
                regime = await regime_detector.detect(df_btc_daily)
            else:
                regime = regime_detector.current_regime

            # FIX #8: Sector tracker respects its own cache
            await sector_tracker.update(binance)
            regime_detector.leading_sector = sector_tracker.leader

            await check_signal_performance()

            btc_atr = calculate_atr(df_btc_daily)
            btc_atr_current = btc_atr.iloc[-1]
            btc_atr_avg = btc_atr.rolling(20).mean().iloc[-1]
            btc_atr_ratio = btc_atr_current / btc_atr_avg if btc_atr_avg > 0 else 1.0

            event_active = await is_event_day()

            ct = await asyncio.to_thread(db_query, 'SELECT COUNT(*) as c FROM trades', fetchone=True)
            open_slots = MAX_OPEN_POSITIONS - (ct['c'] if ct else 0)

            logger.info(f"Scan | {regime} | Slots:{open_slots} | Cash:${p['cash']:.2f} | Fear:{regime_detector.fear_index}")

            if open_slots <= 0:
                await asyncio.sleep(900)
                continue

            if regime == 'DEFENSIVE':
                await run_accumulation(regime_detector)
            elif regime == 'RANGE':
                btc_4h = await binance.fetch_ohlcv('BTC/USDT', '4h', limit=500)
                df_4h = pd.DataFrame(btc_4h, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
                df_4h['ema20'] = df_4h['close'].ewm(span=20).mean()
                df_4h['ema50'] = df_4h['close'].ewm(span=50).mean()
                await strategy_btc_range(regime_detector, df_4h, df_btc_daily)
                if not event_active:
                    await strategy_sector_momentum(sector_tracker, btc_atr_ratio)
                await run_accumulation(regime_detector)
            elif regime == 'TRENDING':
                if not event_active:
                    await strategy_squeeze_breakout(btc_atr_ratio)
                    await strategy_sector_momentum(sector_tracker, btc_atr_ratio)

            await asyncio.sleep(900)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Scanner: {e}", exc_info=True)
            await asyncio.sleep(60)


# --- 16. API ROUTES ---

@app.route('/')
def home():
    trades = db_query('SELECT symbol FROM trades', fetchall=True)
    return f"V34-FIXED Online. Trades: {len(trades) if trades else 0}/{MAX_OPEN_POSITIONS}", 200


@app.route('/regime')
def regime_endpoint():
    latest = db_query('SELECT * FROM regime_history ORDER BY id DESC LIMIT 1', fetchone=True)
    trades = db_query('SELECT symbol, signal_type, entry, sl, tp FROM trades', fetchall=True)
    disabled = db_query('SELECT signal_type FROM signal_status WHERE enabled = 0', fetchall=True)
    accum = db_query('SELECT SUM(cost) as total, SUM(amount) as amount FROM accumulation', fetchone=True)
    return jsonify({
        'regime': latest['regime'] if latest else 'UNKNOWN',
        'btc_adx': latest['btc_adx'] if latest else 0,
        'btc_price': latest['btc_price'] if latest else 0,
        'range': f"${latest['range_low']}-${latest['range_high']}" if latest else 'N/A',
        'fear_index': latest['fear_index'] if latest else 0,
        'leading_sector': latest['leading_sector'] if latest else 'NONE',
        'active_trades': [dict(t) for t in trades] if trades else [],
        'disabled_signals': [d['signal_type'] for d in disabled] if disabled else [],
        'accumulation_usd': round(accum['total'], 2) if accum and accum['total'] else 0,
        'last_updated': latest['timestamp'] if latest else 0
    })


@app.route('/stats')
def stats():
    # FIX #3: Use original_cost for accurate capital tracking
    history = db_query('SELECT * FROM trade_history ORDER BY id DESC LIMIT 50', fetchall=True)
    if not history:
        return jsonify({'message': 'No trade history yet.'}), 200

    wins = sum(1 for h in history if h['pnl_pct'] > 0)
    losses = len(history) - wins
    avg_win = round(sum(h['pnl_pct'] for h in history if h['pnl_pct'] > 0) / max(wins, 1), 2)
    avg_loss = round(sum(h['pnl_pct'] for h in history if h['pnl_pct'] <= 0) / max(losses, 1), 2)
    total_capital = round(sum(h.get('original_cost', h['cost']) for h in history), 2)

    by_signal = {}
    for h in history:
        st = h.get('signal_type', 'UNKNOWN')
        if st not in by_signal:
            by_signal[st] = {'count': 0, 'pnl': 0, 'wins': 0, 'capital': 0}
        by_signal[st]['count'] += 1
        by_signal[st]['pnl'] += h['pnl_pct']
        by_signal[st]['capital'] += h.get('original_cost', h['cost'])
        if h['pnl_pct'] > 0:
            by_signal[st]['wins'] += 1

    breakdown = {k: {
        'count': v['count'], 'pnl': round(v['pnl'], 2),
        'win_rate': round(v['wins'] / v['count'] * 100, 1) if v['count'] > 0 else 0,
        'capital_deployed': round(v['capital'], 2)
    } for k, v in by_signal.items()}

    return jsonify({
        'total_trades': len(history), 'win_rate': round(wins / len(history) * 100, 1),
        'avg_win': avg_win, 'avg_loss': avg_loss,
        'total_pnl': round(sum(h['pnl_pct'] for h in history), 2),
        'total_capital_deployed': total_capital,
        'by_signal': breakdown
    })


# FIX #7: POST endpoint to add events
@app.route('/event', methods=['POST'])
def add_event():
    data = flask_request.get_json()
    if not data or 'date' not in data or 'name' not in data:
        return jsonify({'error': 'Requires date (YYYY-MM-DD) and name'}), 400
    impact = data.get('impact', 'HIGH')
    db_query('INSERT INTO event_calendar (event_date, event_name, impact) VALUES (?, ?, ?)',
             (data['date'], data['name'], impact), commit=True)
    return jsonify({'status': 'ok', 'event': data['name'], 'date': data['date']}), 201


@app.route('/events')
def list_events():
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    events = db_query('SELECT * FROM event_calendar WHERE event_date >= ? ORDER BY event_date',
                      (today,), fetchall=True)
    return jsonify(events if events else [])


@app.route('/reset')
def reset_trades():
    with db_lock:
        c = conn.cursor()
        for table in ['portfolio', 'trades', 'cooldowns', 'pending_signals',
                       'regime_history', 'accumulation', 'signal_status']:
            c.execute(f'DROP TABLE IF EXISTS {table}')
        conn.commit()
    init_db()
    send_telegram("🔄 **V34 RESET** | Balance: $1,000 | Trade history preserved.")
    return "Reset complete.", 200


# --- 17. MAIN ---

shutdown_event = asyncio.Event()


def handle_exit(signum, frame):
    logger.info("Shutdown signal received...")
    shutdown_event.set()


async def main():
    logger.info("Booting V34-FIXED Adaptive Engine...")
    await binance.load_markets()

    # FIX #2: Load futures markets for funding rate client
    try:
        await asyncio.to_thread(binance_futures_sync.load_markets)
    except Exception as e:
        logger.warning(f"Futures markets load failed (funding rates may be unavailable): {e}")

    regime_detector = RegimeDetector()
    sector_tracker = SectorTracker()

    threading.Thread(
        target=lambda: app.run(host='0.0.0.0', port=10000, use_reloader=False),
        daemon=True).start()

    btc_daily = await binance.fetch_ohlcv('BTC/USDT', '1d', limit=100)
    df_btc = pd.DataFrame(btc_daily, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
    regime = await regime_detector.detect(df_btc)
    await sector_tracker.update(binance)
    regime_detector.leading_sector = sector_tracker.leader

    send_telegram(
        f"⚙️ **V34-FIXED DEPLOYED**\n"
        f"Regime: `{regime}` | Range: `${round(regime_detector.range_low, 0)}`–`${round(regime_detector.range_high, 0)}`\n"
        f"Fear: `{regime_detector.fear_index}` | Sector: `{sector_tracker.leader}`\n"
        f"Fixes: Scoping, Caching, Accounting, Swing S/R, Regime Lock, Accum Exit")

    await execute_heartbeat(regime_detector, sector_tracker)

    tasks = [
        asyncio.create_task(heartbeat_loop(regime_detector, sector_tracker)),
        asyncio.create_task(position_manager(regime_detector)),
        asyncio.create_task(scanner_loop(regime_detector, sector_tracker))
    ]

    await shutdown_event.wait()
    await binance.close()
    conn.close()
    for t in tasks:
        t.cancel()
    logger.info("Shutdown complete.")


if __name__ == '__main__':
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
