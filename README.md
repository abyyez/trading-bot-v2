# Adaptive Crypto Trading Engine

An autonomous cryptocurrency trading bot that detects market regimes and dynamically switches between five strategies. Built with Python, async WebSockets, SQLite, and a real-time Flask dashboard.

> **Default mode: paper trading** — simulates trades with real Binance data, no real orders executed.

---

## Architecture

Three concurrent loops handle different responsibilities:

| Loop | Interval | Purpose |
|------|----------|---------|
| Scanner | 3–10 min | Detect regime, score sectors, evaluate entry signals |
| Exit Checker | 5 sec | Trailing stops, partial exits, time stops |
| Heartbeat | 2 hrs | Analytics, equity snapshot, Telegram summary |

---

## Strategies

| Strategy | Regime | Signal |
|----------|--------|---------|
| BTC Range Trading | RANGE | Buy near swing support, RSI + taker volume confirm |
| Sector Momentum | RANGE/TRENDING | 4H EMA20 pullback in leading sector, 8/12 score gate |
| Squeeze Breakout | TRENDING | Bollinger compression → expansion + ADX |
| Trend Following | TRENDING | Donchian 20-period breakout + volume confirm |
| Mean Reversion | Any | Z-score < −1.8 below 50-period mean |
| Accumulation | DEFENSIVE | Fixed BTC buys when Fear & Greed < 15 |

---

## Risk Management

| Parameter | Value |
|-----------|-------|
| Risk per trade | 0.5% portfolio (alts) / 1% (BTC range) |
| Stop loss | 2× ATR below entry |
| Take profit | 5× ATR (min 2.0R required) |
| Partial exit | 30% at 2× ATR, remainder moves to breakeven |
| Trailing stop | 1.5× ATR wide → tightens to 1.2× after +6% |
| Daily drawdown limit | 4% → 24h pause |
| Max positions | 4 (max 2 per sector) |
| Time stop | 48h max hold |

---

## Signal Filters

All entries must pass: confirmation candle (next 4H candle confirms), $5M+ daily volume, <0.3% spread, no 30%+ recent pump, funding rate < 0.05%, BTC ATR < 2.2× average.

---

## Dashboard

`dashboard.py` serves a full monitoring UI at `http://localhost:10000/dashboard`:

- Live equity curve with mark-to-market open positions
- Open positions table with unrealized PnL and R-multiple
- Analytics: win rate, expectancy by signal/regime/exit reason, day×hour heatmap
- Diagnostics: top rejection reasons, quiet mode status
- Controls: pause/resume trading, force exit positions, mode switch, paper reset
- Server-Sent Events: browser receives `trade_closed` events in real time
- Export: trade history and signal analysis as CSV

Set `ADMIN_SECRET` in `.env` to password-protect the dashboard.

---

## API Reference

| Endpoint | Auth | Description |
|----------|------|-------------|
| `GET /` | — | Health check |
| `GET /dashboard` | ✓ | Web UI |
| `GET /regime` | — | Current regime, BTC stats, active trades |
| `GET /positions` | ✓ | Open positions with live prices |
| `GET /api/equity_curve` | — | Realized + MTM equity curve |
| `GET /api/btc_price` | — | Live BTC price |
| `GET /api/heartbeat` | — | Bot liveness |
| `GET /api/analytics/overview` | — | Aggregated stats + auto insights |
| `GET /events/stream` | — | SSE trade notifications |
| `GET /api/export/trades.csv` | — | Full trade log |
| `POST /force_exit` | ✓ | Manually exit a position |
| `POST /pause` / `POST /resume` | ✓ | Toggle new entries |
| `POST /mode` | ✓ | Switch PAPER ↔ LIVE |
| `POST /reset` | ✓ | Reset paper portfolio |

---

## Tech Stack

- **Python 3.11+** · asyncio · CCXT Pro (Binance WebSocket + REST)
- **Pandas / NumPy** — RSI, ATR, ADX, Bollinger Bands, Donchian, Z-score
- **SQLite** (WAL mode) — crash-safe persistent state
- **Flask** — REST + SSE dashboard backend
- **Chart.js** — Dashboard charts (no frontend framework)
- **mplfinance** — Candlestick charts for Telegram alerts

---

## Setup

```bash
git clone https://github.com/abyyez/trading-bot-v2.git
cd trading-bot-v2
pip install -r requirements.txt
cp .env.example .env   # fill in credentials
python main.py
```

### `.env` variables

```
TELEGRAM_BOT_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
BINANCE_API_KEY=your_key
BINANCE_API_SECRET=your_secret

# Optional
EXECUTION_MODE=PAPER    # or LIVE
ADMIN_SECRET=           # dashboard password (empty = no auth)
FLASK_PORT=10000
DISABLE_CHARTS=0        # 1 = skip charts (saves RAM)
```

---

## Project Structure

```
├── main.py                # Bot core — strategies, risk, broker, scanner loops
├── dashboard.py           # Flask dashboard — all HTTP/SSE endpoints
├── templates/
│   └── dashboard.html     # Dashboard UI (vanilla JS + Chart.js)
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Design Notes

- **`main.py` never imports from `dashboard.py`** — the dashboard is bound at startup via `init_dashboard()`, keeping trading logic isolated from the HTTP layer.
- **SQLite over Postgres** — single process, WAL mode gives crash recovery with zero ops overhead.
- **Paper mode by default** — `Broker` class abstracts paper/live. Switching to live requires only `EXECUTION_MODE=LIVE` + `/reconcile`, no code changes.
- **No backtesting** — survivorship bias and realistic slippage simulation make crypto backtests misleading. Paper forward-testing gives honest data.

---

*Built to demonstrate quantitative trading system design, async Python architecture, and real-time data processing.*
