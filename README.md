# Adaptive Crypto Trading Engine

An autonomous cryptocurrency paper trading bot that detects market regimes and dynamically switches between strategies. Built with Python, async WebSockets, and SQLite for persistent state management.

> **Disclaimer:** This is a **paper trading bot** — it simulates trades using real-time Binance market data but does **not** execute real orders. 

---

## How It Works

The bot continuously monitors the crypto market through Binance WebSocket streams and makes autonomous trading decisions based on a three-layer architecture:

**1. Regime Detection**
Every 12 hours (or when BTC moves 3%+), the engine classifies the market into one of three regimes using BTC's daily chart:

| Regime | Condition | Active Strategies |
|--------|-----------|-------------------|
| **RANGE** | ADX < 25, price inside support/resistance | BTC Range Trading + Sector Momentum |
| **TRENDING** | ADX ≥ 25, price above resistance | Squeeze Breakout + Sector Momentum |
| **DEFENSIVE** | Price below range floor or 50 EMA | Accumulation only (small fixed buys) |

**2. Strategy Execution**
Each strategy has its own signal logic, confirmation requirements, and position sizing rules. The Strategy Manager delegates to the correct strategy based on the current regime — no manual intervention required.

**3. Risk Management**
Every position is managed by a real-time WebSocket position manager that handles trailing stops, partial exits, and emergency kill switches independently of the scanner.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Scanner Loop                    │
│              (runs every 15 min)                 │
│                                                  │
│  ┌──────────────┐  ┌────────────────────────┐   │
│  │   Regime      │  │   Sector Tracker       │   │
│  │   Detector    │  │   (7-day relative      │   │
│  │   (BTC Daily) │  │    performance)        │   │
│  └──────┬───────┘  └──────────┬─────────────┘   │
│         │                     │                   │
│         ▼                     ▼                   │
│  ┌──────────────────────────────────────────┐    │
│  │         Strategy Manager                  │    │
│  │  RANGE → BTC Range + Sector Momentum      │    │
│  │  TRENDING → Squeeze Breakout + Sector     │    │
│  │  DEFENSIVE → Accumulation                 │    │
│  └──────────────┬───────────────────────────┘    │
└─────────────────┼────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│           Position Manager (WebSocket)           │
│  • Real-time price monitoring                    │
│  • Adaptive trailing stops                       │
│  • Partial exits (40% at 1.5x ATR)              │
│  • Range breakdown emergency exit                │
│  • Fee-adjusted win/loss tracking                │
└─────────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│              SQLite + Telegram                   │
│  • Persistent state across restarts              │
│  • Trade history & performance analytics         │
│  • Chart notifications via Telegram              │
│  • REST API for monitoring                       │
└─────────────────────────────────────────────────┘
```

---

## Strategies

### BTC Range Trading
Trades the BTC/USDT range during sideways markets. Buys near swing-point support with RSI confirmation and taker buy volume validation. Takes profit near range resistance. Uses 40% portfolio allocation with a hard stop below the range floor.

### Sector Momentum
Tracks five crypto sectors (AI, L1, DeFi, Meme, Privacy) and identifies the one outperforming BTC over a rolling 7-day window. Enters on pullbacks to the 4H EMA20 within the leading sector, confirmed by daily timeframe alignment, funding rate analysis, and BTC pair relative strength.

### Volatility Squeeze Breakout
Activated only in trending regimes. Scans the top 75 altcoins by volume for Bollinger Band compression followed by expansion with above-average volume and strong ADX. Requires confirmation on the following candle.

### Accumulation Mode
During extreme fear (Fear & Greed Index < 15), makes small fixed-dollar BTC purchases near range support. Automatically exits when BTC reclaims the daily 200 EMA or when holdings reach 20%+ profit.

---

## Signal Filters

Every entry signal must pass through multiple validation layers before execution:

- **Confirmation Candle** — Signals are stored as pending and only execute if the next 4H candle confirms the move (5-hour expiry window)
- **Daily Timeframe Overlay** — Coin's daily close must be above its own daily EMA20
- **BTC Pair Relative Strength** — Coin must be outperforming on its /BTC trading pair
- **Anti-Pump Filter** — Rejects entries on assets that rallied 30%+ in the prior 3 days
- **Funding Rate Filter** — Blocks entries when perpetual futures funding exceeds +0.05% (crowded longs)
- **Taker Buy/Sell Ratio** — Confirms genuine buying pressure on breakouts and bounces
- **ADX Trend Strength** — Minimum ADX threshold to confirm a trending environment
- **Correlation Filter** — Skips entries correlated > 0.85 with existing open positions
- **BTC Volatility Filter** — Blocks all entries when BTC daily ATR exceeds 2× its 20-day average

---

## Risk Management

| Feature | Description |
|---------|-------------|
| Position Sizing | 2% risk per trade, max 20% allocation (alts) / 40% (BTC range) |
| Partial Exits | Sells 40% at 1.5× ATR profit, moves remainder to breakeven |
| Trailing Stop | 1.5× ATR wide, tightens to 1.0× ATR after 3% unrealized gain |
| Daily Kill Switch | 4% daily drawdown triggers 24-hour trading pause |
| Range Breakdown | Emergency exit if BTC breaks below the trade's frozen range floor |
| Time Stop | Forces exit after 48 hours if trade hasn't moved |
| Cooldown | 4-hour per-symbol cooldown after any exit |
| Max Positions | 4 concurrent positions with real-time slot tracking |
| Entry Lock | `asyncio.Lock` prevents double-spend race conditions on concurrent signals |
| Event Calendar | Pre-seeded FOMC, CPI, and crypto catalyst dates — reduces exposure automatically |

---

## Performance Intelligence

- **Auto-Disable Signals** — Tracks per-signal win rate over rolling 20 trades. Disables any signal that drops below 30% win rate (25% threshold for first 30 trades). Re-enables automatically when performance recovers.
- **Trade History** — Full trade log with original capital deployed, exit reason, and PnL percentage. Preserved across account resets.
- **Fear & Greed Integration** — Fetched from the Alternative.me API. Used as a filter for accumulation buys and alt entry blocking.

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check — shows version and active trade count |
| `/regime` | GET | Current regime, BTC ADX, range levels, fear index, sector leader, active trades, disabled signals |
| `/stats` | GET | Win rate, avg win/loss, total PnL, per-signal breakdown with capital deployed |
| `/events` | GET | Upcoming events from the calendar |
| `/event` | POST | Add a custom event `{"date": "2026-05-01", "name": "FOMC Decision"}` |
| `/reset` | GET | Resets portfolio to $1,000 (trade history preserved) |

---

## Tech Stack

- **Python 3.11+** with `asyncio` for concurrent operations
- **CCXT Pro** — Async WebSocket streams + REST API for Binance
- **Pandas / NumPy** — Technical indicator calculations (RSI, ATR, ADX, Bollinger Bands, OBV, correlation)
- **SQLite** — Persistent state with WAL mode for crash recovery
- **Flask** — Lightweight REST API for monitoring
- **mplfinance / Matplotlib** — Candlestick chart generation
- **Telegram Bot API** — Real-time trade notifications with retry logic

---

## Setup

### Prerequisites
- Python 3.11+
- Binance account (API key with read permissions — no trading permissions needed for paper mode)
- Telegram bot token ([create one via BotFather](https://t.me/BotFather))

### Installation

```bash
git clone https://github.com/yourusername/adaptive-crypto-engine.git
cd adaptive-crypto-engine

pip install -r requirements.txt

cp .env.example .env
# Edit .env with your API keys
```

### Configuration

```bash
# .env
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_chat_id
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret
```

### Run

```bash
python sniper_bot_v34.py
```

The bot will:
1. Load Binance market metadata
2. Detect the current market regime
3. Calculate sector performance
4. Send a deployment summary to Telegram
5. Begin the scan loop (every 15 minutes)

---

## Project Structure

```
├── sniper_bot_v34.py      # Main bot (single-file architecture)
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variable template
├── .gitignore              # Git ignore rules
├── trading_state.db        # SQLite database (auto-created at runtime)
└── telegram_failed.log     # Fallback log for failed notifications
```

---

## Sample Telegram Output

**Heartbeat (every 2 hours):**
```
🌕 V34 ANALYTICS
BTC: $71,450.00 🟡 Pullback
ADX: 18.3 | RSI: 42.1 | ATR: 1,892.4
---
📊 Regime: RANGE | Range: $65,200–$74,800
Fear: 12 | Sector: AI
AI:+4.2% | L1:-1.8% | DEFI:-3.1% | MEME:-7.4% | PRIVACY:+1.2%
---
Equity: $1,024.50 | Cash: $612.30
Accum: $75.00
PnL(20): 1.8% | 12W-8L | Trades: 2/4
```

**Trade Execution:**
```
💰 EXECUTED: FET/USDT
⚡ SECTOR MOMENTUM | Entry: 1.2450
Size: 48.2 ($60.00)
TP: 1.3120 | SL: 1.2050 | R:R: 2.1:1
```

**Partial Exit:**
```
📤 PARTIAL EXIT (40%): FET/USDT
Sold at 1.2890 | PnL: +3.12%
Stop → breakeven 1.2500
```

---

## Design Decisions

**Why single-file?**
For a bot this size (~1,600 lines), a single file with clear section headers is easier to audit, deploy, and debug than a multi-module package. Every component — from regime detection to position management — is visible in one scroll. For production scaling, this would be split into modules.

**Why SQLite over PostgreSQL/Redis?**
The bot runs as a single process with no concurrent writers. SQLite with WAL mode provides crash recovery, persistent state, and zero ops overhead. The database is typically under 1MB even after months of operation.

**Why paper trading only?**
The bot calculates entry prices with simulated slippage (`price * 1.002`) but does not interact with Binance's order API. This is intentional — the architecture is designed so that replacing the paper execution with real limit orders requires changes to only two functions (`execute_entry` and `execute_entry_btc_range`).

**Why no backtesting module?**
Backtesting crypto strategies is misleading due to survivorship bias (you only backtest on coins that still exist), look-ahead bias in indicator calculations, and the inability to simulate realistic slippage on illiquid alts. The bot is designed for forward-testing in paper mode, which provides more honest performance data.

---

## Limitations

- **Paper trading only** — Does not execute real orders
- **Binance-specific** — Uses Binance API conventions (could be adapted to other CCXT-supported exchanges)
- **Single-process** — Not designed for horizontal scaling
- **No backtesting** — Forward-testing in paper mode is the intended evaluation method
- **Filter sensitivity** — In low-volatility, high-fear markets, signal conditions may rarely align (this is by design to avoid false signals, but it means the bot may go days without trading)

---

## Author

Built as a project demonstrating quantitative trading system design, async Python architecture, and real-time data processing.
