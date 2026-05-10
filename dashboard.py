"""
dashboard.py — HTTP endpoints for SniperBot.

Separated from main.py so dashboard work doesn't touch trading logic.
The bot calls init_dashboard() once at startup to bind references; from
that point on, this module is fully self-contained.

To add a new endpoint that needs new bot data:
  1. Add the field to init_dashboard()'s signature
  2. Pass it from main.py at init time
  3. Use _ctx.<field> inside your route

This file should never import anything from main.py.
"""

from functools import wraps
from types import SimpleNamespace
import asyncio
import csv
import datetime
import hmac
import io
import json
import logging
import threading
import time

from flask import (
    Blueprint,
    jsonify,
    request as flask_request,
    abort,
    render_template,
    render_template_string,
    redirect,
    url_for,
    session,
    Response,
)

logger = logging.getLogger("SniperBot.dashboard")

dashboard_bp = Blueprint('dashboard', __name__)

# Bound by init_dashboard() — every route reads from this.
_ctx: SimpleNamespace = None


# =============================================================================
# INITIALIZATION
# =============================================================================
def init_dashboard(
    flask_app,
    *,
    db_query,
    db_lock,
    db_conn,
    safe_float,
    is_reconcile_blocked,
    is_trading_paused,
    get_quiet_multiplier,
    get_sector_for_symbol,
    set_bot_state,
    send_telegram,
    init_db_func,
    execute_exit,
    broker,
    get_main_loop,
    get_regime_detector,
    admin_secret,
    starting_cash,
    max_open_positions,
    max_hold_time_sec,
    max_positions_per_sector,
    version_string,
):
    """Bind bot internals into the dashboard context and register the blueprint."""
    global _ctx
    _ctx = SimpleNamespace(
        db_query=db_query,
        db_lock=db_lock,
        db_conn=db_conn,
        safe_float=safe_float,
        is_reconcile_blocked=is_reconcile_blocked,
        is_trading_paused=is_trading_paused,
        get_quiet_multiplier=get_quiet_multiplier,
        get_sector_for_symbol=get_sector_for_symbol,
        set_bot_state=set_bot_state,
        send_telegram=send_telegram,
        init_db_func=init_db_func,
        execute_exit=execute_exit,
        broker=broker,
        get_main_loop=get_main_loop,
        get_regime_detector=get_regime_detector,
        admin_secret=admin_secret,
        starting_cash=starting_cash,
        max_open_positions=max_open_positions,
        max_hold_time_sec=max_hold_time_sec,
        max_positions_per_sector=max_positions_per_sector,
        version_string=version_string,
    )

    # Required for Flask session cookies used by dashboard login.
    if admin_secret and not flask_app.secret_key:
        flask_app.secret_key = admin_secret
        flask_app.permanent_session_lifetime = datetime.timedelta(days=30)

    flask_app.register_blueprint(dashboard_bp)
    logger.info(f"Dashboard initialized — {version_string}")


# =============================================================================
# AUTH + ASYNC HELPERS
# =============================================================================
def _is_authorized():
    """Allow browser session auth or Bearer token auth."""
    if not _ctx.admin_secret:
        return True

    token = flask_request.headers.get('Authorization', '').replace('Bearer ', '')
    if token and hmac.compare_digest(token, _ctx.admin_secret):
        return True

    return bool(session.get('dashboard_authed'))


def require_auth(f):
    """Protect routes when ADMIN_SECRET is set."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _is_authorized():
            abort(403)
        return f(*args, **kwargs)
    return decorated


@dashboard_bp.before_request
def protect_dashboard_routes():
    """Protect all dashboard routes when ADMIN_SECRET is set."""
    if not _ctx or not _ctx.admin_secret:
        return None

    allowed_endpoints = {
        'dashboard.login_page',
        'dashboard.logout_page',
    }

    if flask_request.endpoint in allowed_endpoints:
        return None

    if _is_authorized():
        return None

    wants_html = 'text/html' in flask_request.headers.get('Accept', '')
    if flask_request.path == '/dashboard' or wants_html:
        return redirect(url_for('dashboard.login_page', next=flask_request.path))

    abort(403)


def run_async_in_loop(coro, timeout=30):
    """Run an async coroutine on the bot's main event loop from a Flask thread."""
    loop = _ctx.get_main_loop()
    if loop is None:
        raise RuntimeError("Main loop not initialized")
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=timeout)


# =============================================================================
# CACHE + TIMEFRAME HELPERS
# =============================================================================
_analytics_cache = {}
_analytics_cache_lock = threading.Lock()


def _cached(key, ttl, func):
    """Tiny TTL cache. Key is opaque; ttl in seconds; func called on miss."""
    now = time.time()
    with _analytics_cache_lock:
        entry = _analytics_cache.get(key)
        if entry and now - entry[0] < ttl:
            return entry[1]

    value = func()

    with _analytics_cache_lock:
        _analytics_cache[key] = (now, value)
    return value


def _latest_trade_id():
    try:
        row = _ctx.db_query('SELECT MAX(id) AS id FROM trade_history', fetchone=True)
        return row['id'] if row and row['id'] else 0
    except Exception:
        return 0


def _parse_since(since_str):
    """Convert '24h' / '7d' / '30d' / 'all' into a unix timestamp lower-bound."""
    if not since_str or since_str == 'all':
        return None
    try:
        if since_str.endswith('h'):
            return time.time() - int(since_str[:-1]) * 3600
        if since_str.endswith('d'):
            return time.time() - int(since_str[:-1]) * 86400
    except ValueError:
        pass
    return None


def _request_since():
    return _parse_since(flask_request.args.get('since', 'all'))


async def _fetch_prices_for_symbols(symbols):
    """Fetch current price for each symbol in parallel; never raises."""
    if not symbols:
        return {}

    async def safe_one(sym):
        try:
            return sym, await _ctx.broker.get_current_price(sym)
        except Exception:
            return sym, None

    results = await asyncio.gather(*[safe_one(s) for s in symbols])
    return {sym: price for sym, price in results}


def _live_prices_for_open_trades(open_trades, timeout=10):
    """Sync wrapper that returns dict[symbol -> price] for open trades. Never raises."""
    if not open_trades:
        return {}
    symbols = list({t['symbol'] for t in open_trades})
    try:
        return run_async_in_loop(_fetch_prices_for_symbols(symbols), timeout=timeout)
    except Exception as e:
        logger.warning(f"Live price fetch failed: {e}")
        return {}


# =============================================================================
# READ-ONLY ENDPOINTS
# =============================================================================
@dashboard_bp.route('/login', methods=['GET', 'POST'])
def login_page():
    """Simple password login for dashboard access."""
    if not _ctx.admin_secret:
        return redirect(url_for('dashboard.dashboard_page'))

    error = ''
    next_url = flask_request.args.get('next') or url_for('dashboard.dashboard_page')
    if not next_url.startswith('/'):
        next_url = url_for('dashboard.dashboard_page')

    if flask_request.method == 'POST':
        password = flask_request.form.get('password', '')
        posted_next = flask_request.form.get('next') or next_url
        if not posted_next.startswith('/'):
            posted_next = url_for('dashboard.dashboard_page')

        if hmac.compare_digest(password, _ctx.admin_secret):
            session.permanent = True
            session['dashboard_authed'] = True
            return redirect(posted_next)

        error = 'Invalid password'

    return render_template_string('''
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>SniperBot Login</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">

  <style>
    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: radial-gradient(circle at top, #18213b 0, #0b1020 45%, #070b16 100%);
      color: #eef3ff;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
    }

    .card {
      width: min(420px, calc(100vw - 32px));
      background: linear-gradient(180deg, rgba(20, 27, 47, 0.96), rgba(16, 23, 41, 0.96));
      border: 1px solid #26324f;
      border-radius: 18px;
      box-shadow: 0 16px 40px rgba(0, 0, 0, 0.28);
      padding: 24px;
    }

    h1 { margin: 0 0 6px; font-size: 26px; letter-spacing: -0.03em; }
    p { color: #9aa6c3; margin: 0 0 18px; font-size: 14px; }
    label { display: block; color: #9aa6c3; font-size: 13px; margin-bottom: 8px; }

    input {
      width: 100%; padding: 12px 14px; border-radius: 12px;
      border: 1px solid #26324f; background: #0b1020;
      color: #eef3ff; font-size: 15px; outline: none;
    }

    button {
      width: 100%; margin-top: 14px; padding: 12px 14px;
      border: 0; border-radius: 12px; background: #6ea8fe;
      color: #07101f; font-weight: 800; cursor: pointer;
    }

    .error { color: #ff6b6b; margin-top: 12px; font-size: 13px; min-height: 18px; }
  </style>
</head>
<body>
  <form class="card" method="post">
    <h1>SniperBot Dashboard</h1>
    <p>Enter your dashboard password to continue.</p>
    <input type="hidden" name="next" value="{{ next_url }}">
    <label for="password">Password</label>
    <input id="password" name="password" type="password" autocomplete="current-password" autofocus required>
    <button type="submit">Login</button>
    <div class="error">{{ error }}</div>
  </form>
</body>
</html>
    ''', error=error, next_url=next_url)


@dashboard_bp.route('/logout', methods=['GET', 'POST'])
def logout_page():
    session.pop('dashboard_authed', None)
    return redirect(url_for('dashboard.login_page'))


@dashboard_bp.route('/dashboard')
def dashboard_page():
    return render_template(
        'dashboard.html',
        version=_ctx.version_string,
        starting_cash=_ctx.starting_cash,
        max_open_positions=_ctx.max_open_positions,
    )


@dashboard_bp.route('/')
def home():
    trades = _ctx.db_query('SELECT symbol FROM trades', fetchall=True)
    flags = []
    if _ctx.is_reconcile_blocked():
        flags.append('RECONCILE')
    if _ctx.is_trading_paused():
        flags.append('PAUSED')
    status = '|'.join(flags) if flags else 'OK'
    return (f"{_ctx.version_string} [{_ctx.broker.mode}] {status} | "
            f"Trades: {len(trades) if trades else 0}/{_ctx.max_open_positions}"), 200


@dashboard_bp.route('/regime')
def regime_endpoint():
    latest = _ctx.db_query('SELECT * FROM regime_history ORDER BY id DESC LIMIT 1', fetchone=True)
    trades = _ctx.db_query(
        'SELECT symbol, signal_type, entry, sl, tp, risk_dollars, realized_partial_r FROM trades',
        fetchall=True)
    disabled = _ctx.db_query('SELECT signal_type, expectancy FROM signal_status WHERE enabled = 0',
                              fetchall=True)
    return jsonify({
        'mode': _ctx.broker.mode,
        'reconcile_blocked': _ctx.is_reconcile_blocked(),
        'trading_paused': _ctx.is_trading_paused(),
        'regime': latest['regime'] if latest else 'UNKNOWN',
        'btc_price': latest['btc_price'] if latest else 0,
        'fear_index': latest['fear_index'] if latest else 0,
        'active_trades': [dict(t) for t in trades] if trades else [],
        'disabled_signals': [dict(d) for d in disabled] if disabled else [],
    })


@dashboard_bp.route('/api/btc_price')
def btc_price_api():
    latest = _ctx.db_query(
        'SELECT btc_price FROM regime_history ORDER BY id DESC LIMIT 1', fetchone=True)
    fallback_price = _ctx.safe_float(latest['btc_price'], 0.0) if latest else 0.0

    async def _fetch_live_btc():
        price = await _ctx.broker.get_current_price('BTCUSDT')
        return _ctx.safe_float(price, 0.0)

    try:
        live_price = run_async_in_loop(_fetch_live_btc(), timeout=8)
        if live_price > 0:
            return jsonify({'symbol': 'BTCUSDT', 'price': round(live_price, 2),
                            'source': 'broker_live', 'mode': _ctx.broker.mode,
                            'timestamp': time.time(), 'fallback': False})
    except Exception as e:
        logger.warning(f"/api/btc_price live fetch failed: {e}")

    if fallback_price > 0:
        return jsonify({'symbol': 'BTCUSDT', 'price': round(fallback_price, 2),
                        'source': 'regime_cache', 'mode': _ctx.broker.mode,
                        'timestamp': time.time(), 'fallback': True}), 200

    return jsonify({'symbol': 'BTCUSDT', 'price': None, 'source': 'unavailable',
                    'mode': _ctx.broker.mode, 'timestamp': time.time(), 'fallback': False,
                    'error': 'BTC price unavailable'}), 503


@dashboard_bp.route('/api/heartbeat')
def heartbeat_api():
    latest = _ctx.db_query('SELECT MAX(timestamp) AS ts FROM regime_history', fetchone=True)
    last_ts = _ctx.safe_float(latest['ts'] if latest else 0, 0.0)
    age = time.time() - last_ts if last_ts > 0 else None
    if age is None: status = 'unknown'
    elif age < 120: status = 'alive'
    elif age < 600: status = 'stale'
    else: status = 'dead'
    return jsonify({'status': status, 'last_tick': last_ts if last_ts > 0 else None,
                    'age_seconds': round(age, 1) if age is not None else None,
                    'mode': _ctx.broker.mode})


@dashboard_bp.route('/events/stream')
def event_stream():
    """Server-Sent Events: pushes 'trade_closed' events as new trades land."""
    try:
        row = _ctx.db_query('SELECT MAX(id) AS id FROM trade_history', fetchone=True)
        baseline_id = row['id'] if row and row['id'] else 0
    except Exception:
        baseline_id = 0

    def generate(start_id):
        last_id = start_id
        last_ping = time.time()
        yield f'event: connected\ndata: {{"last_trade_id": {last_id}}}\n\n'
        for _ in range(7200):
            try:
                new_rows = _ctx.db_query(
                    '''SELECT id, symbol, signal_type, pnl_pct, r_multiple,
                              exit_reason, exit_time
                       FROM trade_history WHERE id > ? ORDER BY id ASC''',
                    (last_id,), fetchall=True) or []
                for trade in new_rows:
                    payload = json.dumps(dict(trade), default=str)
                    yield f'id: {trade["id"]}\nevent: trade_closed\ndata: {payload}\n\n'
                    last_id = trade['id']
                now = time.time()
                if now - last_ping > 20:
                    yield f': ping {int(now)}\n\n'
                    last_ping = now
            except GeneratorExit:
                return
            except Exception as e:
                logger.warning(f"SSE poll loop: {e}")
            time.sleep(3)

    return Response(generate(baseline_id), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache, no-transform',
                             'X-Accel-Buffering': 'no', 'Connection': 'keep-alive'})


@dashboard_bp.route('/stats')
def stats():
    try:
        history = _ctx.db_query('SELECT * FROM trade_history ORDER BY id DESC LIMIT 500', fetchall=True)
        if not history:
            return jsonify({'message': 'No history', 'mode': _ctx.broker.mode}), 200
        wins = 0
        r_mults = []
        total_fees_sum = 0.0
        for h in history:
            r = _ctx.safe_float(h.get('r_multiple'), 0.0)
            r_mults.append(r)
            if r > 0: wins += 1
            total_fees_sum += _ctx.safe_float(h.get('total_fees'), 0.0)
        expectancy_r = round(sum(r_mults) / len(r_mults), 2) if r_mults else 0
        by_signal = {}
        for h in history:
            st = str(h.get('signal_type') or '?')
            if st not in by_signal:
                by_signal[st] = {'count': 0, 'wins': 0, 'r_sum': 0.0}
            by_signal[st]['count'] += 1
            r = _ctx.safe_float(h.get('r_multiple'), 0.0)
            by_signal[st]['r_sum'] += r
            if r > 0: by_signal[st]['wins'] += 1
        breakdown = {k: {'count': v['count'],
                         'win_rate': round(v['wins'] / v['count'] * 100, 1) if v['count'] > 0 else 0,
                         'expectancy_r': round(v['r_sum'] / v['count'], 2) if v['count'] > 0 else 0}
                     for k, v in by_signal.items()}
        return jsonify({'mode': _ctx.broker.mode, 'total_trades': len(history),
                        'win_rate': round(wins / len(history) * 100, 1),
                        'expectancy_r': expectancy_r, 'total_fees': round(total_fees_sum, 4),
                        'by_signal': breakdown})
    except Exception as e:
        logger.error(f"/stats endpoint: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@dashboard_bp.route('/orders')
def order_log_endpoint():
    orders = _ctx.db_query('SELECT * FROM order_log ORDER BY id DESC LIMIT 30', fetchall=True)
    return jsonify(orders if orders else [])


@dashboard_bp.route('/intents')
def intents_endpoint():
    intents = _ctx.db_query('SELECT * FROM order_intents ORDER BY id DESC LIMIT 30', fetchall=True)
    return jsonify(intents if intents else [])


@dashboard_bp.route('/analysis')
def analysis_endpoint():
    rows = _ctx.db_query(
        '''SELECT th.symbol, th.signal_type, th.r_multiple, th.exit_reason,
                  ta.hour_of_day, ta.day_of_week, ta.regime_at_entry,
                  ta.btc_atr_ratio, ta.fear_index
           FROM trade_history th
           LEFT JOIN trade_analysis ta ON th.id = ta.trade_history_id
           ORDER BY th.id DESC LIMIT 50''', fetchall=True)
    return jsonify(rows if rows else [])


@dashboard_bp.route('/api/recent_trades')
def recent_trades_api():
    rows = _ctx.db_query(
        '''SELECT symbol, signal_type, entry, exit_price, pnl_pct,
                  exit_reason, exit_time, total_fees, r_multiple, regime
           FROM trade_history ORDER BY exit_time DESC LIMIT 25''', fetchall=True) or []
    return jsonify(rows)


@dashboard_bp.route('/api/equity_curve')
def equity_curve_api():
    starting = float(_ctx.starting_cash)
    rows = _ctx.db_query(
        '''SELECT symbol, exit_time, pnl_pct, original_cost, cost, total_fees, r_multiple
           FROM trade_history ORDER BY exit_time ASC LIMIT 500''', fetchall=True) or []
    equity = starting
    peak = starting
    max_dd_pct = 0.0
    points = [{'label': 'Start', 'timestamp': None, 'equity': round(equity, 2),
                'pnl_usd': 0.0, 'symbol': 'START', 'r_multiple': 0.0, 'drawdown_pct': 0.0}]
    for row in rows:
        base_cost = _ctx.safe_float(row.get('original_cost'), 0.0)
        if base_cost <= 0: base_cost = _ctx.safe_float(row.get('cost'), 0.0)
        pnl_pct = _ctx.safe_float(row.get('pnl_pct'), 0.0)
        pnl_usd = base_cost * (pnl_pct / 100.0)
        equity += pnl_usd
        if equity > peak: peak = equity
        dd_pct = (peak - equity) / peak * 100 if peak > 0 else 0.0
        if dd_pct > max_dd_pct: max_dd_pct = dd_pct
        exit_ts = _ctx.safe_float(row.get('exit_time'), 0.0)
        label = (datetime.datetime.fromtimestamp(exit_ts).strftime('%m-%d %H:%M')
                 if exit_ts > 0 else (row.get('symbol') or 'Trade'))
        points.append({'label': label, 'timestamp': exit_ts, 'equity': round(equity, 2),
                       'pnl_usd': round(pnl_usd, 2), 'symbol': row.get('symbol'),
                       'r_multiple': _ctx.safe_float(row.get('r_multiple'), 0.0),
                       'drawdown_pct': round(dd_pct, 2)})
    open_trades = _ctx.db_query('SELECT symbol, entry, amount FROM trades', fetchall=True) or []
    prices = _live_prices_for_open_trades(open_trades, timeout=8)
    open_mtm_usd = 0.0
    for t in open_trades:
        entry = _ctx.safe_float(t.get('entry'), 0.0)
        amount = _ctx.safe_float(t.get('amount'), 0.0)
        current = _ctx.safe_float(prices.get(t['symbol']), 0.0) if prices else 0.0
        if entry > 0 and amount > 0 and current > 0:
            open_mtm_usd += (current - entry) * amount
    mtm_equity = equity + open_mtm_usd
    current_dd_pct = (peak - mtm_equity) / peak * 100 if peak > 0 and mtm_equity < peak else 0.0
    if open_trades and prices:
        points.append({'label': 'Now (MTM)', 'timestamp': time.time(),
                       'equity': round(mtm_equity, 2), 'pnl_usd': round(open_mtm_usd, 2),
                       'symbol': 'OPEN_MTM', 'r_multiple': 0.0,
                       'drawdown_pct': round(current_dd_pct, 2), 'is_mtm': True})
    return jsonify({'starting_cash': starting, 'realized_equity': round(equity, 2),
                    'mtm_equity': round(mtm_equity, 2), 'open_mtm_usd': round(open_mtm_usd, 2),
                    'open_position_count': len(open_trades), 'live_prices_available': bool(prices),
                    'current_drawdown_pct': round(current_dd_pct, 2),
                    'max_drawdown_pct': round(max_dd_pct, 2), 'peak_equity': round(peak, 2),
                    'trade_count': len(rows), 'points': points})


@dashboard_bp.route('/diagnostics')
def diagnostics_endpoint():
    since = time.time() - 24 * 3600
    top = _ctx.db_query(
        '''SELECT signal_type, rejection_reason, COUNT(*) as count
           FROM signal_rejections WHERE timestamp >= ?
           GROUP BY signal_type, rejection_reason ORDER BY count DESC LIMIT 20''',
        (since,), fetchall=True)
    rejected_total = _ctx.db_query(
        'SELECT COUNT(*) as c FROM signal_rejections WHERE timestamp >= ?', (since,), fetchone=True)
    trades_24h = _ctx.db_query(
        'SELECT COUNT(*) as c FROM trade_history WHERE entry_time >= ?', (since,), fetchone=True)
    return jsonify({'window': '24h',
                    'rejections_total_24h': rejected_total['c'] if rejected_total else 0,
                    'trades_opened_24h': trades_24h['c'] if trades_24h else 0,
                    'quiet_mode_multiplier': _ctx.get_quiet_multiplier(),
                    'top_rejection_reasons': top if top else []})


@dashboard_bp.route('/positions')
def positions_endpoint():
    trades = _ctx.db_query('SELECT * FROM trades', fetchall=True) or []
    prices = _live_prices_for_open_trades(trades, timeout=8)
    now = time.time()
    result = []
    total_unreal_usd = 0.0
    for t in trades:
        symbol = t['symbol']
        entry = _ctx.safe_float(t.get('entry'), 0.0)
        sl = _ctx.safe_float(t.get('sl'), 0.0)
        amount = _ctx.safe_float(t.get('amount'), 0.0)
        age_hrs = (now - t['entry_time']) / 3600
        current = _ctx.safe_float(prices.get(symbol), 0.0) if prices else 0.0
        unreal_pct = unreal_r = unreal_usd = None
        if current > 0 and entry > 0:
            unreal_pct = round((current - entry) / entry * 100, 2)
            unreal_usd_val = (current - entry) * amount
            unreal_usd = round(unreal_usd_val, 2)
            total_unreal_usd += unreal_usd_val
            risk_per_unit = entry - sl
            if risk_per_unit > 0:
                unreal_r = round((current - entry) / risk_per_unit, 2)
        result.append({'symbol': symbol, 'signal_type': t.get('signal_type'),
                       'sector': _ctx.get_sector_for_symbol(symbol),
                       'entry': entry, 'sl': sl,
                       'tp': t['tp'] if t['tp'] > 0 else 'trailing',
                       'amount': amount, 'age_hours': round(age_hrs, 1),
                       'past_max_hold': age_hrs > (_ctx.max_hold_time_sec / 3600),
                       'partial_exited': bool(t['partial_exited']),
                       'risk_dollars': t.get('risk_dollars', 0),
                       'current_price': round(current, 6) if current > 0 else None,
                       'unreal_pct': unreal_pct, 'unreal_r': unreal_r, 'unreal_usd': unreal_usd})
    return jsonify({'count': len(result), 'max_hold_hours': _ctx.max_hold_time_sec / 3600,
                    'max_per_sector': _ctx.max_positions_per_sector,
                    'open_unreal_usd': round(total_unreal_usd, 2),
                    'live_prices_available': bool(prices), 'positions': result})


@dashboard_bp.route('/events')
def list_events():
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    return jsonify(_ctx.db_query(
        'SELECT * FROM event_calendar WHERE event_date >= ? ORDER BY event_date',
        (today,), fetchall=True) or [])


# =============================================================================
# DATA ANALYSIS ENDPOINTS
# =============================================================================
def _rows_as_dicts(rows):
    result = []
    for row in rows or []:
        if isinstance(row, dict): result.append(row)
        else:
            try: result.append(dict(row))
            except Exception: result.append({})
    return result


def _trade_history(limit=500, since=None):
    if since is not None:
        rows = _ctx.db_query(
            'SELECT * FROM trade_history WHERE exit_time >= ? ORDER BY id DESC LIMIT ?',
            (since, limit), fetchall=True) or []
    else:
        rows = _ctx.db_query(
            'SELECT * FROM trade_history ORDER BY id DESC LIMIT ?', (limit,), fetchall=True) or []
    return _rows_as_dicts(rows)


def _trade_analysis_rows(limit=500, since=None):
    cache_key = ('trade_analysis_rows', limit, since, _latest_trade_id())

    def _query():
        try:
            if since is not None:
                rows = _ctx.db_query(
                    '''SELECT th.*, ta.hour_of_day, ta.day_of_week,
                              ta.regime_at_entry, ta.btc_atr_ratio, ta.fear_index
                       FROM trade_history th
                       LEFT JOIN trade_analysis ta ON th.id = ta.trade_history_id
                       WHERE th.exit_time >= ? ORDER BY th.id DESC LIMIT ?''',
                    (since, limit), fetchall=True) or []
            else:
                rows = _ctx.db_query(
                    '''SELECT th.*, ta.hour_of_day, ta.day_of_week,
                              ta.regime_at_entry, ta.btc_atr_ratio, ta.fear_index
                       FROM trade_history th
                       LEFT JOIN trade_analysis ta ON th.id = ta.trade_history_id
                       ORDER BY th.id DESC LIMIT ?''',
                    (limit,), fetchall=True) or []
            return _rows_as_dicts(rows)
        except Exception as e:
            logger.warning(f"analytics join failed, falling back: {e}")
            return _trade_history(limit, since)

    return _cached(cache_key, ttl=60, func=_query)


def _r_value(row): return _ctx.safe_float(row.get('r_multiple'), 0.0)
def _pnl_pct(row): return _ctx.safe_float(row.get('pnl_pct'), 0.0)
def _fees(row): return _ctx.safe_float(row.get('total_fees'), 0.0)


def _group_performance(rows, key_func):
    groups = {}
    for row in rows:
        key = str(key_func(row) or 'Unknown')
        if key not in groups:
            groups[key] = {'name': key, 'count': 0, 'wins': 0, 'losses': 0, 'breakeven': 0,
                           'r_sum': 0.0, 'gross_profit_r': 0.0, 'gross_loss_r': 0.0,
                           'pnl_pct_sum': 0.0, 'fees_sum': 0.0}
        r = _r_value(row)
        g = groups[key]
        g['count'] += 1; g['r_sum'] += r; g['pnl_pct_sum'] += _pnl_pct(row); g['fees_sum'] += _fees(row)
        if r > 0: g['wins'] += 1; g['gross_profit_r'] += r
        elif r < 0: g['losses'] += 1; g['gross_loss_r'] += r
        else: g['breakeven'] += 1
    result = []
    for g in groups.values():
        c = g['count']
        gross_loss_abs = abs(g['gross_loss_r'])
        pf = None if gross_loss_abs == 0 else g['gross_profit_r'] / gross_loss_abs
        result.append({'name': g['name'], 'count': c, 'wins': g['wins'],
                       'losses': g['losses'], 'breakeven': g['breakeven'],
                       'win_rate': round((g['wins'] / c) * 100, 1) if c else 0.0,
                       'expectancy_r': round(g['r_sum'] / c, 3) if c else 0.0,
                       'total_r': round(g['r_sum'], 3),
                       'profit_factor': round(pf, 2) if pf is not None else None,
                       'avg_pnl_pct': round(g['pnl_pct_sum'] / c, 3) if c else 0.0,
                       'total_fees': round(g['fees_sum'], 4)})
    return sorted(result, key=lambda x: (x['expectancy_r'], x['count']), reverse=True)


def _bucket_r_multiple(r):
    if r <= -2: return '<= -2R'
    if r <= -1: return '-2R to -1R'
    if r < 0: return '-1R to 0R'
    if r == 0: return '0R'
    if r < 1: return '0R to 1R'
    if r < 2: return '1R to 2R'
    return '>= 2R'


def _make_csv_response(filename, rows, fieldnames):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    for row in rows: writer.writerow(row)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename={filename}'})


@dashboard_bp.route('/api/analytics/overview')
def analytics_overview_api():
    rows = _trade_analysis_rows(500, _request_since())
    count = len(rows)
    if count == 0:
        return jsonify({'trade_count': 0, 'win_rate': 0, 'expectancy_r': 0, 'total_r': 0,
                        'profit_factor': None, 'total_fees': 0, 'avg_hold_hours': None,
                        'insights': ['No closed trades yet.']})
    r_values = [_r_value(r) for r in rows]
    wins = sum(1 for r in r_values if r > 0)
    losses = sum(1 for r in r_values if r < 0)
    gross_profit = sum(r for r in r_values if r > 0)
    gross_loss = abs(sum(r for r in r_values if r < 0))
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    total_fees = sum(_fees(r) for r in rows)
    hold_hours = []
    for row in rows:
        entry_ts = _ctx.safe_float(row.get('entry_time'), 0.0)
        exit_ts = _ctx.safe_float(row.get('exit_time'), 0.0)
        if entry_ts > 0 and exit_ts > entry_ts:
            hold_hours.append((exit_ts - entry_ts) / 3600.0)
    by_signal = _group_performance(rows, lambda r: r.get('signal_type'))
    by_regime = _group_performance(rows, lambda r: r.get('regime_at_entry') or r.get('regime'))
    exit_reasons = _group_performance(rows, lambda r: r.get('exit_reason'))
    insights = [f"Overall expectancy is {round(sum(r_values)/count,2)}R across {count} closed trades."]
    if by_signal:
        best = by_signal[0]
        weakest = sorted(by_signal, key=lambda x: x['expectancy_r'])[0]
        insights.append(f"Best signal: {best['name']} with {best['expectancy_r']}R expectancy over {best['count']} trades.")
        if weakest['expectancy_r'] < 0:
            insights.append(f"Review signal: {weakest['name']} is weakest at {weakest['expectancy_r']}R.")
    if by_regime:
        best_regime = by_regime[0]
        insights.append(f"Best regime: {best_regime['name']} at {best_regime['expectancy_r']}R expectancy.")
    if exit_reasons:
        most_common = sorted(exit_reasons, key=lambda x: x['count'], reverse=True)[0]
        insights.append(f"Most common exit: {most_common['name']} ({most_common['count']} trades).")
    return jsonify({'trade_count': count, 'wins': wins, 'losses': losses,
                    'win_rate': round((wins/count)*100, 1),
                    'expectancy_r': round(sum(r_values)/count, 3),
                    'total_r': round(sum(r_values), 3),
                    'profit_factor': round(profit_factor, 2) if profit_factor is not None else None,
                    'total_fees': round(total_fees, 4), 'avg_fee': round(total_fees/count, 4),
                    'avg_hold_hours': round(sum(hold_hours)/len(hold_hours), 2) if hold_hours else None,
                    'insights': insights})


@dashboard_bp.route('/api/analytics/by_signal')
def analytics_by_signal_api():
    rows = _trade_analysis_rows(500, _request_since())
    return jsonify(_group_performance(rows, lambda r: r.get('signal_type')))


@dashboard_bp.route('/api/analytics/by_regime')
def analytics_by_regime_api():
    rows = _trade_analysis_rows(500, _request_since())
    return jsonify(_group_performance(rows, lambda r: r.get('regime_at_entry') or r.get('regime')))


@dashboard_bp.route('/api/analytics/exit_reasons')
def analytics_exit_reasons_api():
    rows = _trade_analysis_rows(500, _request_since())
    return jsonify(sorted(_group_performance(rows, lambda r: r.get('exit_reason')),
                          key=lambda x: x['count'], reverse=True))


@dashboard_bp.route('/api/analytics/time_performance')
def analytics_time_performance_api():
    rows = _trade_analysis_rows(500, _request_since())
    day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    def hour_key(row):
        h = row.get('hour_of_day')
        if h is None or h == '':
            ts = _ctx.safe_float(row.get('exit_time'), 0.0)
            return datetime.datetime.fromtimestamp(ts).hour if ts > 0 else 'Unknown'
        return int(_ctx.safe_float(h, 0))

    def day_key(row):
        d = row.get('day_of_week')
        if isinstance(d, str) and d and not d.isdigit():
            return d[:3]
        if d is not None and d != '':
            idx = int(_ctx.safe_float(d, 0))
            if 0 <= idx <= 6: return day_names[idx]
        ts = _ctx.safe_float(row.get('exit_time'), 0.0)
        return day_names[datetime.datetime.fromtimestamp(ts).weekday()] if ts > 0 else 'Unknown'

    by_hour = sorted(_group_performance(rows, hour_key),
                     key=lambda x: int(x['name']) if str(x['name']).isdigit() else 99)
    by_day = sorted(_group_performance(rows, day_key),
                    key=lambda x: day_names.index(x['name']) if x['name'] in day_names else 99)
    return jsonify({'by_hour': by_hour, 'by_day': by_day})


@dashboard_bp.route('/api/analytics/rejection_summary')
def analytics_rejection_summary_api():
    user_since = _request_since()
    since = user_since if user_since is not None else time.time() - 7 * 24 * 3600
    rows = _rows_as_dicts(_ctx.db_query(
        '''SELECT signal_type, rejection_reason, COUNT(*) as count
           FROM signal_rejections WHERE timestamp >= ?
           GROUP BY signal_type, rejection_reason ORDER BY count DESC''',
        (since,), fetchall=True) or [])
    by_reason = {}
    by_signal = {}
    total = 0
    for row in rows:
        reason = row.get('rejection_reason') or 'Unknown'
        signal = row.get('signal_type') or 'Unknown'
        count = int(_ctx.safe_float(row.get('count'), 0))
        total += count
        by_reason[reason] = by_reason.get(reason, 0) + count
        by_signal[signal] = by_signal.get(signal, 0) + count
    return jsonify({'window': '7d', 'total_rejections': total, 'rows': rows[:50],
                    'by_reason': [{'name': k, 'count': v} for k, v in sorted(by_reason.items(), key=lambda x: x[1], reverse=True)],
                    'by_signal': [{'name': k, 'count': v} for k, v in sorted(by_signal.items(), key=lambda x: x[1], reverse=True)]})


@dashboard_bp.route('/api/analytics/distributions')
def analytics_distributions_api():
    rows = _trade_analysis_rows(500, _request_since())
    r_buckets = {}
    pnl_buckets = {'<= -5%': 0, '-5% to -2%': 0, '-2% to 0%': 0, '0%': 0,
                   '0% to 2%': 0, '2% to 5%': 0, '>= 5%': 0}
    for row in rows:
        r = _r_value(row)
        r_buckets[_bucket_r_multiple(r)] = r_buckets.get(_bucket_r_multiple(r), 0) + 1
        pnl = _pnl_pct(row)
        if pnl <= -5: pnl_buckets['<= -5%'] += 1
        elif pnl <= -2: pnl_buckets['-5% to -2%'] += 1
        elif pnl < 0: pnl_buckets['-2% to 0%'] += 1
        elif pnl == 0: pnl_buckets['0%'] += 1
        elif pnl < 2: pnl_buckets['0% to 2%'] += 1
        elif pnl < 5: pnl_buckets['2% to 5%'] += 1
        else: pnl_buckets['>= 5%'] += 1
    r_order = ['<= -2R', '-2R to -1R', '-1R to 0R', '0R', '0R to 1R', '1R to 2R', '>= 2R']
    return jsonify({'r_multiple': [{'bucket': k, 'count': r_buckets.get(k, 0)} for k in r_order],
                    'pnl_pct': [{'bucket': k, 'count': v} for k, v in pnl_buckets.items()]})


@dashboard_bp.route('/api/analytics/heatmap')
def analytics_heatmap_api():
    rows = _trade_analysis_rows(500, _request_since())
    day_short = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    def _hour(row):
        h = row.get('hour_of_day')
        if h is None or h == '':
            ts = _ctx.safe_float(row.get('exit_time'), 0.0)
            return datetime.datetime.fromtimestamp(ts).hour if ts > 0 else None
        try: return int(_ctx.safe_float(h, -1))
        except (TypeError, ValueError): return None

    def _day(row):
        d = row.get('day_of_week')
        if isinstance(d, str) and d:
            short = d[:3]
            if short in day_short: return day_short.index(short)
            if d.isdigit(): return int(d)
            return None
        if d is not None and d != '':
            try: return int(_ctx.safe_float(d, -1))
            except (TypeError, ValueError): return None
        ts = _ctx.safe_float(row.get('exit_time'), 0.0)
        return datetime.datetime.fromtimestamp(ts).weekday() if ts > 0 else None

    grid = {}
    for row in rows:
        h = _hour(row); d = _day(row)
        if h is None or d is None or not (0 <= h < 24) or not (0 <= d < 7): continue
        cell = grid.setdefault((d, h), {'count': 0, 'r_sum': 0.0, 'wins': 0})
        r = _r_value(row); cell['count'] += 1; cell['r_sum'] += r
        if r > 0: cell['wins'] += 1
    cells = [{'day': d, 'day_name': day_short[d], 'hour': h, 'count': data['count'],
               'expectancy_r': round(data['r_sum']/data['count'], 3) if data['count'] else 0,
               'win_rate': round(data['wins']/data['count']*100, 1) if data['count'] else 0}
              for (d, h), data in grid.items()]
    return jsonify({'cells': cells, 'days': day_short})


@dashboard_bp.route('/api/export/trades.csv')
def export_trades_csv():
    rows = _trade_analysis_rows(2000)
    fields = ['symbol', 'signal_type', 'entry', 'exit_price', 'pnl_pct', 'r_multiple',
              'exit_reason', 'entry_time', 'exit_time', 'total_fees', 'regime',
              'hour_of_day', 'day_of_week', 'regime_at_entry', 'btc_atr_ratio', 'fear_index']
    return _make_csv_response('sniperbot_trades.csv', rows, fields)


@dashboard_bp.route('/api/export/signal_analysis.csv')
def export_signal_analysis_csv():
    rows = _group_performance(_trade_analysis_rows(2000), lambda r: r.get('signal_type'))
    fields = ['name', 'count', 'wins', 'losses', 'breakeven', 'win_rate',
              'expectancy_r', 'total_r', 'profit_factor', 'avg_pnl_pct', 'total_fees']
    return _make_csv_response('sniperbot_signal_analysis.csv', rows, fields)


@dashboard_bp.route('/api/export/rejections.csv')
def export_rejections_csv():
    rows = _rows_as_dicts(_ctx.db_query(
        'SELECT * FROM signal_rejections ORDER BY timestamp DESC LIMIT 2000', fetchall=True) or [])
    fields = ['timestamp', 'signal_type', 'rejection_reason', 'symbol', 'regime', 'price']
    if rows: fields = fields + [k for k in rows[0].keys() if k not in fields]
    return _make_csv_response('sniperbot_rejections.csv', rows, fields)


# =============================================================================
# ACTION ENDPOINTS (auth required)
# =============================================================================
@dashboard_bp.route('/force_exit', methods=['POST'])
@require_auth
def force_exit():
    data = flask_request.get_json() or {}
    symbol = data.get('symbol')
    if not symbol:
        return jsonify({'error': 'requires symbol'}), 400
    trade = _ctx.db_query('SELECT * FROM trades WHERE symbol = ?', (symbol,), fetchone=True)
    if not trade:
        return jsonify({'error': 'no open position for symbol'}), 404

    async def _do_exit():
        price = await _ctx.broker.get_current_price(symbol)
        if price is None: return {'error': 'cannot fetch price'}
        success = await _ctx.execute_exit(trade, "MANUAL_FORCE", price, _ctx.get_regime_detector())
        return {'success': success, 'price': price}

    try:
        result = run_async_in_loop(_do_exit(), timeout=30)
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"Force exit endpoint: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@dashboard_bp.route('/mode', methods=['POST'])
@require_auth
def set_mode():
    data = flask_request.get_json() or {}
    new_mode = data.get('mode', '').upper()
    if new_mode not in ('PAPER', 'LIVE'):
        return jsonify({'error': 'mode must be PAPER or LIVE'}), 400
    if new_mode == 'LIVE' and not data.get('confirm_live'):
        return jsonify({'error': 'LIVE requires confirm_live: true'}), 400
    try:
        _ctx.broker.set_mode(new_mode)
        _ctx.set_bot_state('reconcile_blocked', '1')
        _ctx.send_telegram("⚠️ *Reconciliation required after mode switch*")
        return jsonify({'status': 'ok', 'mode': _ctx.broker.mode, 'action': 'reconcile_required'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@dashboard_bp.route('/reconcile', methods=['POST'])
@require_auth
def trigger_reconcile():
    try:
        result = run_async_in_loop(_ctx.broker.reconcile_positions(), timeout=60)
        _ctx.set_bot_state('reconcile_blocked', '0' if not result['errors'] else '1')
        return jsonify(result), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@dashboard_bp.route('/unblock', methods=['POST'])
@require_auth
def manual_unblock():
    _ctx.set_bot_state('reconcile_blocked', '0')
    _ctx.send_telegram("✅ *Reconcile manually unblocked*")
    return jsonify({'status': 'unblocked'}), 200


@dashboard_bp.route('/pause', methods=['POST'])
@require_auth
def pause_trading():
    _ctx.db_query('UPDATE portfolio SET trading_paused = 1 WHERE id=1', commit=True)
    _ctx.send_telegram("⏸ *Trading paused* — no new entries. Open positions still managed.")
    return jsonify({'status': 'paused'}), 200


@dashboard_bp.route('/resume', methods=['POST'])
@require_auth
def resume_trading():
    _ctx.db_query('UPDATE portfolio SET trading_paused = 0 WHERE id=1', commit=True)
    _ctx.send_telegram("▶️ *Trading resumed*")
    return jsonify({'status': 'resumed'}), 200


@dashboard_bp.route('/event', methods=['POST'])
@require_auth
def add_event():
    data = flask_request.get_json()
    if not data or 'date' not in data or 'name' not in data:
        return jsonify({'error': 'Requires date and name'}), 400
    _ctx.db_query(
        'INSERT INTO event_calendar (event_date, event_name, impact) VALUES (?, ?, ?)',
        (data['date'], data['name'], data.get('impact', 'HIGH')), commit=True)
    return jsonify({'status': 'ok'}), 201


@dashboard_bp.route('/reset', methods=['POST'])
@require_auth
def reset_trades():
    if _ctx.broker.mode == 'LIVE':
        return jsonify({'error': 'Reset is blocked in LIVE mode. Switch to PAPER first.'}), 403
    with _ctx.db_lock:
        c = _ctx.db_conn.cursor()
        for t in ['portfolio', 'trades', 'cooldowns', 'pending_signals',
                   'regime_history', 'accumulation', 'signal_status',
                   'signal_rejections', 'bot_state', 'order_log',
                   'order_intents', 'trade_analysis']:
            c.execute(f'DROP TABLE IF EXISTS {t}')
        _ctx.db_conn.commit()
    _ctx.init_db_func()
    _ctx.send_telegram(
        f"🔄 *{_ctx.version_string} RESET* [{_ctx.broker.mode}] | "
        f"Balance: ${_ctx.starting_cash}")
    return jsonify({'status': 'reset'}), 200
