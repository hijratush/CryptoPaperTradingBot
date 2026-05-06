"""
FastAPI-based real-time dashboard backend.

Endpoints:
  GET  /              → serve dashboard HTML
  GET  /api/status    → bot status snapshot
  GET  /api/portfolio → portfolio state
  GET  /api/trades    → trade history
  GET  /api/prices    → latest prices
  WS   /ws/live       → push live updates every second
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import Any

import orjson
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from src.core.config import settings
from src.core.event_bus import event_bus, Topics

log = logging.getLogger(__name__)


def create_app(engine, price_store, strategy, feed, kline_feed=None) -> FastAPI:
    """Factory: creates and configures the FastAPI app."""

    app = FastAPI(
        title="Crypto Paper Trading Bot",
        version="1.0.0",
        docs_url="/docs",
    )

    # ---- WebSocket connection manager ----
    class ConnectionManager:
        def __init__(self):
            self._clients: list[WebSocket] = []

        async def connect(self, ws: WebSocket) -> None:
            await ws.accept()
            self._clients.append(ws)
            log.debug(f"Dashboard WS client connected (total: {len(self._clients)})")

        def disconnect(self, ws: WebSocket) -> None:
            self._clients.remove(ws)
            log.debug(f"Dashboard WS client disconnected (total: {len(self._clients)})")

        async def broadcast(self, data: dict) -> None:
            if not self._clients:
                return
            raw = orjson.dumps(data).decode()
            dead = []
            for ws in self._clients:
                try:
                    await ws.send_text(raw)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self._clients.remove(ws)

    manager = ConnectionManager()

    # ---- Background pusher task ----
    async def push_loop():
        """Push dashboard updates to all connected WS clients every second."""
        while True:
            try:
                prices = price_store.get_all_prices()
                portfolio_dict = engine.portfolio.to_dict(prices)
                indicators = strategy.get_smc_snapshot()

                last_age = feed.last_message_age
                last_age_display = round(last_age, 1) if last_age != float("inf") else None
                kline_age = kline_feed.last_message_age if kline_feed else float("inf")
                kline_age_display = round(kline_age, 1) if kline_age != float("inf") else None

                feed_status = {
                    "connected": feed.is_connected,
                    "last_message_age": last_age_display,
                    "stale": last_age > settings.WS_STALE_THRESHOLD,
                    "kline_connected": kline_feed.is_connected if kline_feed else False,
                    "kline_last_message_age": kline_age_display,
                }

                payload = {
                    "type": "update",
                    "ts": time.time(),
                    "portfolio": portfolio_dict,
                    "prices": prices,
                    "indicators": indicators,
                    "feed": feed_status,
                    "config": {
                        "initial_balance": settings.INITIAL_USDT_BALANCE,
                        "symbols": settings.SYMBOLS,
                        "taker_fee_pct": round(settings.total_taker_fee * 100, 4),
                        "max_positions": settings.MAX_OPEN_POSITIONS,
                        "position_size_pct": settings.POSITION_SIZE_PCT * 100,
                        "cut_loss_pct": settings.CUT_LOSS_PCT * 100,
                        "tp_ratio": settings.DEFAULT_TP_RATIO,
                        "trailing_stop_pct": settings.TRAILING_STOP_PCT * 100,
                    },
                }
                await manager.broadcast(payload)
            except Exception as exc:
                log.error(f"Push loop error: {exc}", exc_info=True)
            await asyncio.sleep(1.0)

    @app.on_event("startup")
    async def startup():
        asyncio.create_task(push_loop(), name="dashboard-push")

    # ---- Routes ----

    @app.get("/", response_class=HTMLResponse)
    async def serve_dashboard():
        return HTMLResponse(content=_get_dashboard_html())

    @app.get("/api/status")
    async def api_status():
        prices = price_store.get_all_prices()
        return JSONResponse({
            "status": "running",
            "ts": time.time(),
            "ws_connected": feed.is_connected,
            "ws_last_message_age": round(feed.last_message_age, 1),
            "active_symbols": price_store.active_symbols,
            "portfolio_value": engine.portfolio.total_value(prices),
            "usdt_balance": engine.usdt_balance,
            "open_positions": len([p for p in engine.positions.values() if p.is_open]),
            "total_trades": len(engine.orders),
        })

    @app.get("/api/portfolio")
    async def api_portfolio():
        prices = price_store.get_all_prices()
        return JSONResponse(engine.portfolio.to_dict(prices))

    @app.get("/api/trades")
    async def api_trades(limit: int = 50):
        orders = engine.orders[-limit:]
        return JSONResponse([o.to_dict() for o in reversed(orders)])

    @app.get("/api/prices")
    async def api_prices():
        return JSONResponse(price_store.get_all_prices())

    @app.get("/api/indicators")
    async def api_indicators():
        return JSONResponse(strategy.get_smc_snapshot())

    @app.websocket("/ws/live")
    async def ws_live(websocket: WebSocket):
        await manager.connect(websocket)
        try:
            while True:
                # Keep alive — just wait for disconnect
                await websocket.receive_text()
        except WebSocketDisconnect:
            manager.disconnect(websocket)

    return app


def _get_dashboard_html() -> str:
    """Inline HTML dashboard with real-time updates."""
    return '''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Crypto Paper Trading Bot</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600;700&family=Syne:wght@400;600;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #0a0e1a;
      --panel: #0f1628;
      --border: #1e2d4a;
      --accent: #00d4ff;
      --green: #00ff88;
      --red: #ff3366;
      --yellow: #ffd700;
      --dim: #4a6080;
      --text: #c8deff;
      --mono: 'JetBrains Mono', monospace;
      --sans: 'Syne', sans-serif;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: var(--mono);
      min-height: 100vh;
      overflow-x: hidden;
    }
    /* Animated grid background */
    body::before {
      content: '';
      position: fixed; inset: 0;
      background-image:
        linear-gradient(rgba(0,212,255,0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0,212,255,0.03) 1px, transparent 1px);
      background-size: 40px 40px;
      pointer-events: none;
      z-index: 0;
    }
    .wrap { position: relative; z-index: 1; padding: 20px; max-width: 1600px; margin: 0 auto; }

    /* Header */
    header {
      display: flex; align-items: center; justify-content: space-between;
      padding: 16px 24px;
      border-bottom: 1px solid var(--border);
      background: rgba(15,22,40,0.8);
      backdrop-filter: blur(10px);
      position: sticky; top: 0; z-index: 100;
    }
    .logo { font-family: var(--sans); font-size: 1.3rem; font-weight: 800; letter-spacing: -0.02em; }
    .logo span { color: var(--accent); }
    .status-pill {
      display: flex; align-items: center; gap: 8px;
      padding: 6px 14px; border-radius: 20px;
      font-size: 0.75rem; font-weight: 600;
      border: 1px solid;
      transition: all 0.3s;
    }
    .status-pill.connected { color: var(--green); border-color: var(--green); background: rgba(0,255,136,0.08); }
    .status-pill.disconnected { color: var(--red); border-color: var(--red); background: rgba(255,51,102,0.08); }
    .status-pill.stale { color: var(--yellow); border-color: var(--yellow); background: rgba(255,215,0,0.08); }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; animation: pulse 2s infinite; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

    /* Grid layout */
    .grid { display: grid; gap: 16px; margin-top: 20px; }
    .grid-top { grid-template-columns: repeat(4, 1fr); }
    .grid-main { grid-template-columns: 1fr 1fr; }
    .grid-bottom { grid-template-columns: repeat(3, 1fr); }

    /* Panels */
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px;
      position: relative;
      overflow: hidden;
    }
    .panel::before {
      content: '';
      position: absolute; top: 0; left: 0; right: 0; height: 1px;
      background: linear-gradient(90deg, transparent, var(--accent), transparent);
      opacity: 0.4;
    }
    .panel-title {
      font-family: var(--sans); font-size: 0.7rem; font-weight: 600;
      text-transform: uppercase; letter-spacing: 0.12em;
      color: var(--dim); margin-bottom: 12px;
    }
    .metric {
      font-size: 1.8rem; font-weight: 700;
      font-family: var(--sans); letter-spacing: -0.03em;
    }
    .metric-sm { font-size: 1.1rem; }
    .metric-label { font-size: 0.7rem; color: var(--dim); margin-top: 4px; }
    .up { color: var(--green); }
    .down { color: var(--red); }
    .neutral { color: var(--accent); }

    /* Prices table */
    .price-grid {
      display: grid; grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .price-row {
      display: flex; justify-content: space-between; align-items: center;
      padding: 8px 12px;
      background: rgba(0,212,255,0.04);
      border: 1px solid var(--border);
      border-radius: 8px;
      transition: border-color 0.3s;
    }
    .price-row:hover { border-color: var(--accent); }
    .sym { font-size: 0.75rem; font-weight: 600; color: var(--accent); }
    .price-val { font-size: 0.85rem; font-weight: 600; }
    .price-rsi { font-size: 0.65rem; color: var(--dim); }

    /* Positions */
    .position-card {
      padding: 12px;
      border: 1px solid var(--border);
      border-radius: 8px;
      margin-bottom: 8px;
      background: rgba(0,212,255,0.03);
    }
    .pos-header { display: flex; justify-content: space-between; margin-bottom: 6px; }
    .pos-sym { font-size: 0.8rem; font-weight: 700; color: var(--accent); }
    .pos-pnl { font-size: 0.8rem; font-weight: 700; }
    .pos-details { display: flex; gap: 16px; font-size: 0.68rem; color: var(--dim); }

    /* Trades */
    .trade-row {
      display: flex; justify-content: space-between; align-items: center;
      padding: 8px 0;
      border-bottom: 1px solid rgba(30,45,74,0.5);
      font-size: 0.72rem;
    }
    .trade-row:last-child { border-bottom: none; }
    .trade-side-buy { color: var(--green); font-weight: 700; }
    .trade-side-sell { color: var(--red); font-weight: 700; }
    .trade-sym { color: var(--text); }
    .trade-fee { color: var(--dim); }

    /* Indicator bars */
    .ind-row { display: flex; align-items: center; gap: 10px; padding: 6px 0; border-bottom: 1px solid rgba(30,45,74,0.3); }
    .ind-sym { font-size: 0.7rem; color: var(--accent); width: 80px; flex-shrink: 0; }
    .ind-bar-wrap { flex: 1; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }
    .ind-bar { height: 100%; border-radius: 2px; transition: width 0.5s; }
    .bar-bull { background: var(--green); }
    .bar-bear { background: var(--red); }
    .ind-rsi { font-size: 0.65rem; width: 40px; text-align: right; }
    .ind-signal { font-size: 0.6rem; padding: 2px 6px; border-radius: 3px; }
    .sig-buy { background: rgba(0,255,136,0.15); color: var(--green); }
    .sig-sell { background: rgba(255,51,102,0.15); color: var(--red); }

    /* Scrollable */
    .scroll { max-height: 320px; overflow-y: auto; }
    .scroll::-webkit-scrollbar { width: 4px; }
    .scroll::-webkit-scrollbar-track { background: transparent; }
    .scroll::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }

    /* Empty state */
    .empty { color: var(--dim); font-size: 0.75rem; text-align: center; padding: 20px; }

    /* Timestamp */
    .ts { font-size: 0.65rem; color: var(--dim); text-align: right; margin-top: 16px; }

    @media (max-width: 1200px) {
      .grid-top { grid-template-columns: repeat(2, 1fr); }
      .grid-main { grid-template-columns: 1fr; }
      .grid-bottom { grid-template-columns: 1fr 1fr; }
    }
    @media (max-width: 640px) {
      .grid-top, .grid-bottom { grid-template-columns: 1fr; }
      .price-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
<header>
  <div class="logo">PAPER<span>TRADE</span> <span style="color:var(--dim);font-size:0.65rem">SPOT · USDT</span></div>
  <div id="statusPill" class="status-pill disconnected">
    <div class="dot"></div>
    <span id="statusText">Connecting…</span>
  </div>
</header>

<div class="wrap">
  <!-- Top KPIs -->
  <div class="grid grid-top">
    <div class="panel">
      <div class="panel-title">Total Portfolio Value</div>
      <div class="metric neutral" id="totalValue">—</div>
      <div class="metric-label">USDT</div>
    </div>
    <div class="panel">
      <div class="panel-title">USDT Balance (Free)</div>
      <div class="metric" id="usdtBalance">—</div>
      <div class="metric-label" id="pnlLabel">—</div>
    </div>
    <div class="panel">
      <div class="panel-title">Open Positions</div>
      <div class="metric neutral" id="openPositions">0</div>
      <div class="metric-label" id="totalTrades">0 total trades</div>
    </div>
    <div class="panel">
      <div class="panel-title">Fees Paid</div>
      <div class="metric down" id="feesPaid">0.0000</div>
      <div class="metric-label" id="feeRate">Taker fee: —%</div>
    </div>
  </div>

  <!-- Main content -->
  <div class="grid grid-main" style="margin-top:16px">
    <!-- Prices -->
    <div class="panel">
      <div class="panel-title">Live Prices · USDT Pairs</div>
      <div class="price-grid" id="priceGrid">
        <div class="empty">Waiting for data…</div>
      </div>
    </div>
    <!-- Positions -->
    <div class="panel">
      <div class="panel-title">Open Positions</div>
      <div class="scroll" id="positionsDiv">
        <div class="empty">No open positions</div>
      </div>
    </div>
  </div>

  <!-- Bottom row -->
  <div class="grid grid-bottom" style="margin-top:16px">
    <!-- Indicators -->
    <div class="panel">
      <div class="panel-title">SMC Analysis · HTF Bias & Signals</div>
      <div id="indicatorDiv">
        <div class="empty">Warming up…</div>
      </div>
    </div>
    <!-- Trades -->
    <div class="panel" style="grid-column: span 2">
      <div class="panel-title">Recent Trades</div>
      <div class="scroll" id="tradesDiv">
        <div class="empty">No trades yet</div>
      </div>
    </div>
  </div>

  <div class="ts" id="lastUpdate">Last update: —</div>
</div>

<script>
  const WS_URL = `ws://${location.host}/ws/live`;
  let ws, reconnectTimer;
  let trades = [];

  function connect() {
    ws = new WebSocket(WS_URL);
    ws.onopen = () => {
      setStatus('connected', 'Live');
      clearTimeout(reconnectTimer);
    };
    ws.onmessage = (e) => {
      try { render(JSON.parse(e.data)); } catch {}
    };
    ws.onclose = ws.onerror = () => {
      setStatus('disconnected', 'Reconnecting…');
      reconnectTimer = setTimeout(connect, 3000);
    };
  }

  function setStatus(cls, label) {
    const pill = document.getElementById('statusPill');
    pill.className = 'status-pill ' + cls;
    document.getElementById('statusText').textContent = label;
  }

  function fmt(n, d=4) { return Number(n).toLocaleString('en-US', {minimumFractionDigits:d, maximumFractionDigits:d}); }
  function fmtPct(n) { const s = (n>=0?'+':'')+Number(n).toFixed(2)+'%'; return s; }

  function render(data) {
    if (data.type !== 'update') return;
    const p = data.portfolio;
    const prices = data.prices || {};
    const feed = data.feed || {};
    const cfg = data.config || {};

    // Status pill
    if (!feed.connected) setStatus('disconnected','Disconnected');
    else if (feed.stale) setStatus('stale', `Stale (${feed.last_message_age}s)`);
    else setStatus('connected','Live ·' + feed.last_message_age + 's');

    // KPIs
    const tv = parseFloat(p.total_value||0);
    const initBal = cfg.initial_balance || 100;
    const pnl = tv - initBal;
    const pnlPct = (pnl/initBal)*100;
    document.getElementById('totalValue').textContent = fmt(tv, 2);
    document.getElementById('totalValue').className = 'metric ' + (tv >= initBal ? 'up' : 'down');
    document.getElementById('usdtBalance').textContent = fmt(p.usdt_balance, 2);
    document.getElementById('pnlLabel').innerHTML = `PnL: <span class="${pnl>=0?'up':'down'}">${fmtPct(pnlPct)} (${(pnl>=0?'+':'')+fmt(pnl,4)} USDT)</span>`;
    document.getElementById('openPositions').textContent = Object.keys(p.positions||{}).length;
    document.getElementById('totalTrades').textContent = (p.total_trades||0) + ' total trades';
    document.getElementById('feesPaid').textContent = fmt(p.total_fees_paid||0, 4);
    document.getElementById('feeRate').textContent = `Taker fee: ${cfg.taker_fee_pct||'—'}%`;

    // Prices grid
    const pg = document.getElementById('priceGrid');
    if (Object.keys(prices).length) {
      pg.innerHTML = Object.entries(prices).map(([sym, price]) => {
        const ind = (data.indicators||{})[sym] || {};
        const bias = ind.htf_bias || 'NEUTRAL';
        const biasColor = bias==='BULLISH'?'var(--green)':bias==='BEARISH'?'var(--red)':'var(--dim)';
        const ema13 = ind.ema13_ltf ? ind.ema13_ltf.toFixed(2) : '—';
        const ema21 = ind.ema21_ltf ? ind.ema21_ltf.toFixed(2) : '—';
        return `<div class="price-row">
          <div>
            <div class="sym">${sym}</div>
            <div class="price-rsi" style="color:${biasColor}">${bias}</div>
          </div>
          <div style="text-align:right">
            <div class="price-val">${price > 1000 ? fmt(price,2) : price > 1 ? fmt(price,4) : fmt(price,6)}</div>
            <div class="price-rsi">E13 ${ema13} / E21 ${ema21}</div>
          </div>
        </div>`;
      }).join('');
    }

    // Positions — show SL/TP/trailing
    const pd = document.getElementById('positionsDiv');
    const positions = p.positions || {};
    if (Object.keys(positions).length) {
      pd.innerHTML = Object.entries(positions).map(([sym, pos]) => {
        const pnl = pos.unrealized_pnl || 0;
        const pnlPct = pos.unrealized_pnl_pct || 0;
        const sl = pos.stop_loss ? fmt(pos.stop_loss,4) : '—';
        const tp = pos.take_profit ? fmt(pos.take_profit,4) : '—';
        const trail = pos.trailing_stop_pct ? pos.trailing_stop_pct.toFixed(2)+'%' : '—';
        const partial = pos.partial_closed ? ' <span style="color:var(--yellow);font-size:0.65rem">½ CLOSED</span>' : '';
        return `<div class="position-card">
          <div class="pos-header">
            <span class="pos-sym">${sym}${partial}</span>
            <span class="pos-pnl ${pnl>=0?'up':'down'}">${fmtPct(pnlPct)} (${(pnl>=0?'+':'')+fmt(pnl,4)})</span>
          </div>
          <div class="pos-details">
            <span>qty: ${Number(pos.quantity).toFixed(6)}</span>
            <span>avg: ${fmt(pos.avg_buy_price,4)}</span>
            <span>cur: ${fmt(pos.current_price,4)}</span>
            <span>val: ${fmt(pos.current_value,2)} USDT</span>
          </div>
          <div class="pos-details" style="margin-top:4px;color:var(--dim)">
            <span style="color:var(--red)">SL ${sl}</span>
            <span style="color:var(--green)">TP ${tp}</span>
            <span>Trail ${trail}</span>
            <span>${pos.setup_note ? pos.setup_note.split('|')[0].trim() : ''}</span>
          </div>
        </div>`;
      }).join('');
    } else {
      pd.innerHTML = '<div class="empty">No open positions</div>';
    }

    // SMC Indicators panel
    const id = document.getElementById('indicatorDiv');
    const inds = data.indicators || {};
    // const cfg = data.config || {};
    if (Object.keys(inds).length) {
      id.innerHTML = Object.entries(inds).map(([sym, ind]) => {
        const bias = ind.htf_bias || 'NEUTRAL';
        const biasColor = bias==='BULLISH'?'var(--green)':bias==='BEARISH'?'var(--red)':'var(--dim)';
        const valid = ind.htf_valid ? '' : ' <span style="color:var(--dim);font-size:0.6rem">(warming up)</span>';
        const sig = ind.last_signal;
        const obCount = ind.ob_count || 0;
        const fvgCount = ind.fvg_count || 0;
        const ote = ind.ote_zone ? `OTE ${fmt(ind.ote_zone.low,2)}–${fmt(ind.ote_zone.high,2)}` : '';
        const sb = (ind.structure_breaks||[]).slice(-1)[0];
        const sbLabel = sb ? `${sb.choch?'CHoCH':'BoS'} ${sb.dir}` : '—';
        return `<div class="ind-row">
          <span class="ind-sym">${sym.replace('USDT','')}</span>
          <span style="color:${biasColor};font-size:0.7rem;font-weight:700;min-width:64px">${bias}${valid}</span>
          <span style="color:var(--dim);font-size:0.65rem">OB:${obCount} FVG:${fvgCount}</span>
          <span style="color:var(--yellow);font-size:0.65rem">${ote}</span>
          <span style="color:var(--accent);font-size:0.65rem">${sbLabel}</span>
          ${sig ? `<span class="ind-signal ${sig==='buy'?'sig-buy':'sig-sell'}">${sig.toUpperCase()}</span>` : ''}
        </div>`;
      }).join('') + `<div style="margin-top:12px;padding-top:10px;border-top:1px solid var(--border);font-size:0.65rem;color:var(--dim);display:flex;gap:16px;flex-wrap:wrap">
        <span>Max pos: <b style="color:var(--text)">${cfg.max_positions||4}</b></span>
        <span>Size: <b style="color:var(--text)">${cfg.position_size_pct||10}%</b></span>
        <span>SL: <b style="color:var(--red)">${cfg.cut_loss_pct||2}%</b></span>
        <span>TP: <b style="color:var(--green)">${cfg.tp_ratio||'1:2'}</b></span>
        <span>Trail: <b style="color:var(--yellow)">${cfg.trailing_stop_pct||1.5}%</b></span>
      </div>`;
    }

    // Trades — fetch separately
    fetchTrades();

    document.getElementById('lastUpdate').textContent = 'Last update: ' + new Date().toLocaleTimeString();
  }

  let lastTradeCount = 0;
  async function fetchTrades() {
    try {
      const r = await fetch('/api/trades?limit=20');
      const data = await r.json();
      if (data.length === lastTradeCount) return;
      lastTradeCount = data.length;
      const td = document.getElementById('tradesDiv');
      if (!data.length) { td.innerHTML = '<div class="empty">No trades yet</div>'; return; }
      td.innerHTML = data.map(t => {
        const isBuy = t.side === 'BUY';
        const ts = new Date(t.timestamp*1000).toLocaleTimeString();
        return `<div class="trade-row">
          <span class="trade-side-${isBuy?'buy':'sell'}">${t.side}</span>
          <span class="trade-sym">${t.symbol}</span>
          <span>${Number(t.quantity).toFixed(6)}</span>
          <span>@ ${fmt(t.price, 4)}</span>
          <span class="trade-fee">fee: ${fmt(t.fee_usdt,4)}</span>
          <span style="color:var(--dim);font-size:0.65rem">${ts}</span>
        </div>`;
      }).join('');
    } catch {}
  }

  connect();
</script>
</body>
</html>'''