"""
Historical Candle Seeder

Fetches OHLCV history via Tokocrypto REST API at startup and injects
into CandleStore so HTF/LTF analysis is immediately valid — no waiting
for WebSocket to accumulate candles over days/hours.

Endpoint: GET https://www.tokocrypto.site/api/v3/klines
Params  : symbol, interval, limit (max 1000)

Called once during boot, before WebSocket streams start.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp

from src.core.config import settings
from src.core.models import Candle
from src.data.candle_store import CandleStore

log = logging.getLogger(__name__)

# Tokocrypto public REST (symbol type 1 / MBX)
_REST_URL = "https://www.tokocrypto.site/api/v3/klines"

# Intraday timeframes needed by the new strategy
SEED_TIMEFRAMES = ["1h", "15m", "5m"]

# How many candles to fetch per (symbol, timeframe)
SEED_LIMIT = 200

# Tokocrypto interval strings (same as WS)
_TF_INTERVAL = {
    "1h": "1h",
    "15m": "15m",
    "5m": "5m",
}

# Polite delay between requests to avoid rate-limiting
_REQUEST_DELAY = 0.15   # seconds


async def _fetch_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    limit: int,
) -> Optional[list[dict]]:
    """
    Fetch raw kline data from REST API.

    Tokocrypto kline response (array of arrays):
    [
      [open_time, open, high, low, close, volume,
       close_time, quote_vol, trades, taker_buy_base, taker_buy_quote, ignore],
      ...
    ]
    """
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(_REST_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                log.warning(f"[Seeder] {symbol} {interval} HTTP {resp.status}")
                return None
            data = await resp.json()
            if not isinstance(data, list):
                log.warning(f"[Seeder] {symbol} {interval} unexpected response: {str(data)[:100]}")
                return None
            return data
    except asyncio.TimeoutError:
        log.warning(f"[Seeder] {symbol} {interval} timeout")
        return None
    except Exception as exc:
        log.warning(f"[Seeder] {symbol} {interval} error: {exc}")
        return None


def _parse_kline_row(symbol: str, timeframe: str, row: list) -> Optional[Candle]:
    """Convert raw kline array row to Candle object."""
    try:
        return Candle(
            symbol=symbol,
            timeframe=timeframe,
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            timestamp=float(row[0]) / 1000.0,  # ms → seconds
            closed=True,  # historical candles are always closed
        )
    except (IndexError, ValueError, TypeError) as exc:
        log.debug(f"[Seeder] parse error: {exc} | row={row!r}")
        return None


async def seed_candle_store(store: CandleStore) -> None:
    """
    Main entry point. Fetches historical candles for all configured
    symbols × timeframes and injects them into the CandleStore.

    Call this once at startup before starting WebSocket feeds.
    """
    symbols = settings.SYMBOLS
    total = len(symbols) * len(SEED_TIMEFRAMES)
    log.info(
        f"[Seeder] Starting — {len(symbols)} symbols × {SEED_TIMEFRAMES} "
        f"= {total} requests, {SEED_LIMIT} candles each"
    )
    t0 = time.time()
    success = 0
    failed = 0

    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            for tf in SEED_TIMEFRAMES:
                interval = _TF_INTERVAL[tf]
                rows = await _fetch_klines(session, symbol, interval, SEED_LIMIT)

                if rows is None:
                    log.warning(f"[Seeder] Failed: {symbol} {tf}")
                    failed += 1
                    continue

                # Exclude the last row — it's the current (open) candle
                closed_rows = rows[:-1]
                candles_added = 0

                for row in closed_rows:
                    candle = _parse_kline_row(symbol, tf, row)
                    if candle is None:
                        continue
                    # Inject directly (sync, no lock needed before WS starts)
                    k = (symbol, tf)
                    buf = store._candles[k]
                    if not buf or buf[-1].timestamp != candle.timestamp:
                        buf.append(candle)
                        candles_added += 1

                log.info(f"[Seeder] {symbol:10s} {tf:4s} → {candles_added} candles loaded")
                success += 1

                # Polite delay
                await asyncio.sleep(_REQUEST_DELAY)

    elapsed = time.time() - t0
    log.info(
        f"[Seeder] Done in {elapsed:.1f}s — "
        f"{success}/{total} OK, {failed} failed"
    )
