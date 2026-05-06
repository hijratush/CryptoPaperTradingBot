"""
CandleStore — in-memory OHLCV candle storage.

Stores historical + live candles per (symbol, timeframe).
Thread-safe via asyncio lock.

Used by SMCStrategy as the candle_provider:
    candle_store.get_candles(symbol, timeframe)  -> list[Candle]
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from typing import Optional

from src.core.models import Candle

log = logging.getLogger(__name__)

# Max candles kept in memory per (symbol, timeframe)
MAX_CANDLES = 500


class CandleStore:
    """
    Stores closed + current live candle per symbol/timeframe.

    Usage:
        store = CandleStore()
        store.apply_kline(symbol, timeframe, kline_data)   # from WS kline event
        candles = store.get_candles("BTCUSDT", "1d")       # returns list[Candle]
    """

    def __init__(self) -> None:
        # { (symbol, timeframe): deque[Candle] }
        self._candles: dict[tuple[str, str], deque[Candle]] = defaultdict(
            lambda: deque(maxlen=MAX_CANDLES)
        )
        # Live (not-yet-closed) candle per key
        self._live: dict[tuple[str, str], Optional[Candle]] = {}
        self._lock = asyncio.Lock()

    # ──────────────────────────────────────────────────────
    # Ingest
    # ──────────────────────────────────────────────────────

    async def apply_kline(
        self,
        symbol: str,
        timeframe: str,
        k: dict,
    ) -> None:
        """
        Process a kline payload (from Tokocrypto WS kline event).

        Expected keys in `k`:
            t  — kline open time (ms)
            o, h, l, c, v — OHLCV strings
            x  — bool, True if candle is closed
        """
        key = (symbol, timeframe)
        open_time = k["t"] / 1000.0
        candle = Candle(
            symbol=symbol,
            timeframe=timeframe,
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            timestamp=open_time,
            closed=bool(k["x"]),
        )

        async with self._lock:
            if candle.closed:
                # Push closed candle into history
                buf = self._candles[key]
                # Replace if same timestamp (duplicate from WS)
                if buf and buf[-1].timestamp == open_time:
                    buf[-1] = candle
                else:
                    buf.append(candle)
                self._live[key] = None
                log.debug(
                    f"[CANDLE CLOSED] {symbol} {timeframe} "
                    f"o={candle.open} h={candle.high} "
                    f"l={candle.low} c={candle.close}"
                )
            else:
                # Update live candle
                self._live[key] = candle

    def apply_kline_sync(self, symbol: str, timeframe: str, k: dict) -> None:
        """Synchronous version — call from non-async context if needed."""
        key = (symbol, timeframe)
        open_time = k["t"] / 1000.0
        candle = Candle(
            symbol=symbol,
            timeframe=timeframe,
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            timestamp=open_time,
            closed=bool(k["x"]),
        )
        buf = self._candles[key]
        if candle.closed:
            if buf and buf[-1].timestamp == open_time:
                buf[-1] = candle
            else:
                buf.append(candle)
            self._live[key] = None
        else:
            self._live[key] = candle

    # ──────────────────────────────────────────────────────
    # Query
    # ──────────────────────────────────────────────────────

    def get_candles(
        self,
        symbol: str,
        timeframe: str,
        include_live: bool = True,
    ) -> list[Candle]:
        """
        Return closed candles, optionally appending the current live candle.
        This is the candle_provider callable expected by SMCStrategy.
        """
        key = (symbol, timeframe)
        result = list(self._candles[key])
        if include_live:
            live = self._live.get(key)
            if live is not None:
                result.append(live)
        return result

    def get_latest_closed(self, symbol: str, timeframe: str) -> Optional[Candle]:
        key = (symbol, timeframe)
        buf = self._candles[key]
        return buf[-1] if buf else None

    def candle_count(self, symbol: str, timeframe: str) -> int:
        return len(self._candles[(symbol, timeframe)])

    def has_enough(self, symbol: str, timeframe: str, min_candles: int) -> bool:
        return self.candle_count(symbol, timeframe) >= min_candles

    def summary(self) -> dict:
        return {
            f"{sym}/{tf}": len(buf)
            for (sym, tf), buf in self._candles.items()
        }
