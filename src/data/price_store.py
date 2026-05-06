"""
In-memory price store + rolling tick history.
Provides latest prices to all consumers (strategy, dashboard, portfolio).
Also persists tick data to JSONL file.
"""
from __future__ import annotations
import asyncio
import logging
import time
from collections import defaultdict, deque
from typing import Optional

import aiofiles
import orjson

from src.core.config import settings
from src.core.event_bus import event_bus, Topics
from src.core.models import Tick

log = logging.getLogger(__name__)

# Keep up to N ticks per symbol for strategy calculations
_HISTORY_MAXLEN = 500


class PriceStore:
    """
    Thread-safe (asyncio) store for latest prices and tick history.
    Subscribes to TICK events automatically.
    """

    def __init__(self) -> None:
        self._prices: dict[str, float] = {}
        self._ticks: dict[str, deque[Tick]] = defaultdict(lambda: deque(maxlen=_HISTORY_MAXLEN))
        self._last_update: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._log_file = settings.DATA_DIR / settings.PRICE_LOG_FILE
        self._log_handle = None

    async def start(self) -> None:
        self._log_handle = await aiofiles.open(self._log_file, mode="a", encoding="utf-8")
        event_bus.subscribe(Topics.TICK, self._on_tick)
        log.info("PriceStore started")

    async def stop(self) -> None:
        event_bus.unsubscribe(Topics.TICK, self._on_tick)
        if self._log_handle:
            await self._log_handle.close()

    async def _on_tick(self, tick: Tick) -> None:
        async with self._lock:
            self._prices[tick.symbol] = tick.price
            self._ticks[tick.symbol].append(tick)
            self._last_update[tick.symbol] = tick.event_time

        # Persist to file (fire-and-forget)
        asyncio.create_task(self._log_tick(tick))

    async def _log_tick(self, tick: Tick) -> None:
        try:
            record = orjson.dumps({
                "s": tick.symbol,
                "p": tick.price,
                "q": tick.qty,
                "t": tick.timestamp,
            }).decode() + "\n"
            await self._log_handle.write(record)
        except Exception as exc:
            log.error(f"Failed to log tick: {exc}")

    # ------------------------------------------------------------------
    # Query API
    # ------------------------------------------------------------------

    def get_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)

    def get_all_prices(self) -> dict[str, float]:
        return dict(self._prices)

    def get_ticks(self, symbol: str, n: int = 100) -> list[Tick]:
        """Get the last N ticks for a symbol."""
        ticks = self._ticks.get(symbol)
        if not ticks:
            return []
        items = list(ticks)
        return items[-n:]

    def get_prices_list(self, symbol: str, n: int = 100) -> list[float]:
        """Get the last N closing prices for a symbol."""
        return [t.price for t in self.get_ticks(symbol, n)]

    def get_last_update(self, symbol: str) -> Optional[float]:
        return self._last_update.get(symbol)

    def is_stale(self, symbol: str, threshold: float = 30.0) -> bool:
        last = self._last_update.get(symbol)
        if last is None:
            return True
        return (time.time() - last) > threshold

    @property
    def active_symbols(self) -> list[str]:
        return list(self._prices.keys())


# Global singleton
price_store = PriceStore()
