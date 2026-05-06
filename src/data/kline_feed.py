"""
Tokocrypto Kline (OHLCV) WebSocket Feed.

Subscribes to kline streams for configured symbols × timeframes.
Feeds data into CandleStore and publishes CANDLE_CLOSED events.

Stream format:
    {symbol.lower()}@kline_{interval}
    e.g. btcusdt@kline_1h, btcusdt@kline_15m, btcusdt@kline_5m

SMC Intraday needs: 1h (HTF bias), 15m (mid/POI), 5m (LTF entry)

Note: Tokocrypto kline WS delivers a new message on every tick
      (live update), plus a final closed=True message at candle close.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import websockets
from websockets.connection import State
from websockets.exceptions import ConnectionClosed, WebSocketException

from src.core.config import settings
from src.core.event_bus import event_bus, Topics
from src.data.candle_store import CandleStore

log = logging.getLogger(__name__)

# Timeframes needed by SMC Intraday strategy
SMC_TIMEFRAMES = ["1h", "15m", "5m"]

# Map our labels to Tokocrypto stream interval strings
TF_TO_INTERVAL = {
    "1d": "1d",
    "4h": "4h",
    "1h": "1h",
    "15m": "15m",
    "5m": "5m",
    "1m": "1m",
}


def _build_kline_subscribe(symbols: list[str], timeframes: list[str]) -> dict:
    streams = [
        f"{sym.lower()}@kline_{TF_TO_INTERVAL[tf]}"
        for sym in symbols
        for tf in timeframes
        if tf in TF_TO_INTERVAL
    ]
    return {"method": "SUBSCRIBE", "params": streams, "id": 10}


class KlineFeed:
    """
    Async WebSocket feed for Tokocrypto kline/OHLCV data.

    Publishes:
        Topics.CANDLE_CLOSED  — when a candle closes (payload: Candle)
        Topics.WS_DISCONNECTED — on connection error
    """

    def __init__(
        self,
        candle_store: CandleStore,
        symbols: Optional[list[str]] = None,
        timeframes: Optional[list[str]] = None,
    ) -> None:
        self._store = candle_store
        self.symbols = symbols or settings.SYMBOLS
        self.timeframes = timeframes or SMC_TIMEFRAMES
        self._running = False
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._last_message_at: float = 0.0
        self._reconnect_delay: float = settings.WS_RECONNECT_DELAY
        self._connect_count: int = 0

    # ──────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        stream_count = len(self.symbols) * len(self.timeframes)
        log.info(
            f"KlineFeed starting — "
            f"{len(self.symbols)} symbols × {self.timeframes} = {stream_count} streams"
        )
        tasks = [
            asyncio.create_task(self._connect_loop(), name="kline-feed"),
            asyncio.create_task(self._stale_watchdog(), name="kline-watchdog"),
        ]
        await asyncio.gather(*tasks)

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    # ──────────────────────────────────────────────────────
    # Connection
    # ──────────────────────────────────────────────────────

    async def _connect_loop(self) -> None:
        while self._running:
            try:
                await self._connect_and_listen()
                self._reconnect_delay = settings.WS_RECONNECT_DELAY
            except Exception as exc:
                if not self._running:
                    break
                log.warning(
                    f"[KlineFeed] Error ({type(exc).__name__}: {exc}). "
                    f"Reconnecting in {self._reconnect_delay:.1f}s …"
                )
                await event_bus.publish(
                    Topics.WS_DISCONNECTED, {"source": "kline", "reason": str(exc)}
                )
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    settings.WS_MAX_RECONNECT_DELAY,
                )

    async def _connect_and_listen(self) -> None:
        url = settings.WS_BASE_URL
        self._connect_count += 1
        log.info(f"[KlineFeed #{self._connect_count}] Connecting to {url}")

        async with websockets.connect(
            url,
            ping_interval=None,
            ping_timeout=None,
            max_size=2**22,
            open_timeout=15,
        ) as ws:
            self._ws = ws
            self._last_message_at = time.time()
            log.info(f"[KlineFeed #{self._connect_count}] Connected ✓")

            sub = _build_kline_subscribe(self.symbols, self.timeframes)
            await ws.send(json.dumps(sub))
            log.debug(f"[KlineFeed] Subscribed: {sub['params'][:3]}…")

            async for raw in ws:
                self._last_message_at = time.time()
                await self._handle_raw(raw)

    # ──────────────────────────────────────────────────────
    # Message handling
    # ──────────────────────────────────────────────────────

    async def _handle_raw(self, raw: str | bytes) -> None:
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            msg = json.loads(raw)

            # Skip subscription acks
            if "result" in msg and "data" not in msg:
                return

            data = msg.get("data", msg)
            event_type = data.get("e", "")

            if event_type != "kline":
                return

            k = data["k"]
            symbol: str = data["s"]          # e.g. "BTCUSDT"
            interval: str = k["i"]           # e.g. "1d"

            # Normalise interval → our timeframe label
            tf = interval  # already matches our TF_TO_INTERVAL values

            # Feed into CandleStore (async)
            await self._store.apply_kline(symbol, tf, k)

            # Publish CANDLE_CLOSED event for strategy to react
            if bool(k["x"]):
                candle = self._store.get_latest_closed(symbol, tf)
                if candle:
                    await event_bus.publish(Topics.CANDLE_CLOSED, candle)
                    log.debug(
                        f"[CANDLE CLOSED] {symbol} {tf} "
                        f"c={candle.close:.4f} vol={candle.volume:.2f}"
                    )

        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            log.debug(f"[KlineFeed] Parse error: {exc} | raw={str(raw)[:200]!r}")

    # ──────────────────────────────────────────────────────
    # Stale watchdog
    # ──────────────────────────────────────────────────────

    async def _stale_watchdog(self) -> None:
        await asyncio.sleep(settings.WS_STALE_THRESHOLD * 2)  # grace period on start
        while self._running:
            age = time.time() - self._last_message_at
            if self._last_message_at > 0 and age > settings.WS_STALE_THRESHOLD * 3:
                # Kline streams are lower frequency than trades; use 3× threshold
                log.warning(f"[KlineFeed] Stale — last message {age:.1f}s ago")
                if self._ws and self._ws.state is State.OPEN:
                    await self._ws.close()
            await asyncio.sleep(settings.WS_STALE_THRESHOLD)

    # ──────────────────────────────────────────────────────
    # Properties
    # ──────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._ws.state is State.OPEN

    @property
    def last_message_age(self) -> float:
        if self._last_message_at == 0:
            return float("inf")
        return time.time() - self._last_message_at