"""
Tokocrypto WebSocket data feed.

Connects to stream-cloud.tokocrypto.site/stream and subscribes to
individual trade streams for all configured USDT pairs.

Features:
- Exponential backoff reconnect
- Stale-data detection via asyncio watchdog
- Pong keepalive responder
- Event-driven: publishes Tick events to the event bus
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from typing import Optional

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.connection import State
from websockets.exceptions import (
    ConnectionClosed,
    WebSocketException,
)

from src.core.config import settings
from src.core.event_bus import event_bus, Topics
from src.core.models import Tick

log = logging.getLogger(__name__)


def _build_subscribe_message(symbols: list[str]) -> dict:
    """Build subscription payload for aggTrade streams.

    Tokocrypto exposes aggTrade (not raw trade) on the public WS.
    Stream name: <symbol_lower>@aggTrade
    Payload event type: "aggTrade"
    """
    streams = [f"{sym.lower()}@aggTrade" for sym in symbols]
    return {
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1,
    }


def _build_unsubscribe_message(symbols: list[str]) -> dict:
    streams = [f"{sym.lower()}@aggTrade" for sym in symbols]
    return {
        "method": "UNSUBSCRIBE",
        "params": streams,
        "id": 2,
    }


class TokocryptoFeed:
    """
    Async WebSocket feed for Tokocrypto market data.
    Publishes Tick objects to the event bus on topic Topics.TICK.
    """

    def __init__(self, symbols: list[str]) -> None:
        self.symbols = symbols
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._last_message_at: float = 0.0
        self._reconnect_delay: float = settings.WS_RECONNECT_DELAY
        self._connect_count: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the feed loop (runs until stop() is called)."""
        self._running = True
        log.info(f"Starting Tokocrypto feed for {len(self.symbols)} symbols")
        tasks = [
            asyncio.create_task(self._connect_loop(), name="ws-feed"),
            asyncio.create_task(self._stale_watchdog(), name="ws-stale-watchdog"),
        ]
        await asyncio.gather(*tasks)

    async def stop(self) -> None:
        """Gracefully stop the feed."""
        self._running = False
        if self._ws:
            await self._ws.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _connect_loop(self) -> None:
        """Main reconnect loop with exponential backoff."""
        while self._running:
            try:
                await self._connect_and_listen()
                # Clean exit — reset backoff
                self._reconnect_delay = settings.WS_RECONNECT_DELAY
            except Exception as exc:
                if not self._running:
                    break
                log.warning(
                    f"WebSocket error ({type(exc).__name__}: {exc}). "
                    f"Reconnecting in {self._reconnect_delay:.1f}s …"
                )
                await event_bus.publish(Topics.WS_DISCONNECTED, {"reason": str(exc)})
                await asyncio.sleep(self._reconnect_delay)
                # Exponential backoff
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    settings.WS_MAX_RECONNECT_DELAY,
                )

    async def _connect_and_listen(self) -> None:
        """Open a single WebSocket connection and listen until closed."""
        url = settings.WS_BASE_URL
        self._connect_count += 1
        log.info(f"[WS #{self._connect_count}] Connecting to {url}")

        async with websockets.connect(
            url,
            ping_interval=None,   # we handle keepalive manually
            ping_timeout=None,
            max_size=2**23,       # 8 MB
            open_timeout=15,
        ) as ws:
            self._ws = ws
            self._last_message_at = time.time()
            log.info(f"[WS #{self._connect_count}] Connected ✓")
            await event_bus.publish(Topics.WS_CONNECTED, {"connect_count": self._connect_count})

            # Subscribe to streams
            sub_msg = _build_subscribe_message(self.symbols)
            await ws.send(json.dumps(sub_msg))
            log.info(f"[WS] Subscribed to {len(sub_msg['params'])} streams: {sub_msg['params'][:3]}…")

            async for raw in ws:
                self._last_message_at = time.time()
                await self._handle_raw(raw)

    async def _handle_raw(self, raw: str | bytes) -> None:
        """Parse raw WebSocket message and dispatch tick event.

        Tokocrypto uses aggTrade streams on the public WS endpoint.
        aggTrade payload fields:
            e  - event type ("aggTrade")
            E  - event time (ms)
            s  - symbol (e.g. "BTCUSDT")
            p  - price (string)
            q  - quantity (string)
            T  - trade time (ms)
            a  - aggregate trade ID
            f  - first trade ID
            l  - last trade ID
            m  - is buyer maker
        """
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            # log.info(f"[WS RAW] {raw[:300]}")
            msg = json.loads(raw)

            # Skip subscription confirmations: {"result": null, "id": 1}
            # FIX: was `"result" in msg or "id" in msg and "data" not in msg`
            # which has wrong operator precedence (and binds tighter than or).
            if ("result" in msg) or ("id" in msg and "data" not in msg):
                log.debug(f"[WS] Subscribe ACK: {msg}")
                return

            # Handle combined stream envelope: {"stream": "...", "data": {...}}
            data = msg.get("data", msg)
            event_type = data.get("e", "")

            # FIX: Tokocrypto public WS exposes aggTrade, not raw trade
            if event_type == "aggTrade":
                now = time.time()
                tick = Tick(
                    symbol=data["s"],
                    price=float(data["p"]),
                    qty=float(data["q"]),
                    timestamp=float(data.get("T", data.get("E", now * 1000))) / 1000.0,
                    event_time=float(data.get("E", now * 1000)) / 1000.0,
                )
                # log.info(f"[WS TICK] Publishing tick: {data['s']} @ {data['p']}")
                await event_bus.publish(Topics.TICK, tick)
            elif event_type:
                # Log unexpected event types at debug level to aid future debugging
                log.debug(f"[WS] Unhandled event type: {event_type!r}")

        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            log.debug(f"Failed to parse message: {exc} | raw={raw[:200]!r}")

    async def _stale_watchdog(self) -> None:
        """Periodically check if we're still receiving data."""
        await asyncio.sleep(settings.WS_STALE_THRESHOLD)
        while self._running:
            age = time.time() - self._last_message_at
            if self._last_message_at > 0 and age > settings.WS_STALE_THRESHOLD:
                log.warning(f"[WS] Stale data detected — last message {age:.1f}s ago")
                await event_bus.publish(Topics.WS_STALE, {"age_seconds": age})
                # Force reconnect by closing current connection
                if self._ws and self._ws.state is State.OPEN:
                    await self._ws.close()
            await asyncio.sleep(settings.WS_STALE_THRESHOLD / 2)

    @property
    def last_message_age(self) -> float:
        if self._last_message_at == 0:
            return float("inf")
        return time.time() - self._last_message_at

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._ws.state is State.OPEN