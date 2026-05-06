"""
Async event bus - decouples producers from consumers.
Supports typed topics and multiple async subscribers.
"""
from __future__ import annotations
import asyncio
import logging
from collections import defaultdict
from typing import Callable, Any, Awaitable

log = logging.getLogger(__name__)

Handler = Callable[[Any], Awaitable[None]]


class EventBus:
    """
    Simple async pub/sub event bus.
    Topics are plain strings. Handlers are async callables.
    Errors in one handler don't block others.
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Handler]] = defaultdict(list)

    def subscribe(self, topic: str, handler: Handler) -> None:
        self._subscribers[topic].append(handler)
        log.debug(f"Subscribed {handler.__qualname__!r} to topic {topic!r}")

    def unsubscribe(self, topic: str, handler: Handler) -> None:
        try:
            self._subscribers[topic].remove(handler)
        except ValueError:
            pass

    async def publish(self, topic: str, payload: Any) -> None:
        handlers = self._subscribers.get(topic, [])
        if not handlers:
            return
        results = await asyncio.gather(
            *[h(payload) for h in handlers],
            return_exceptions=True,
        )
        for h, r in zip(handlers, results):
            if isinstance(r, Exception):
                log.error(f"Handler {h.__qualname__!r} raised on topic {topic!r}: {r}", exc_info=r)


# --- Well-known topics ---
class Topics:
    TICK = "tick"                        # Tick received
    PORTFOLIO_UPDATE = "portfolio"       # Portfolio changed
    ORDER_FILLED = "order.filled"        # Order executed
    WS_CONNECTED = "ws.connected"       # WebSocket connected
    WS_DISCONNECTED = "ws.disconnected" # WebSocket disconnected
    WS_STALE = "ws.stale"               # No data received recently
    SIGNAL_BUY = "signal.buy"           # Strategy buy signal
    SIGNAL_SELL = "signal.sell"         # Strategy sell signal
    CANDLE_CLOSED = "candle.closed"


# Global singleton
event_bus = EventBus()
