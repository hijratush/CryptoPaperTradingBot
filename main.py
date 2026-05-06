"""
Main entrypoint for the Crypto Paper Trading Bot.

Boot sequence:
1. Setup directories & logging
2. Start PriceStore (tick persistence)
3. Start PaperTradingEngine
4. Start Strategy (subscribes to ticks)
5. Start WebSocket feed (Tokocrypto)
6. Start FastAPI dashboard (uvicorn)
7. Periodic portfolio snapshots
"""
from __future__ import annotations
import asyncio
import logging
import signal
import sys

import uvicorn

from src.core.config import settings
from src.core.engine import PaperTradingEngine
from src.core.event_bus import event_bus, Topics
from src.core.logging_setup import setup_logging, get_logger
from src.dashboard.app import create_app
from src.data.feed import TokocryptoFeed
from src.data.price_store import PriceStore
from src.data.candle_store import CandleStore
from src.data.kline_feed import KlineFeed
from src.data.history_seeder import seed_candle_store
from src.strategy.smc_strategy import SMCStrategy


def _setup() -> None:
    """Initialize directories and logging."""
    settings.ensure_dirs()
    setup_logging(
        log_dir=settings.LOG_DIR,
        log_file=settings.LOG_FILE,
        log_level=settings.LOG_LEVEL,
    )


async def _snapshot_loop(engine: PaperTradingEngine, price_store: PriceStore, interval: float = 60.0) -> None:
    """Save portfolio snapshot every `interval` seconds."""
    log = get_logger("snapshot")
    while True:
        await asyncio.sleep(interval)
        prices = price_store.get_all_prices()
        if prices:
            await engine.save_snapshot(prices)
            total = engine.portfolio.total_value(prices)
            log.info(f"Portfolio snapshot | total_value={total:.4f} USDT")


async def _log_ws_events() -> None:
    """Log WebSocket lifecycle events."""
    log = get_logger("ws.events")

    async def on_connect(payload):
        log.info(f"[WS] Connected (attempt #{payload['connect_count']})")

    async def on_disconnect(payload):
        log.warning(f"[WS] Disconnected: {payload['reason']}")

    async def on_stale(payload):
        log.warning(f"[WS] Stale data — {payload['age_seconds']:.1f}s since last message")

    event_bus.subscribe(Topics.WS_CONNECTED, on_connect)
    event_bus.subscribe(Topics.WS_DISCONNECTED, on_disconnect)
    event_bus.subscribe(Topics.WS_STALE, on_stale)


async def main() -> None:
    _setup()
    log = get_logger("main")

    log.info("=" * 60)
    log.info("  Crypto Paper Trading Bot — Starting Up")
    log.info(f"  Symbols  : {', '.join(settings.SYMBOLS)}")
    log.info(f"  Balance  : {settings.INITIAL_USDT_BALANCE:.2f} USDT")
    log.info(f"  Taker fee: {settings.total_taker_fee * 100:.4f}%")
    log.info(f"  Dashboard: http://localhost:{settings.DASHBOARD_PORT}")
    log.info("=" * 60)

    # --- Instantiate components ---
    engine = PaperTradingEngine()
    price_store = PriceStore()
    candle_store = CandleStore()
    await seed_candle_store(candle_store)
    feed = TokocryptoFeed(symbols=settings.SYMBOLS)
    kline_feed = KlineFeed(candle_store)
    strategy = SMCStrategy(
        engine=engine, 
        price_store=price_store, 
        candle_provider=candle_store.get_candles,
    )

    # --- Start components ---
    await price_store.start()
    await engine.start()
    strategy.start()
    await _log_ws_events()

    # --- FastAPI dashboard ---
    app = create_app(engine, price_store, strategy, feed, kline_feed)
    server_config = uvicorn.Config(
        app=app,
        host=settings.DASHBOARD_HOST,
        port=settings.DASHBOARD_PORT,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(server_config)

    # --- Gather all tasks ---
    tasks = [
        asyncio.create_task(feed.start(), name="ws-feed"),
        asyncio.create_task(kline_feed.start(), name="kline-feed"),
        asyncio.create_task(server.serve(), name="dashboard"),
        asyncio.create_task(_snapshot_loop(engine, price_store), name="snapshot"),
    ]

    log.info(f"All systems running. Dashboard → http://localhost:{settings.DASHBOARD_PORT}")

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown(sig_name: str) -> None:
        log.info(f"Received {sig_name} — shutting down gracefully …")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown, sig.name)
        except NotImplementedError:
            pass  # Windows

    # ── Supervisor: watch tasks, only exit on signal or unexpected crash ──
    async def _supervisor() -> None:
        while not stop_event.is_set():
            await asyncio.sleep(5.0)
            for t in tasks:
                if t.done() and not t.cancelled():
                    exc = t.exception()
                    if exc is not None:
                        log.error(
                            f"Task '{t.get_name()}' crashed: {exc} — triggering shutdown",
                            exc_info=exc,
                        )
                        stop_event.set()
                        return
                    else:
                        # Task exited cleanly but shouldn't have (feed/server loops are infinite)
                        log.error(
                            f"Task '{t.get_name()}' exited unexpectedly — triggering shutdown"
                        )
                        stop_event.set()
                        return

    supervisor_task = asyncio.create_task(_supervisor(), name="supervisor")

    try:
        await stop_event.wait()
    finally:
        supervisor_task.cancel()
        log.info("Stopping components …")
        strategy.stop()
        await feed.stop()
        await kline_feed.stop()
        await price_store.stop()
        await engine.stop()
        server.should_exit = True

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        log.info("Bot stopped cleanly. Goodbye.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass