"""
Trading Strategy: EMA Crossover + RSI Filter

Signal rules:
  BUY  when: EMA(fast) crosses above EMA(slow) AND RSI < overbought_threshold
  SELL when: EMA(fast) crosses below EMA(slow) OR RSI > overbought_threshold

Risk management:
  - Max position size per symbol: 20% of total portfolio value
  - Stop loss: 3% from avg buy price
  - Take profit: 5% from avg buy price
"""
from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from src.core.config import settings
from src.core.engine import PaperTradingEngine
from src.core.event_bus import event_bus, Topics
from src.core.models import Tick
from src.data.price_store import PriceStore

log = logging.getLogger(__name__)


# ----- Indicator helpers -----

def ema(prices: list[float], period: int) -> Optional[float]:
    """Exponential Moving Average of last `period` values."""
    if len(prices) < period:
        return None
    k = 2.0 / (period + 1)
    val = prices[-period]
    for p in prices[-period + 1:]:
        val = p * k + val * (1 - k)
    return val


def rsi(prices: list[float], period: int = 14) -> Optional[float]:
    """Relative Strength Index."""
    if len(prices) < period + 1:
        return None
    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    deltas = deltas[-period:]
    gains = [d for d in deltas if d > 0]
    losses = [-d for d in deltas if d < 0]
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1 + rs))


@dataclass
class SymbolState:
    symbol: str
    prev_fast_ema: Optional[float] = None
    prev_slow_ema: Optional[float] = None
    # Track whether we generated a signal to avoid duplicates
    last_signal: Optional[str] = None  # "buy" | "sell" | None


class EMACrossStrategy:
    """
    EMA crossover strategy that operates on live tick data.
    Subscribes to TICK events, runs indicator calculations,
    and calls the paper trading engine on signals.
    """

    FAST_EMA = 9
    SLOW_EMA = 21
    RSI_PERIOD = 14
    RSI_OVERBOUGHT = 70.0
    RSI_OVERSOLD = 30.0

    # Risk parameters
    MAX_POSITION_PCT = 0.20      # max 20% of balance per position
    STOP_LOSS_PCT = 0.03         # 3% stop loss
    TAKE_PROFIT_PCT = 0.05       # 5% take profit
    MIN_TICKS_REQUIRED = 25      # need at least this many ticks

    def __init__(self, engine: PaperTradingEngine, price_store: PriceStore) -> None:
        self._engine = engine
        self._price_store = price_store
        self._states: dict[str, SymbolState] = {
            sym: SymbolState(symbol=sym) for sym in settings.SYMBOLS
        }
        self._tick_count: dict[str, int] = {sym: 0 for sym in settings.SYMBOLS}

    def start(self) -> None:
        event_bus.subscribe(Topics.TICK, self._on_tick)
        log.info(
            f"EMA Crossover Strategy started | "
            f"EMA({self.FAST_EMA}/{self.SLOW_EMA}) + RSI({self.RSI_PERIOD})"
        )

    def stop(self) -> None:
        event_bus.unsubscribe(Topics.TICK, self._on_tick)

    async def _on_tick(self, tick: Tick) -> None:
        symbol = tick.symbol
        if symbol not in self._states:
            return

        self._tick_count[symbol] = self._tick_count.get(symbol, 0) + 1

        # Only analyze every Nth tick to reduce noise
        if self._tick_count[symbol] % 5 != 0:
            return

        prices = self._price_store.get_prices_list(symbol, n=50)
        if len(prices) < self.MIN_TICKS_REQUIRED:
            return

        fast = ema(prices, self.FAST_EMA)
        slow = ema(prices, self.SLOW_EMA)
        rsi_val = rsi(prices, self.RSI_PERIOD)

        if fast is None or slow is None or rsi_val is None:
            return

        state = self._states[symbol]
        prev_fast = state.prev_fast_ema
        prev_slow = state.prev_slow_ema

        # Check stop-loss / take-profit for open positions
        await self._check_exit_conditions(symbol, tick.price)

        if prev_fast is not None and prev_slow is not None:
            # Bullish crossover: fast crosses above slow
            bullish_cross = prev_fast <= prev_slow and fast > slow
            # Bearish crossover: fast crosses below slow
            bearish_cross = prev_fast >= prev_slow and fast < slow

            if bullish_cross and rsi_val < self.RSI_OVERBOUGHT:
                if state.last_signal != "buy" and not self._engine.has_position(symbol):
                    await self._emit_buy(symbol, tick.price, fast, slow, rsi_val)
                    state.last_signal = "buy"

            elif bearish_cross or rsi_val > self.RSI_OVERBOUGHT:
                if state.last_signal != "sell" and self._engine.has_position(symbol):
                    await self._emit_sell(symbol, tick.price, fast, slow, rsi_val)
                    state.last_signal = "sell"

        state.prev_fast_ema = fast
        state.prev_slow_ema = slow

    async def _check_exit_conditions(self, symbol: str, current_price: float) -> None:
        """Check stop-loss and take-profit on open positions."""
        pos = self._engine.get_position(symbol)
        if not pos or not pos.is_open:
            return

        change = (current_price / pos.avg_buy_price) - 1

        if change <= -self.STOP_LOSS_PCT:
            log.warning(
                f"[SL ] {symbol} | price={current_price:.4f} | "
                f"entry={pos.avg_buy_price:.4f} | change={change*100:.2f}%"
            )
            await self._emit_sell(symbol, current_price, None, None, None, note="stop_loss")
            self._states[symbol].last_signal = "sell"

        elif change >= self.TAKE_PROFIT_PCT:
            log.info(
                f"[TP ] {symbol} | price={current_price:.4f} | "
                f"entry={pos.avg_buy_price:.4f} | change={change*100:.2f}%"
            )
            await self._emit_sell(symbol, current_price, None, None, None, note="take_profit")
            self._states[symbol].last_signal = "sell"

    async def _emit_buy(
        self,
        symbol: str,
        price: float,
        fast: float,
        slow: float,
        rsi_val: float,
    ) -> None:
        """Calculate position size and execute buy."""
        balance = self._engine.usdt_balance
        amount = min(balance * self.MAX_POSITION_PCT, balance)
        amount = max(0, amount)

        if amount < settings.MIN_ORDER_USDT:
            log.debug(f"BUY signal {symbol}: insufficient balance for min order")
            return

        note = f"EMA cross bullish | EMA9={fast:.4f} EMA21={slow:.4f} RSI={rsi_val:.1f}"
        log.info(f"[SIGNAL BUY ] {symbol} | {note}")
        await event_bus.publish(Topics.SIGNAL_BUY, {"symbol": symbol, "price": price})
        await self._engine.market_buy(symbol, amount, price, note=note)

    async def _emit_sell(
        self,
        symbol: str,
        price: float,
        fast: Optional[float],
        slow: Optional[float],
        rsi_val: Optional[float],
        note: str = "",
    ) -> None:
        if not note:
            note = f"EMA cross bearish | EMA9={fast:.4f} EMA21={slow:.4f} RSI={rsi_val:.1f}"
        log.info(f"[SIGNAL SELL] {symbol} | {note}")
        await event_bus.publish(Topics.SIGNAL_SELL, {"symbol": symbol, "price": price})
        await self._engine.market_sell(symbol, None, price, note=note)

    def get_indicator_snapshot(self) -> dict:
        """Return latest indicator values for the dashboard."""
        result = {}
        for symbol in settings.SYMBOLS:
            prices = self._price_store.get_prices_list(symbol, n=50)
            if len(prices) < self.MIN_TICKS_REQUIRED:
                continue
            fast = ema(prices, self.FAST_EMA)
            slow = ema(prices, self.SLOW_EMA)
            rsi_val = rsi(prices, self.RSI_PERIOD)
            state = self._states.get(symbol)
            result[symbol] = {
                "ema_fast": fast,
                "ema_slow": slow,
                "rsi": rsi_val,
                "trend": (
                    "bullish" if (fast and slow and fast > slow) else
                    "bearish" if (fast and slow and fast < slow) else
                    "neutral"
                ),
                "last_signal": state.last_signal if state else None,
            }
        return result
