"""
Paper Trading Engine.

Simulates SPOT market orders with realistic fee calculation.
Manages portfolio state and emits events.

Fee structure (Tokocrypto USDT pairs):
  Base taker fee:  0.10%
  VAT:             0.12%
  CFT:             0.0444%
  Total taker:     ~0.2644% per trade
"""
from __future__ import annotations
import asyncio
import logging
import time
import uuid
from typing import Optional

import aiofiles
import orjson

from src.core.config import settings
from src.core.event_bus import event_bus, Topics
from src.core.models import (
    Order, OrderSide, OrderStatus,
    Portfolio, Position,
)

log = logging.getLogger(__name__)


class PaperTradingEngine:
    """
    Executes simulated SPOT market orders.
    Thread-safe via asyncio lock.
    """

    def __init__(self) -> None:
        self._usdt_balance: float = settings.INITIAL_USDT_BALANCE
        self._positions: dict[str, Position] = {}
        self._orders: list[Order] = []
        self._total_fees: float = 0.0
        self._lock = asyncio.Lock()
        self._trade_log_file = settings.DATA_DIR / settings.TRADE_LOG_FILE
        self._snapshot_file = settings.DATA_DIR / settings.PORTFOLIO_SNAPSHOT_FILE
        self._trade_handle = None
        self._snapshot_handle = None

    async def start(self) -> None:
        self._trade_handle = await aiofiles.open(
            self._trade_log_file, mode="a", encoding="utf-8"
        )
        self._snapshot_handle = await aiofiles.open(
            self._snapshot_file, mode="a", encoding="utf-8"
        )
        log.info(
            f"Paper Trading Engine started | "
            f"Initial balance: {self._usdt_balance:.2f} USDT"
        )

    async def stop(self) -> None:
        if self._trade_handle:
            await self._trade_handle.close()
        if self._snapshot_handle:
            await self._snapshot_handle.close()

    # ------------------------------------------------------------------
    # Order execution
    # ------------------------------------------------------------------

    async def market_buy(
        self,
        symbol: str,
        usdt_amount: float,
        current_price: float,
        note: str = "",
    ) -> Optional[Order]:
        """
        Execute a paper market BUY order.
        usdt_amount: USDT to spend (before fees).
        Returns Order on success, None on rejection.
        """
        async with self._lock:
            return await self._execute_buy(symbol, usdt_amount, current_price, note)

    async def market_sell(
        self,
        symbol: str,
        quantity: Optional[float],
        current_price: float,
        note: str = "",
    ) -> Optional[Order]:
        """
        Execute a paper market SELL order.
        quantity: base asset qty to sell (None = sell all).
        Returns Order on success, None on rejection.
        """
        async with self._lock:
            return await self._execute_sell(symbol, quantity, current_price, note)

    async def _execute_buy(
        self,
        symbol: str,
        usdt_amount: float,
        price: float,
        note: str,
    ) -> Optional[Order]:
        # Validate
        if usdt_amount < settings.MIN_ORDER_USDT:
            log.warning(f"BUY {symbol}: order too small ({usdt_amount:.4f} USDT)")
            return self._reject_order(symbol, OrderSide.BUY, 0, price, "Order too small")

        if usdt_amount > self._usdt_balance:
            log.warning(
                f"BUY {symbol}: insufficient balance "
                f"({self._usdt_balance:.4f} USDT available, need {usdt_amount:.4f})"
            )
            return self._reject_order(symbol, OrderSide.BUY, 0, price, "Insufficient USDT")

        # Fee calculation
        fee_rate = settings.total_taker_fee
        fee_usdt = usdt_amount * fee_rate
        usdt_after_fee = usdt_amount - fee_usdt
        quantity = usdt_after_fee / price

        # Update balances
        self._usdt_balance -= usdt_amount
        self._total_fees += fee_usdt

        # Update position
        pos = self._positions.get(symbol, Position(symbol=symbol))
        total_qty = pos.quantity + quantity
        total_cost = pos.total_cost_usdt + usdt_amount
        new_avg = total_cost / total_qty if total_qty > 0 else price
        self._positions[symbol] = Position(
            symbol=symbol,
            quantity=total_qty,
            avg_buy_price=new_avg,
            total_cost_usdt=total_cost,
        )

        order = Order(
            order_id=str(uuid.uuid4())[:8],
            symbol=symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            price=price,
            status=OrderStatus.FILLED,
            fee_usdt=fee_usdt,
            net_usdt=-usdt_amount,   # spent USDT
            timestamp=time.time(),
            note=note,
        )
        self._orders.append(order)

        log.info(
            f"[BUY ] {symbol:12s} | qty={quantity:.6f} | price={price:.4f} | "
            f"cost={usdt_amount:.4f} USDT | fee={fee_usdt:.4f} USDT | "
            f"balance={self._usdt_balance:.4f} USDT"
        )

        await self._persist_order(order)
        await event_bus.publish(Topics.ORDER_FILLED, order)
        await event_bus.publish(Topics.PORTFOLIO_UPDATE, self._get_portfolio())
        return order

    async def _execute_sell(
        self,
        symbol: str,
        quantity: Optional[float],
        price: float,
        note: str,
    ) -> Optional[Order]:
        pos = self._positions.get(symbol)
        if not pos or not pos.is_open:
            log.warning(f"SELL {symbol}: no open position")
            return self._reject_order(symbol, OrderSide.SELL, 0, price, "No position")

        sell_qty = quantity if quantity is not None else pos.quantity
        sell_qty = min(sell_qty, pos.quantity)

        gross_usdt = sell_qty * price
        fee_rate = settings.total_taker_fee
        fee_usdt = gross_usdt * fee_rate
        net_usdt = gross_usdt - fee_usdt

        if gross_usdt < settings.MIN_ORDER_USDT:
            log.warning(f"SELL {symbol}: order too small ({gross_usdt:.4f} USDT)")
            return self._reject_order(symbol, OrderSide.SELL, sell_qty, price, "Order too small")

        # Update balances
        self._usdt_balance += net_usdt
        self._total_fees += fee_usdt

        # Update position
        remaining_qty = pos.quantity - sell_qty
        if remaining_qty < 1e-10:
            self._positions.pop(symbol, None)
        else:
            ratio = remaining_qty / pos.quantity
            self._positions[symbol] = Position(
                symbol=symbol,
                quantity=remaining_qty,
                avg_buy_price=pos.avg_buy_price,
                total_cost_usdt=pos.total_cost_usdt * ratio,
                stop_loss=pos.stop_loss,
                take_profit=pos.take_profit,
                trailing_stop_pct=pos.trailing_stop_pct,
                trailing_high=pos.trailing_high,
                entry_time=pos.entry_time,
                setup_note=pos.setup_note,
                htf_bias=pos.htf_bias,
                partial_closed=pos.partial_closed,
            )

        pnl = net_usdt - (pos.avg_buy_price * sell_qty)
        pnl_pct = (price / pos.avg_buy_price - 1) * 100 if pos.avg_buy_price else 0

        order = Order(
            order_id=str(uuid.uuid4())[:8],
            symbol=symbol,
            side=OrderSide.SELL,
            quantity=sell_qty,
            price=price,
            status=OrderStatus.FILLED,
            fee_usdt=fee_usdt,
            net_usdt=net_usdt,
            timestamp=time.time(),
            note=note,
        )
        self._orders.append(order)

        pnl_emoji = "📈" if pnl >= 0 else "📉"
        log.info(
            f"[SELL] {symbol:12s} | qty={sell_qty:.6f} | price={price:.4f} | "
            f"gross={gross_usdt:.4f} | net={net_usdt:.4f} USDT | "
            f"pnl={pnl:+.4f} ({pnl_pct:+.2f}%) {pnl_emoji} | "
            f"balance={self._usdt_balance:.4f} USDT"
        )

        await self._persist_order(order)
        await event_bus.publish(Topics.ORDER_FILLED, order)
        await event_bus.publish(Topics.PORTFOLIO_UPDATE, self._get_portfolio())
        return order

    def _reject_order(
        self, symbol: str, side: OrderSide, qty: float, price: float, note: str
    ) -> Order:
        return Order(
            order_id=str(uuid.uuid4())[:8],
            symbol=symbol,
            side=side,
            quantity=qty,
            price=price,
            status=OrderStatus.REJECTED,
            fee_usdt=0.0,
            net_usdt=0.0,
            note=note,
        )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    async def _persist_order(self, order: Order) -> None:
        try:
            record = orjson.dumps(order.to_dict()).decode() + "\n"
            await self._trade_handle.write(record)
            await self._trade_handle.flush()
        except Exception as exc:
            log.error(f"Failed to persist order: {exc}")

    async def save_snapshot(self, prices: dict[str, float]) -> None:
        """Persist a portfolio snapshot to JSONL."""
        try:
            snapshot = self._get_portfolio().to_dict(prices)
            record = orjson.dumps(snapshot).decode() + "\n"
            await self._snapshot_handle.write(record)
            await self._snapshot_handle.flush()
        except Exception as exc:
            log.error(f"Failed to save snapshot: {exc}")

    # ------------------------------------------------------------------
    # State access (read-only)
    # ------------------------------------------------------------------

    def _get_portfolio(self) -> Portfolio:
        return Portfolio(
            usdt_balance=self._usdt_balance,
            positions=dict(self._positions),
            total_trades=len([o for o in self._orders if o.status == OrderStatus.FILLED]),
            total_fees_paid=self._total_fees,
        )

    @property
    def portfolio(self) -> Portfolio:
        return self._get_portfolio()

    @property
    def usdt_balance(self) -> float:
        return self._usdt_balance

    @property
    def positions(self) -> dict[str, Position]:
        return dict(self._positions)

    @property
    def orders(self) -> list[Order]:
        return list(self._orders)

    def get_position(self, symbol: str) -> Optional[Position]:
        return self._positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        pos = self._positions.get(symbol)
        return pos is not None and pos.is_open
