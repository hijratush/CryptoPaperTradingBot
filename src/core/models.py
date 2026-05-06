"""
Domain models: immutable value objects and events.
All timestamps are UTC epoch seconds (float).

Extended for SMC strategy:
- Position now carries stop_loss, take_profit levels, trailing state
- Candle is mutable for live aggregation
- HTFContext holds per-symbol multi-timeframe SMC analysis
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(str, Enum):
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class Bias(str, Enum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"
    NEUTRAL = "NEUTRAL"


class TPRatio(str, Enum):
    R1 = "1:1"
    R2 = "1:2"
    R3 = "1:3"


@dataclass(frozen=True)
class Tick:
    symbol: str
    price: float
    qty: float
    timestamp: float
    event_time: float


@dataclass
class Candle:
    """Mutable OHLCV candle — updated live tick-by-tick until closed."""
    symbol: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: float   # candle open time epoch seconds
    closed: bool = False

    def update(self, price: float, qty: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += qty

    def body_size(self) -> float:
        return abs(self.close - self.open)

    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    def is_bullish(self) -> bool:
        return self.close > self.open

    def is_bearish(self) -> bool:
        return self.close < self.open

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol, "timeframe": self.timeframe,
            "open": self.open, "high": self.high,
            "low": self.low, "close": self.close,
            "volume": self.volume, "timestamp": self.timestamp,
        }


@dataclass
class SwingPoint:
    price: float
    timestamp: float
    is_high: bool
    broken: bool = False


@dataclass
class OrderBlock:
    symbol: str
    timeframe: str
    side: OrderSide        # BUY = demand zone, SELL = supply zone
    zone_high: float
    zone_low: float
    origin_timestamp: float
    mitigated: bool = False

    @property
    def midpoint(self) -> float:
        return (self.zone_high + self.zone_low) / 2


@dataclass
class FairValueGap:
    symbol: str
    timeframe: str
    gap_high: float
    gap_low: float
    is_bullish: bool
    timestamp: float
    filled: bool = False

    @property
    def midpoint(self) -> float:
        return (self.gap_high + self.gap_low) / 2


@dataclass
class LiquidityLevel:
    price: float
    is_buy_side: bool
    strength: int
    swept: bool = False
    timestamp: float = field(default_factory=time.time)


@dataclass
class StructureBreak:
    symbol: str
    timeframe: str
    level: float
    direction: OrderSide
    is_choch: bool   # False=BOS, True=CHoCH
    timestamp: float


@dataclass
class FibLevel:
    ratio: float
    price: float
    label: str


@dataclass
class OTEZone:
    symbol: str
    swing_high: float
    swing_low: float
    fib_levels: list[FibLevel]
    is_bullish: bool
    timestamp: float

    @property
    def ote_high(self) -> float:
        prices = [f.price for f in self.fib_levels]
        return max(prices) if prices else 0.0

    @property
    def ote_low(self) -> float:
        prices = [f.price for f in self.fib_levels]
        return min(prices) if prices else 0.0

    def price_in_ote(self, price: float) -> bool:
        return self.ote_low <= price <= self.ote_high


@dataclass
class HTFContext:
    """Full SMC analysis result for one symbol (HTF 1D + 4H)."""
    symbol: str
    bias: Bias
    order_blocks: list[OrderBlock] = field(default_factory=list)
    fvgs: list[FairValueGap] = field(default_factory=list)
    liquidity: list[LiquidityLevel] = field(default_factory=list)
    structure_breaks: list[StructureBreak] = field(default_factory=list)
    ote_zone: Optional[OTEZone] = None
    ema13_htf: Optional[float] = None
    ema21_htf: Optional[float] = None
    nearest_poi_high: Optional[float] = None
    nearest_poi_low: Optional[float] = None
    analyzed_at: float = field(default_factory=time.time)
    is_valid: bool = False


@dataclass
class TradeSetup:
    symbol: str
    entry_price: float
    stop_loss: float
    take_profit_1r: float
    take_profit_2r: float
    take_profit_3r: float
    risk_usdt: float
    position_usdt: float
    tp_ratio: TPRatio
    trailing_stop_pct: float
    note: str = ""
    htf_bias: Bias = Bias.BULLISH


@dataclass
class Position:
    symbol: str
    quantity: float = 0.0
    avg_buy_price: float = 0.0
    total_cost_usdt: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    trailing_stop_pct: float = 0.0
    trailing_high: float = 0.0
    entry_time: float = field(default_factory=time.time)
    setup_note: str = ""
    htf_bias: str = "BULLISH"
    partial_closed: bool = False

    @property
    def is_open(self) -> bool:
        return self.quantity > 0.0

    def current_pnl_pct(self, price: float) -> float:
        if self.avg_buy_price <= 0:
            return 0.0
        return (price / self.avg_buy_price - 1) * 100

    def update_trailing(self, current_price: float) -> Optional[float]:
        """Returns new SL price if trailing moved it up, else None."""
        if self.trailing_stop_pct <= 0:
            return None
        if current_price > self.trailing_high:
            self.trailing_high = current_price
            new_sl = current_price * (1 - self.trailing_stop_pct)
            if new_sl > self.stop_loss:
                self.stop_loss = new_sl
                return new_sl
        return None


@dataclass
class Order:
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    status: OrderStatus
    fee_usdt: float
    net_usdt: float
    timestamp: float = field(default_factory=time.time)
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side.value,
            "quantity": self.quantity,
            "price": self.price,
            "status": self.status.value,
            "fee_usdt": round(self.fee_usdt, 8),
            "net_usdt": round(self.net_usdt, 8),
            "timestamp": self.timestamp,
            "note": self.note,
        }


@dataclass
class Portfolio:
    usdt_balance: float
    positions: dict[str, Position]
    total_trades: int
    total_fees_paid: float
    timestamp: float = field(default_factory=time.time)

    def total_value(self, prices: dict[str, float]) -> float:
        crypto_value = sum(
            pos.quantity * prices.get(sym, pos.avg_buy_price)
            for sym, pos in self.positions.items()
            if pos.is_open
        )
        return self.usdt_balance + crypto_value

    def to_dict(self, prices: dict[str, float]) -> dict:
        positions_dict = {}
        for sym, pos in self.positions.items():
            if pos.is_open:
                cur = prices.get(sym, pos.avg_buy_price)
                pnl = (cur - pos.avg_buy_price) * pos.quantity
                positions_dict[sym] = {
                    "symbol": sym,
                    "quantity": pos.quantity,
                    "avg_buy_price": pos.avg_buy_price,
                    "current_price": cur,
                    "cost_usdt": pos.total_cost_usdt,
                    "current_value": pos.quantity * cur,
                    "unrealized_pnl": pnl,
                    "unrealized_pnl_pct": pos.current_pnl_pct(cur),
                    "stop_loss": pos.stop_loss,
                    "take_profit": pos.take_profit,
                    "trailing_stop_pct": round(pos.trailing_stop_pct * 100, 2),
                    "trailing_high": pos.trailing_high,
                    "setup_note": pos.setup_note,
                    "partial_closed": pos.partial_closed,
                    "entry_time": pos.entry_time,
                }
        return {
            "usdt_balance": self.usdt_balance,
            "positions": positions_dict,
            "total_value": self.total_value(prices),
            "total_trades": self.total_trades,
            "total_fees_paid": self.total_fees_paid,
            "timestamp": self.timestamp,
        }