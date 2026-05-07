"""
Trading Strategy: SMC Intraday — Conservative

Timeframe stack:
  HTF Bias    : 1H  — market structure, OB, FVG, CHoCH/BoS, OTE
  Mid confirm : 15m — POI refinement (tighter zones injected into HTF)
  LTF entry   : 5m  — CHoCH + rejection candle + EMA cross

Entry mode: CONSERVATIVE — ALL 4 conditions must be met simultaneously:
  1. Price inside HTF POI (OB or FVG from 1H or 15m)
  2. Price in OTE retracement zone (0.618–0.786)
  3. LTF CHoCH bullish on 5m (recent structure shift confirms reversal)
  4. LTF EMA cross OR rejection candle on 5m (momentum confirmation)

All 4 required = fewer trades, higher quality setups.

Exit:
  - Stop loss  : CUT_LOSS_PCT below entry (default 2%)
  - Partial TP : 50% at 1R
  - Full TP    : remaining at 2R (or 3R per config)
  - Trailing   : TRAILING_STOP_PCT after partial close

Position sizing: POSITION_SIZE_PCT × total equity, max MAX_OPEN_POSITIONS.
History seed  : 500 candles per timeframe via REST at startup.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from src.core.config import settings
from src.core.engine import PaperTradingEngine
from src.core.event_bus import event_bus, Topics
from src.core.models import (
    Bias, Candle, FairValueGap, HTFContext, LiquidityLevel,
    OrderBlock, OrderSide, OTEZone, FibLevel, StructureBreak,
    SwingPoint, Tick, TPRatio, TradeSetup,
)
from src.data.price_store import PriceStore

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# Indicator helpers
# ─────────────────────────────────────────────────────────────

def ema(prices: list[float], period: int) -> Optional[float]:
    """Single EMA value from close prices."""
    if len(prices) < period:
        return None
    k = 2.0 / (period + 1)
    val = sum(prices[:period]) / period   # SMA seed
    for p in prices[period:]:
        val = p * k + val * (1 - k)
    return val


def ema_series(prices: list[float], period: int) -> list[float]:
    """Full EMA series aligned to prices (NaN-padded at start)."""
    if len(prices) < period:
        return []
    k = 2.0 / (period + 1)
    val = sum(prices[:period]) / period
    result = [val]
    for p in prices[period:]:
        val = p * k + val * (1 - k)
        result.append(val)
    pad = len(prices) - len(result)
    return [float("nan")] * pad + result


def find_swing_highs_lows(
    candles: list[Candle], lookback: int = 3
) -> tuple[list[SwingPoint], list[SwingPoint]]:
    """Pivot-based swing detection with configurable lookback."""
    highs: list[SwingPoint] = []
    lows: list[SwingPoint] = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        window_h = [c.high for c in candles[i - lookback: i + lookback + 1]]
        window_l = [c.low  for c in candles[i - lookback: i + lookback + 1]]
        if candles[i].high == max(window_h):
            highs.append(SwingPoint(
                price=candles[i].high,
                timestamp=candles[i].timestamp,
                is_high=True,
            ))
        if candles[i].low == min(window_l):
            lows.append(SwingPoint(
                price=candles[i].low,
                timestamp=candles[i].timestamp,
                is_high=False,
            ))
    return highs, lows


def detect_order_blocks(
    candles: list[Candle], symbol: str, timeframe: str,
    min_impulse_candles: int = 2,
) -> list[OrderBlock]:
    """
    Demand OB : last bearish candle before bullish impulse.
    Supply OB : last bullish candle before bearish impulse.
    Scaled for intraday — impulse threshold slightly relaxed.
    """
    obs: list[OrderBlock] = []
    if len(candles) < 6:
        return obs
    for i in range(2, len(candles) - 3):
        c = candles[i]
        next3 = candles[i + 1: i + 4]
        # Demand OB
        if c.is_bearish():
            impulse = sum(
                1 for x in next3
                if x.is_bullish() and x.body_size() > c.body_size() * 0.4
            )
            if impulse >= min_impulse_candles:
                obs.append(OrderBlock(
                    symbol=symbol, timeframe=timeframe,
                    side=OrderSide.BUY,
                    zone_high=c.open,
                    zone_low=c.low,
                    origin_timestamp=c.timestamp,
                ))
        # Supply OB
        if c.is_bullish():
            impulse = sum(
                1 for x in next3
                if x.is_bearish() and x.body_size() > c.body_size() * 0.4
            )
            if impulse >= min_impulse_candles:
                obs.append(OrderBlock(
                    symbol=symbol, timeframe=timeframe,
                    side=OrderSide.SELL,
                    zone_high=c.high,
                    zone_low=c.close,
                    origin_timestamp=c.timestamp,
                ))
    return obs


def detect_fvgs(
    candles: list[Candle], symbol: str, timeframe: str,
    min_gap_pct: float = 0.001,   # ignore micro-gaps < 0.1%
) -> list[FairValueGap]:
    """Three-candle FVG pattern with minimum gap size filter."""
    fvgs: list[FairValueGap] = []
    for i in range(2, len(candles)):
        c0, c1, c2 = candles[i - 2], candles[i - 1], candles[i]
        # Bullish FVG: c0.high < c2.low
        if c2.low > c0.high:
            gap = c2.low - c0.high
            mid = (c2.low + c0.high) / 2
            if gap / mid >= min_gap_pct:
                fvgs.append(FairValueGap(
                    symbol=symbol, timeframe=timeframe,
                    gap_high=c2.low, gap_low=c0.high,
                    is_bullish=True, timestamp=c1.timestamp,
                ))
        # Bearish FVG: c0.low > c2.high
        if c2.high < c0.low:
            gap = c0.low - c2.high
            mid = (c0.low + c2.high) / 2
            if gap / mid >= min_gap_pct:
                fvgs.append(FairValueGap(
                    symbol=symbol, timeframe=timeframe,
                    gap_high=c0.low, gap_low=c2.high,
                    is_bullish=False, timestamp=c1.timestamp,
                ))
    return fvgs


def detect_structure_breaks(
    candles: list[Candle],
    swing_highs: list[SwingPoint],
    swing_lows: list[SwingPoint],
    symbol: str,
    timeframe: str,
) -> list[StructureBreak]:
    """
    Scan all candles for closes that break swing levels.
    Returns full historical list — not just the last candle.
    """
    breaks: list[StructureBreak] = []
    if not swing_highs or not swing_lows or len(candles) < 3:
        return breaks

    for i in range(1, len(candles)):
        c = candles[i]
        # Collect swings that formed BEFORE this candle
        sh_before = [s for s in swing_highs if s.timestamp < c.timestamp]
        sl_before = [s for s in swing_lows  if s.timestamp < c.timestamp]

        if not sh_before or not sl_before:
            continue

        last_sh = sh_before[-1].price
        last_sl = sl_before[-1].price
        prev_sh = sh_before[-2].price if len(sh_before) >= 2 else None
        prev_sl = sl_before[-2].price if len(sl_before) >= 2 else None

        # BOS/CHoCH bullish
        if c.close > last_sh:
            is_choch = prev_sh is not None and last_sh < prev_sh
            breaks.append(StructureBreak(
                symbol=symbol, timeframe=timeframe,
                level=last_sh, direction=OrderSide.BUY,
                is_choch=is_choch, timestamp=c.timestamp,
            ))

        # BOS/CHoCH bearish
        if c.close < last_sl:
            is_choch = prev_sl is not None and last_sl > prev_sl
            breaks.append(StructureBreak(
                symbol=symbol, timeframe=timeframe,
                level=last_sl, direction=OrderSide.SELL,
                is_choch=is_choch, timestamp=c.timestamp,
            ))

    return breaks


def detect_liquidity(
    swing_highs: list[SwingPoint],
    swing_lows: list[SwingPoint],
    tolerance_pct: float = 0.002,
) -> list[LiquidityLevel]:
    """Equal highs/lows clusters = liquidity pools."""
    levels: list[LiquidityLevel] = []

    def cluster(points: list[SwingPoint], is_buy_side: bool) -> None:
        prices = [p.price for p in points]
        for p in prices:
            cluster_prices = [q for q in prices if abs(q - p) / p <= tolerance_pct]
            if len(cluster_prices) >= 2:
                avg = sum(cluster_prices) / len(cluster_prices)
                if not any(abs(lv.price - avg) / avg < tolerance_pct for lv in levels):
                    levels.append(LiquidityLevel(
                        price=avg, is_buy_side=is_buy_side,
                        strength=len(cluster_prices),
                    ))

    cluster(swing_highs, is_buy_side=True)
    cluster(swing_lows, is_buy_side=False)
    return levels


def compute_ote_zone(
    swing_high: float, swing_low: float,
    symbol: str, is_bullish: bool, timestamp: float,
) -> Optional[OTEZone]:
    """OTE retracement 0.618–0.786. Returns None if swing is invalid."""
    if swing_high <= swing_low:
        return None
    diff = swing_high - swing_low
    ratios = [0.618, 0.702, 0.786]
    if is_bullish:
        fib_levels = [
            FibLevel(ratio=r, price=round(swing_high - diff * r, 8), label=f"{r:.3f}")
            for r in ratios
        ]
    else:
        fib_levels = [
            FibLevel(ratio=r, price=round(swing_low + diff * r, 8), label=f"{r:.3f}")
            for r in ratios
        ]
    return OTEZone(
        symbol=symbol,
        swing_high=swing_high, swing_low=swing_low,
        fib_levels=fib_levels, is_bullish=is_bullish,
        timestamp=timestamp,
    )


def determine_bias(
    candles: list[Candle],
    structure_breaks: list[StructureBreak],
) -> Bias:
    """
    Bias from most recent structure break.
    Fallback: compare closes over last 20 bars.
    """
    if structure_breaks:
        # Use the most recent break
        last = structure_breaks[-1]
        if last.direction == OrderSide.BUY:
            return Bias.BULLISH
        return Bias.BEARISH

    if len(candles) >= 20:
        recent = candles[-1].close
        old = candles[-20].close
        if recent > old * 1.01:
            return Bias.BULLISH
        if recent < old * 0.99:
            return Bias.BEARISH
    return Bias.NEUTRAL


def is_rejection_candle(candle: Candle) -> bool:
    """Hammer / pin bar: lower wick ≥ 2× body, small upper wick."""
    body = candle.body_size()
    if body == 0:
        return False
    return candle.lower_wick() >= 2 * body and candle.upper_wick() <= body * 0.5


def invalidate_obs(obs: list[OrderBlock], candles: list[Candle]) -> None:
    """Mark OBs as mitigated if price has closed through them."""
    for ob in obs:
        if ob.mitigated:
            continue
        for c in candles:
            if c.timestamp <= ob.origin_timestamp:
                continue
            if ob.side == OrderSide.BUY and c.close < ob.zone_low:
                ob.mitigated = True
                break
            if ob.side == OrderSide.SELL and c.close > ob.zone_high:
                ob.mitigated = True
                break


def invalidate_fvgs(fvgs: list[FairValueGap], candles: list[Candle]) -> None:
    """Mark FVGs as filled if price has closed through the midpoint."""
    for fvg in fvgs:
        if fvg.filled:
            continue
        mid = fvg.midpoint
        for c in candles:
            if c.timestamp <= fvg.timestamp:
                continue
            if fvg.is_bullish and c.close < mid:
                fvg.filled = True
                break
            if not fvg.is_bullish and c.close > mid:
                fvg.filled = True
                break


# ─────────────────────────────────────────────────────────────
# HTF Analyser  (1H primary, 15m as secondary filter)
# ─────────────────────────────────────────────────────────────

class HTFAnalyser:
    """
    Builds HTFContext using 1H as primary timeframe.
    15m is used to refine the nearest POI and check for
    recent micro-CHoCH as additional filter.

    Results cached and refreshed every HTF_ANALYSIS_INTERVAL seconds.
    """

    # Intraday: use 1H for bias, 15m for POI refinement
    PRIMARY_TF   = "1h"
    SECONDARY_TF = "15m"
    MIN_CANDLES  = 30    # minimum to produce a valid context

    def __init__(self, candle_provider) -> None:
        self._provider = candle_provider
        self._contexts: dict[str, HTFContext] = {}

    def analyse(self, symbol: str) -> HTFContext:
        candles_1h  = self._provider(symbol, self.PRIMARY_TF)
        candles_15m = self._provider(symbol, self.SECONDARY_TF)

        if len(candles_1h) < self.MIN_CANDLES:
            ctx = HTFContext(symbol=symbol, bias=Bias.NEUTRAL, is_valid=False)
            self._contexts[symbol] = ctx
            return ctx

        # ── Primary analysis on 1H ──
        sh, sl = find_swing_highs_lows(candles_1h, lookback=settings.SWING_LOOKBACK)
        obs     = detect_order_blocks(candles_1h, symbol, self.PRIMARY_TF)
        fvgs    = detect_fvgs(candles_1h, symbol, self.PRIMARY_TF)
        sb      = detect_structure_breaks(candles_1h, sh, sl, symbol, self.PRIMARY_TF)
        liq     = detect_liquidity(sh, sl)
        bias    = determine_bias(candles_1h, sb)

        # Invalidate stale zones
        invalidate_obs(obs, candles_1h)
        invalidate_fvgs(fvgs, candles_1h)

        # ── Secondary refinement on 15m ──
        # Add tighter OBs and FVGs from 15m to get more precise POIs
        if len(candles_15m) >= 20:
            obs_15m  = detect_order_blocks(candles_15m, symbol, self.SECONDARY_TF)
            fvgs_15m = detect_fvgs(candles_15m, symbol, self.SECONDARY_TF)
            invalidate_obs(obs_15m, candles_15m)
            invalidate_fvgs(fvgs_15m, candles_15m)
            # Keep only buy-side POIs when bias is bullish (and vice versa)
            if bias == Bias.BULLISH:
                obs  += [o for o in obs_15m  if o.side == OrderSide.BUY  and not o.mitigated]
                fvgs += [f for f in fvgs_15m if f.is_bullish              and not f.filled]
            elif bias == Bias.BEARISH:
                obs  += [o for o in obs_15m  if o.side == OrderSide.SELL and not o.mitigated]
                fvgs += [f for f in fvgs_15m if not f.is_bullish          and not f.filled]

        # ── OTE zone from most recent 1H swing pair (time-ordered) ──
        ote_zone = None
        if sh and sl:
            # Sort by timestamp to get truly last swing of each type
            last_sh = sorted(sh, key=lambda s: s.timestamp)[-1]
            last_sl = sorted(sl, key=lambda s: s.timestamp)[-1]
            ote_zone = compute_ote_zone(
                swing_high=last_sh.price,
                swing_low=last_sl.price,
                symbol=symbol,
                is_bullish=(bias == Bias.BULLISH),
                timestamp=time.time(),
            )

        # ── HTF EMAs ──
        closes_1h = [c.close for c in candles_1h]
        ema13 = ema(closes_1h, settings.EMA_FAST)
        ema21 = ema(closes_1h, settings.EMA_SLOW)

        # ── Nearest active POIs ──
        active_obs  = [o for o in obs  if not o.mitigated]
        active_fvgs = [f for f in fvgs if not f.filled]
        poi_highs = [o.zone_high for o in active_obs] + [f.gap_high for f in active_fvgs]
        poi_lows  = [o.zone_low  for o in active_obs] + [f.gap_low  for f in active_fvgs]

        ctx = HTFContext(
            symbol=symbol,
            bias=bias,
            order_blocks=obs,
            fvgs=fvgs,
            liquidity=liq,
            structure_breaks=sb,
            ote_zone=ote_zone,
            ema13_htf=ema13,
            ema21_htf=ema21,
            nearest_poi_high=max(poi_highs) if poi_highs else None,
            nearest_poi_low=min(poi_lows)   if poi_lows  else None,
            is_valid=True,
        )
        self._contexts[symbol] = ctx
        return ctx

    def get_context(self, symbol: str) -> Optional[HTFContext]:
        return self._contexts.get(symbol)


# ─────────────────────────────────────────────────────────────
# Per-symbol runtime state
# ─────────────────────────────────────────────────────────────

@dataclass
class SMCSymbolState:
    symbol: str
    last_htf_run: float = 0.0
    prev_ema13_ltf: Optional[float] = None
    prev_ema21_ltf: Optional[float] = None
    last_signal: Optional[str] = None   # "buy" | "sell"
    cooldown_until: float = 0.0         # epoch — no entries before this


# ─────────────────────────────────────────────────────────────
# SMC Intraday Strategy
# ─────────────────────────────────────────────────────────────

class SMCStrategy:
    """
    Intraday SMC strategy — Conservative mode.

    HTF  : 1H  — bias + POI (OB, FVG, OTE)
    Mid  : 15m — injected into HTF POI list for tighter zones
    LTF  : 5m  — entry confirmation

    Entry: CONSERVATIVE — ALL 4 conditions required:
      1. In HTF POI (OB or FVG from 1H/15m)
      2. In OTE zone (0.618–0.786 retracement)
      3. LTF CHoCH bullish (structural confirmation of reversal)
      4. LTF EMA cross OR rejection candle (momentum confirmation)

    Exit:
      SL          : CUT_LOSS_PCT below entry
      Partial TP  : 50% at 1R
      Full TP     : rest at nR (config)
      Trailing    : TRAILING_STOP_PCT after partial close
    """

    # ── Timeframe config ──────────────────────
    LTF_TIMEFRAME      = "5m"
    LTF_CANDLES_NEEDED = 30          # 30 × 5m = 2.5h of LTF history

    # HTF refresh: every 5 minutes (300s) is enough for 1H bias
    HTF_INTERVAL = settings.HTF_ANALYSIS_INTERVAL  # 300s default

    # Cooldown between entries for same symbol (seconds)
    ENTRY_COOLDOWN = 300   # 5 minutes

    # Ticks to skip between LTF evaluations (throttle)
    TICK_MODULO = 5

    def __init__(
        self,
        engine: PaperTradingEngine,
        price_store: PriceStore,
        candle_provider,
    ) -> None:
        self._engine          = engine
        self._price_store     = price_store
        self._htf             = HTFAnalyser(candle_provider)
        self._candle_provider = candle_provider
        self._states: dict[str, SMCSymbolState] = {
            sym: SMCSymbolState(symbol=sym) for sym in settings.SYMBOLS
        }
        self._tick_count: dict[str, int] = {sym: 0 for sym in settings.SYMBOLS}

    # ── Lifecycle ─────────────────────────────

    def start(self) -> None:
        event_bus.subscribe(Topics.TICK, self._on_tick)
        log.info("SMC Intraday Strategy started (HTF=1H, Mid=15m, LTF=5m)")

    def stop(self) -> None:
        event_bus.unsubscribe(Topics.TICK, self._on_tick)

    # ── Main tick handler ─────────────────────

    async def _on_tick(self, tick: Tick) -> None:
        symbol = tick.symbol
        if symbol not in self._states:
            return

        state = self._states[symbol]
        self._tick_count[symbol] = self._tick_count.get(symbol, 0) + 1

        # Re-run HTF analysis periodically
        now = time.time()
        if now - state.last_htf_run >= self.HTF_INTERVAL:
            self._htf.analyse(symbol)
            state.last_htf_run = now

        # Throttle LTF evaluation
        if self._tick_count[symbol] % self.TICK_MODULO != 0:
            return

        # Manage exits first
        await self._manage_exits(symbol, tick.price)

        # Look for new entry
        if not self._engine.has_position(symbol):
            if now >= state.cooldown_until:
                await self._check_entry(symbol, tick.price, state)

    # ── Exit management ───────────────────────

    async def _manage_exits(self, symbol: str, price: float) -> None:
        pos = self._engine.get_position(symbol)
        if not pos or not pos.is_open:
            return

        # Trailing stop update
        new_sl = pos.update_trailing(price)
        if new_sl:
            log.debug(f"[TRAIL] {symbol} SL → {new_sl:.4f}")

        # Stop loss hit
        if pos.stop_loss > 0 and price <= pos.stop_loss:
            log.warning(f"[SL] {symbol} SL={pos.stop_loss:.4f} hit @ {price:.4f}")
            await self._sell(symbol, price, note="stop_loss")
            self._states[symbol].last_signal = "sell"
            # Cooldown after SL to avoid revenge trading
            self._states[symbol].cooldown_until = time.time() + self.ENTRY_COOLDOWN
            return

        # Take profit
        if pos.take_profit > 0 and price >= pos.take_profit:
            if not pos.partial_closed:
                half_qty = pos.quantity * 0.5
                await self._engine.market_sell(symbol, half_qty, price, note="partial_tp_1R")
                pos.partial_closed = True
                log.info(f"[TP1R] {symbol} 50% closed @ {price:.4f}")
            else:
                await self._sell(symbol, price, note="take_profit_final")
                self._states[symbol].last_signal = "sell"

    # ── Entry logic ───────────────────────────

    async def _check_entry(
        self, symbol: str, price: float, state: SMCSymbolState
    ) -> None:

        # Gate: max open positions
        open_count = sum(1 for s in settings.SYMBOLS if self._engine.has_position(s))
        if open_count >= settings.MAX_OPEN_POSITIONS:
            return

        htf = self._htf.get_context(symbol)
        if htf is None or not htf.is_valid:
            return

        # Only long setups when 1H bias is bullish
        if htf.bias != Bias.BULLISH:
            return

        # ── Condition 1: price inside a HTF POI (OB or FVG) ──
        in_demand_ob = any(
            ob.side == OrderSide.BUY and not ob.mitigated
            and ob.zone_low <= price <= ob.zone_high
            for ob in htf.order_blocks
        )
        in_bullish_fvg = any(
            fvg.is_bullish and not fvg.filled
            and fvg.gap_low <= price <= fvg.gap_high
            for fvg in htf.fvgs
        )
        cond_poi = in_demand_ob or in_bullish_fvg

        # ── Condition 2: price in OTE zone (0.618–0.786) ──
        cond_ote = htf.ote_zone is not None and htf.ote_zone.price_in_ote(price)

        # ── Conditions 3 + 4: LTF (5m) — CHoCH AND (EMA cross OR rejection) ──
        cond_choch, cond_momentum, ltf_detail = await self._ltf_confirmed(symbol, price, state)

        # CONSERVATIVE: ALL 4 conditions must be met
        if not cond_poi:
            log.debug(f"[SKIP] {symbol} — not in POI (price={price:.4f})")
            return
        if not cond_ote:
            log.debug(f"[SKIP] {symbol} — not in OTE zone")
            return
        if not cond_choch:
            log.debug(f"[SKIP] {symbol} — no LTF CHoCH ({ltf_detail})")
            return
        if not cond_momentum:
            log.debug(f"[SKIP] {symbol} — no LTF momentum ({ltf_detail})")
            return

        # Build and execute setup
        setup = self._build_setup(symbol, price, htf)
        if setup is None:
            return

        poi_label = "OB" if in_demand_ob else "FVG"
        note = (
            f"SMC Intraday Conservative | bias=BULLISH | "
            f"POI={poi_label} OTE=✓ CHoCH=✓ Momentum={ltf_detail} | "
            f"SL={setup.stop_loss:.4f} TP={setup.take_profit_1r:.4f}"
        )
        log.info(f"[ENTRY] {symbol} @ {price:.4f} | {note}")
        await event_bus.publish(Topics.SIGNAL_BUY, {"symbol": symbol, "price": price})

        order = await self._engine.market_buy(
            symbol, setup.position_usdt, price, note=note
        )

        if order and order.status.value == "FILLED":
            pos = self._engine.get_position(symbol)
            if pos:
                pos.stop_loss         = setup.stop_loss
                pos.take_profit       = setup.take_profit_1r
                pos.trailing_stop_pct = setup.trailing_stop_pct
                pos.trailing_high     = price
                pos.setup_note        = note
                pos.htf_bias          = htf.bias.value
            state.last_signal = "buy"

    # ── LTF confirmation (5m) ─────────────────

    async def _ltf_confirmed(
        self, symbol: str, price: float, state: SMCSymbolState
    ) -> tuple[bool, bool, str]:
        """
        Returns (choch_confirmed, momentum_confirmed, detail).

        Conservative requires BOTH:
          choch_confirmed   : bullish CHoCH on recent 5m bars
          momentum_confirmed: EMA13 cross above EMA21 OR rejection candle
                              (both require EMA13 > EMA21 at time of check)
        """
        ltf = self._candle_provider(symbol, self.LTF_TIMEFRAME)
        if len(ltf) < self.LTF_CANDLES_NEEDED:
            return False, False, "warming_up"

        closes = [c.close for c in ltf]
        fast_now  = ema(closes, settings.EMA_FAST)   # EMA 13
        slow_now  = ema(closes, settings.EMA_SLOW)    # EMA 21
        if fast_now is None or slow_now is None:
            return False, False, "ema_unavail"

        prev_fast = state.prev_ema13_ltf
        prev_slow = state.prev_ema21_ltf
        state.prev_ema13_ltf = fast_now
        state.prev_ema21_ltf = slow_now

        # ── Condition 3: CHoCH bullish on recent 5m ──
        window = ltf[-40:]
        sh5, sl5 = find_swing_highs_lows(window, lookback=2)
        sb5 = detect_structure_breaks(window, sh5, sl5, symbol, self.LTF_TIMEFRAME)
        choch_confirmed = any(
            sb.direction == OrderSide.BUY and sb.is_choch
            for sb in sb5[-6:]
        )

        # ── Condition 4: EMA cross OR rejection candle (momentum) ──
        ema_cross = (
            prev_fast is not None and prev_slow is not None
            and prev_fast <= prev_slow
            and fast_now > slow_now
        )
        rejection = is_rejection_candle(ltf[-1]) and fast_now > slow_now

        momentum_confirmed = ema_cross or rejection
        detail = (
            f"choch={'✓' if choch_confirmed else '✗'} "
            f"cross={'✓' if ema_cross else '✗'} "
            f"rej={'✓' if rejection else '✗'} "
            f"ema=({fast_now:.2f}/{slow_now:.2f})"
        )
        return choch_confirmed, momentum_confirmed, detail

    # ── Setup builder ─────────────────────────

    def _build_setup(
        self, symbol: str, entry: float, htf: HTFContext
    ) -> Optional[TradeSetup]:
        prices = {
            s: (self._price_store.get_price(s) or entry)
            for s in settings.SYMBOLS
        }
        equity = self._engine.portfolio.total_value(prices)
        position_usdt = equity * settings.POSITION_SIZE_PCT

        if position_usdt < settings.MIN_ORDER_USDT:
            return None

        sl = entry * (1 - settings.CUT_LOSS_PCT)
        risk_per_unit = entry - sl
        if risk_per_unit <= 0:
            return None

        tp_map = {"1:1": 1, "1:2": 2, "1:3": 3}
        r_mult = tp_map.get(settings.DEFAULT_TP_RATIO, 2)
        tp_enum = {1: TPRatio.R1, 2: TPRatio.R2, 3: TPRatio.R3}.get(r_mult, TPRatio.R2)

        return TradeSetup(
            symbol=symbol,
            entry_price=entry,
            stop_loss=sl,
            take_profit_1r=entry + risk_per_unit,
            take_profit_2r=entry + risk_per_unit * 2,
            take_profit_3r=entry + risk_per_unit * 3,
            risk_usdt=position_usdt * settings.CUT_LOSS_PCT,
            position_usdt=position_usdt,
            tp_ratio=tp_enum,
            trailing_stop_pct=settings.TRAILING_STOP_PCT,
            htf_bias=htf.bias,
            note=f"SMC Intraday {htf.bias.value}",
        )

    # ── Helpers ───────────────────────────────

    async def _sell(self, symbol: str, price: float, note: str = "") -> None:
        await event_bus.publish(Topics.SIGNAL_SELL, {"symbol": symbol, "price": price})
        await self._engine.market_sell(symbol, None, price, note=note)

    # ── Dashboard snapshot ────────────────────

    def get_smc_snapshot(self) -> dict:
        result = {}
        for symbol in settings.SYMBOLS:
            htf   = self._htf.get_context(symbol)
            state = self._states.get(symbol)
            ltf   = self._candle_provider(symbol, self.LTF_TIMEFRAME)
            closes_ltf = [c.close for c in ltf] if ltf else []
            result[symbol] = {
                "htf_bias"   : htf.bias.value if htf else "NEUTRAL",
                "htf_valid"  : htf.is_valid   if htf else False,
                "ema13_htf"  : htf.ema13_htf  if htf else None,
                "ema21_htf"  : htf.ema21_htf  if htf else None,
                "ema13_ltf"  : ema(closes_ltf, settings.EMA_FAST) if len(closes_ltf) >= settings.EMA_FAST else None,
                "ema21_ltf"  : ema(closes_ltf, settings.EMA_SLOW) if len(closes_ltf) >= settings.EMA_SLOW else None,
                "ob_count"   : len([o for o in (htf.order_blocks if htf else []) if not o.mitigated]),
                "fvg_count"  : len([f for f in (htf.fvgs        if htf else []) if not f.filled]),
                "ote_zone"   : {
                    "high": htf.ote_zone.ote_high,
                    "low" : htf.ote_zone.ote_low,
                } if htf and htf.ote_zone else None,
                "last_signal": state.last_signal if state else None,
                "cooldown"   : max(0, round(state.cooldown_until - time.time())) if state else 0,
                "structure_breaks": [
                    {"level": sb.level, "dir": sb.direction.value, "choch": sb.is_choch}
                    for sb in (htf.structure_breaks[-5:] if htf else [])
                ],
            }
        return result
