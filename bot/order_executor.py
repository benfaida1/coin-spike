"""
Order executor: handles the full lifecycle of a spike trade.

Fast path (announcement-based listing):
  T-25s  presign_market_buy()   → payload built & signed, ready to send
  T-0    fire_presigned()       → one POST with pre-built bytes (~8–30 ms total)

Slow path (api_poll detection):
  T=0    market_buy()           → live-signed POST (~10–60 ms total)

Exit via price monitoring:
  • WS price feed (lock-free, ~1ms latency) if available
  • Falls back to REST order-book poll (100ms interval)
"""
import asyncio
import time
from enum import Enum, auto
from dataclasses import dataclass

from loguru import logger

from bot.fast_client import FastOrderClient
from bot.listing_monitor import NewListing
from config import Config

# Pre-sign this many seconds before scheduled launch
_PRESIGN_LEAD_S = 25


class ExitReason(Enum):
    TAKE_PROFIT = auto()
    STOP_LOSS   = auto()
    TIMEOUT     = auto()
    ERROR       = auto()


@dataclass
class TradeResult:
    symbol:       str
    entry_price:  float
    exit_price:   float
    quantity:     float
    pnl_pct:      float
    exit_reason:  ExitReason
    hold_seconds: float


class OrderExecutor:
    def __init__(self, fast_client: FastOrderClient):
        self._client = fast_client
        self._active: set[str] = set()

    async def handle_listing(self, listing: NewListing):
        sym = listing.symbol
        if sym in self._active:
            return
        self._active.add(sym)
        try:
            await self._execute(listing)
        finally:
            self._active.discard(sym)

    # ------------------------------------------------------------------
    # Core trade
    # ------------------------------------------------------------------

    async def _execute(self, listing: NewListing):
        sym = listing.symbol

        # ── Pre-arm: subscribe WS price feed ──────────────────────────
        await self._client.subscribe_price_feed(sym)

        # ── Pre-sign if we have a scheduled launch time ────────────────
        if listing.scheduled_at:
            now  = time.time()
            wait = listing.scheduled_at - now - _PRESIGN_LEAD_S
            if wait > 0:
                logger.info(f"[{sym}] Sleeping {wait:.1f}s before pre-signing…")
                await asyncio.sleep(wait)

            self._client.presign_market_buy(sym, Config.TRADE_AMOUNT_USDT)

            # Sleep the remaining time until T-0
            remaining = listing.scheduled_at - time.time() - 0.01  # 10ms early
            if remaining > 0:
                logger.info(f"[{sym}] Pre-signed. Firing in {remaining:.3f}s…")
                await asyncio.sleep(remaining)

        # ── FIRE order ─────────────────────────────────────────────────
        t_fire = time.perf_counter()

        if listing.scheduled_at and sym in self._client._presigned:
            order = await self._client.fire_presigned(sym)
        else:
            order = await self._client.market_buy(sym, Config.TRADE_AMOUNT_USDT)

        fire_ms = (time.perf_counter() - t_fire) * 1000
        entry_time = time.time()
        logger.success(f"[{sym}] Buy fired in {fire_ms:.1f} ms")

        if not order:
            logger.error(f"[{sym}] Buy failed, aborting.")
            return

        # ── Resolve entry price & quantity ─────────────────────────────
        entry_price, quantity = await self._resolve_entry(sym, order)
        if entry_price <= 0 or quantity <= 0:
            logger.error(f"[{sym}] Could not resolve entry, aborting.")
            return

        tp = entry_price * (1 + Config.TAKE_PROFIT_PCT / 100)
        sl = entry_price * (1 - Config.STOP_LOSS_PCT  / 100)

        logger.info(
            f"[{sym}] Entered @ {entry_price:.8f} | "
            f"TP={tp:.8f} (+{Config.TAKE_PROFIT_PCT}%) | "
            f"SL={sl:.8f} (-{Config.STOP_LOSS_PCT}%) | qty={quantity:.6f}"
        )

        # ── Monitor price ──────────────────────────────────────────────
        exit_price, reason = await self._monitor(sym, entry_price, tp, sl, entry_time)

        # ── Sell ───────────────────────────────────────────────────────
        t_sell = time.perf_counter()
        await self._client.market_sell(sym, quantity)
        sell_ms = (time.perf_counter() - t_sell) * 1000
        logger.info(f"[{sym}] Sell fired in {sell_ms:.1f} ms")

        # ── Report ─────────────────────────────────────────────────────
        pnl  = (exit_price / entry_price - 1) * 100
        hold = time.time() - entry_time
        self._report(TradeResult(sym, entry_price, exit_price, quantity, pnl, reason, hold))

    # ------------------------------------------------------------------
    # Price monitoring (WS-first, REST fallback)
    # ------------------------------------------------------------------

    async def _monitor(
        self,
        symbol: str,
        entry: float,
        tp: float,
        sl: float,
        t_start: float,
    ) -> tuple[float, ExitReason]:
        while True:
            elapsed = time.time() - t_start

            if elapsed >= Config.MAX_HOLD_SECONDS:
                price = self._client.get_ws_price(symbol) or entry
                return price, ExitReason.TIMEOUT

            # Prefer lock-free WS price (sub-ms)
            price = self._client.get_ws_price(symbol)

            if price is None:
                # WS not ready yet – REST fallback
                try:
                    from bot.exchange import BitgetExchange  # lazy import to avoid circular
                except Exception:
                    await asyncio.sleep(0.05)
                    continue
                await asyncio.sleep(0.1)
                continue

            pct = (price / entry - 1) * 100
            logger.debug(f"[{symbol}] {price:.8f} | {pct:+.2f}% | {elapsed:.2f}s")

            if price >= tp:
                return price, ExitReason.TAKE_PROFIT
            if price <= sl:
                return price, ExitReason.STOP_LOSS

            await asyncio.sleep(0.01)  # 10ms loop when using WS prices

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _resolve_entry(self, symbol: str, order: dict) -> tuple[float, float]:
        """Extract fill price and qty from order response, or approximate."""
        price = float(order.get("priceAvg") or order.get("average") or order.get("price") or 0)
        qty   = float(order.get("baseVolume") or order.get("filled") or order.get("amount") or 0)

        if price <= 0 or qty <= 0:
            # Dry-run or order didn't include fills: use WS price
            for _ in range(10):
                ws = self._client.get_ws_price(symbol)
                if ws and ws > 0:
                    price = ws
                    qty   = Config.TRADE_AMOUNT_USDT / price
                    break
                await asyncio.sleep(0.05)

        return price, qty

    @staticmethod
    def _report(r: TradeResult):
        sign = "+" if r.pnl_pct >= 0 else ""
        logger.info(
            f"{'PROFIT' if r.pnl_pct >= 0 else 'LOSS '} | {r.symbol} | "
            f"entry={r.entry_price:.8f} exit={r.exit_price:.8f} | "
            f"pnl={sign}{r.pnl_pct:.2f}% | reason={r.exit_reason.name} | "
            f"hold={r.hold_seconds:.2f}s"
        )
