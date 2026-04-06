"""
Order executor: handles the full lifecycle of a spike trade.

Flow:
  1. Receive a NewListing signal.
  2. If the listing has a scheduled_at time, sleep until T-0.
  3. Fire a market buy immediately.
  4. Start a tight price-polling loop.
  5. Exit (market sell) when:
     - Price >= entry * (1 + TAKE_PROFIT_PCT / 100)   → take profit
     - Price <= entry * (1 - STOP_LOSS_PCT / 100)     → stop loss
     - Time elapsed > MAX_HOLD_SECONDS                 → time exit
"""
import asyncio
import time
from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional

from loguru import logger

from bot.exchange import BitgetExchange
from bot.listing_monitor import NewListing
from config import Config


class ExitReason(Enum):
    TAKE_PROFIT = auto()
    STOP_LOSS = auto()
    TIMEOUT = auto()
    ERROR = auto()


@dataclass
class TradeResult:
    symbol: str
    entry_price: float
    exit_price: float
    quantity: float
    pnl_pct: float
    exit_reason: ExitReason
    hold_seconds: float


class OrderExecutor:
    def __init__(self, exchange: BitgetExchange):
        self._exchange = exchange
        self._active_symbols: set[str] = set()

    async def handle_listing(self, listing: NewListing):
        """Entry point called by ListingMonitor for every new coin."""
        sym = listing.symbol

        if sym in self._active_symbols:
            logger.debug(f"Already trading {sym}, skipping.")
            return

        self._active_symbols.add(sym)
        try:
            await self._execute_spike_trade(listing)
        finally:
            self._active_symbols.discard(sym)

    # ------------------------------------------------------------------
    # Core trade logic
    # ------------------------------------------------------------------

    async def _execute_spike_trade(self, listing: NewListing):
        sym = listing.symbol
        base = listing.base

        # ── 1. Pre-arm: wait until scheduled launch ────────────────────
        if listing.scheduled_at:
            now = time.time()
            wait = listing.scheduled_at - now - 0.05  # arrive 50 ms early
            if wait > 0:
                logger.info(f"[{sym}] Waiting {wait:.1f}s until scheduled launch…")
                await asyncio.sleep(wait)

        # ── 2. Market buy ──────────────────────────────────────────────
        logger.info(f"[{sym}] Firing market BUY for {Config.TRADE_AMOUNT_USDT} USDT")
        buy_order = await self._exchange.market_buy(sym, Config.TRADE_AMOUNT_USDT)

        if not buy_order:
            logger.error(f"[{sym}] Buy order failed, aborting.")
            return

        entry_time = time.time()

        # ── 3. Determine entry price and quantity ──────────────────────
        entry_price, quantity = await self._get_entry_details(sym, buy_order)
        if entry_price <= 0 or quantity <= 0:
            logger.error(f"[{sym}] Could not determine entry price/qty, aborting.")
            return

        tp_price = entry_price * (1 + Config.TAKE_PROFIT_PCT / 100)
        sl_price = entry_price * (1 - Config.STOP_LOSS_PCT / 100)

        logger.info(
            f"[{sym}] Entered @ {entry_price:.8f} | "
            f"TP={tp_price:.8f} (+{Config.TAKE_PROFIT_PCT}%) | "
            f"SL={sl_price:.8f} (-{Config.STOP_LOSS_PCT}%) | "
            f"Qty={quantity:.6f}"
        )

        # ── 4. Price monitoring loop ───────────────────────────────────
        exit_price, exit_reason = await self._monitor_price(
            sym, entry_price, tp_price, sl_price, entry_time
        )

        # ── 5. Market sell ─────────────────────────────────────────────
        logger.info(f"[{sym}] Exiting ({exit_reason.name}) @ {exit_price:.8f}")
        await self._exchange.market_sell(sym, quantity)

        # ── 6. Report ──────────────────────────────────────────────────
        pnl_pct = (exit_price / entry_price - 1) * 100
        hold = time.time() - entry_time
        result = TradeResult(
            symbol=sym,
            entry_price=entry_price,
            exit_price=exit_price,
            quantity=quantity,
            pnl_pct=pnl_pct,
            exit_reason=exit_reason,
            hold_seconds=hold,
        )
        self._log_result(result)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _get_entry_details(self, symbol: str, buy_order: dict) -> tuple[float, float]:
        """
        Extract average fill price and filled quantity from the buy order.
        Falls back to polling the order book if the order doesn't have fills.
        """
        avg_price = float(buy_order.get("average") or buy_order.get("price") or 0)
        filled_qty = float(buy_order.get("filled") or buy_order.get("amount") or 0)

        if avg_price <= 0 or filled_qty <= 0:
            if Config.DRY_RUN:
                # Simulate: fetch best ask as entry price, compute quantity
                try:
                    price = await self._exchange.poll_price(symbol)
                    avg_price = price
                    filled_qty = Config.TRADE_AMOUNT_USDT / price if price > 0 else 0
                except Exception:
                    return 0.0, 0.0
            else:
                # Try to fetch order details from exchange
                try:
                    await asyncio.sleep(0.3)
                    ticker = await self._exchange.get_ticker(symbol)
                    avg_price = float(ticker.get("last") or 0)
                    filled_qty = Config.TRADE_AMOUNT_USDT / avg_price if avg_price > 0 else 0
                except Exception:
                    return 0.0, 0.0

        return avg_price, filled_qty

    async def _monitor_price(
        self,
        symbol: str,
        entry_price: float,
        tp_price: float,
        sl_price: float,
        entry_time: float,
    ) -> tuple[float, ExitReason]:
        """
        Poll price at ~100 ms intervals and return (price, reason) when an
        exit condition is triggered.
        """
        while True:
            elapsed = time.time() - entry_time

            # Time-based exit
            if elapsed >= Config.MAX_HOLD_SECONDS:
                try:
                    current = await self._exchange.poll_price(symbol)
                except Exception:
                    current = entry_price
                return current, ExitReason.TIMEOUT

            try:
                current = await self._exchange.poll_price(symbol)
            except Exception as e:
                logger.warning(f"[{symbol}] Price fetch error: {e}")
                await asyncio.sleep(0.1)
                continue

            pct = (current / entry_price - 1) * 100
            logger.debug(f"[{symbol}] Price={current:.8f} | {pct:+.2f}% | t={elapsed:.2f}s")

            if current >= tp_price:
                return current, ExitReason.TAKE_PROFIT
            if current <= sl_price:
                return current, ExitReason.STOP_LOSS

            await asyncio.sleep(0.1)  # 100 ms poll

    @staticmethod
    def _log_result(result: TradeResult):
        emoji = "✅" if result.pnl_pct >= 0 else "❌"
        logger.info(
            f"{emoji} TRADE CLOSED | {result.symbol} | "
            f"Entry={result.entry_price:.8f} | Exit={result.exit_price:.8f} | "
            f"PnL={result.pnl_pct:+.2f}% | Reason={result.exit_reason.name} | "
            f"Hold={result.hold_seconds:.2f}s"
        )
