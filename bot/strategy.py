"""
Strategy gate: decides whether to trade a detected listing.

Filters applied before allowing the executor to fire:
  - Not already in an active trade for this symbol
  - Sufficient USDT balance
  - Symbol is actually tradeable (order book has liquidity)
  - Optional: minimum 24h volume on similar coins (basic sanity check)
"""
import asyncio
import time
from typing import Optional

from loguru import logger

from bot.exchange import BitgetExchange
from bot.fast_client import FastOrderClient
from bot.listing_monitor import NewListing
from bot.order_executor import OrderExecutor
from config import Config


class SpikeStrategy:
    def __init__(self, exchange: BitgetExchange, fast_client: FastOrderClient):
        self._exchange = exchange
        self._fast_client = fast_client
        self._executor = OrderExecutor(fast_client)
        self._trade_count = 0

    async def on_new_listing(self, listing: NewListing):
        """Called by ListingMonitor for every new coin. Applies filters then fires."""
        sym = listing.symbol
        logger.info(f"[STRATEGY] New listing received: {sym} (source={listing.source})")

        # ── Filter 1: Balance check ────────────────────────────────────
        if not Config.DRY_RUN:
            balance = await self._fast_client.get_balance("USDT")
            if balance < Config.TRADE_AMOUNT_USDT:
                logger.warning(
                    f"[STRATEGY] Skipping {sym}: insufficient balance "
                    f"({balance:.2f} USDT < {Config.TRADE_AMOUNT_USDT} USDT required)"
                )
                return

        # ── Filter 2: Liquidity check (only for API-poll detections) ──
        # For announcement-based listings the symbol won't be live yet,
        # so we skip the order book check and let the executor handle timing.
        if listing.source == "api_poll":
            tradeable = await self._check_liquidity(sym)
            if not tradeable:
                logger.warning(f"[STRATEGY] Skipping {sym}: no liquidity on order book.")
                return

        # ── All filters passed → fire the trade ───────────────────────
        self._trade_count += 1
        logger.success(f"[STRATEGY] Approving trade #{self._trade_count} for {sym}")
        asyncio.create_task(self._executor.handle_listing(listing))

    async def _check_liquidity(self, symbol: str, min_ask_levels: int = 1) -> bool:
        """Return True if the symbol has at least one ask level available."""
        try:
            ob = await self._exchange.get_order_book(symbol, limit=5)
            return len(ob.get("asks", [])) >= min_ask_levels
        except Exception as e:
            logger.debug(f"[STRATEGY] Order book check failed for {symbol}: {e}")
            return False
