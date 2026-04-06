"""
Listing monitor: detects new USDT spot pairs appearing on Bitget.

Two detection methods run in parallel:
  1. API polling  – compares the live market list against the known snapshot.
  2. Announcement scraping – parses Bitget's announcement feed for listing notices
     and extracts the scheduled launch timestamp so the executor can pre-arm itself.
"""
import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Optional

import aiohttp
from loguru import logger

from bot.exchange import BitgetExchange
from config import Config

# Bitget public announcement API (no auth required)
_ANNOUNCEMENTS_URL = "https://api.bitget.com/api/v2/public/annc/list"
_ANNC_KEYWORDS = ["will list", "new listing", "listing notice", "spot trading"]


@dataclass
class NewListing:
    symbol: str                     # e.g. "NEWTOKEN/USDT"
    base: str                       # e.g. "NEWTOKEN"
    detected_at: float = field(default_factory=time.time)
    scheduled_at: Optional[float] = None   # Unix timestamp if known from announcement
    source: str = "api_poll"        # "api_poll" | "announcement"


NewListingCallback = Callable[[NewListing], Awaitable[None]]


class ListingMonitor:
    """
    Continuously scans Bitget for new spot listings and fires a callback
    for every newly detected coin.
    """

    def __init__(self, exchange: BitgetExchange, on_new_listing: NewListingCallback):
        self._exchange = exchange
        self._on_new_listing = on_new_listing
        self._known_symbols: set[str] = set()
        self._seen_annc_ids: set[str] = set()
        self._running = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self):
        """Initialise the known-symbols snapshot and start both monitors."""
        logger.info("Loading initial market snapshot…")
        self._known_symbols = await self._exchange.get_all_symbols()
        logger.info(f"Snapshot loaded: {len(self._known_symbols)} USDT pairs tracked.")
        self._running = True
        await asyncio.gather(
            self._poll_new_symbols(),
            self._poll_announcements(),
        )

    def stop(self):
        self._running = False

    # ------------------------------------------------------------------
    # Internal – symbol diff polling
    # ------------------------------------------------------------------

    async def _poll_new_symbols(self):
        interval = Config.POLL_INTERVAL_MS / 1000
        while self._running:
            try:
                current = await self._exchange.get_all_symbols()
                new = current - self._known_symbols
                for sym in new:
                    base = sym.split("/")[0]
                    listing = NewListing(symbol=sym, base=base, source="api_poll")
                    logger.success(f"[POLL] NEW LISTING DETECTED: {sym}")
                    self._known_symbols.add(sym)
                    asyncio.create_task(self._on_new_listing(listing))
            except Exception as e:
                logger.warning(f"[POLL] Error fetching symbols: {e}")
            await asyncio.sleep(interval)

    # ------------------------------------------------------------------
    # Internal – announcement feed
    # ------------------------------------------------------------------

    async def _poll_announcements(self):
        """
        Fetch Bitget's public announcement list every 10 s.
        When we spot a listing notice, parse out the coin ticker and
        scheduled launch time (if present) so the executor can pre-arm.
        """
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    await self._fetch_announcements(session)
                except Exception as e:
                    logger.warning(f"[ANNC] Error fetching announcements: {e}")
                await asyncio.sleep(10)

    async def _fetch_announcements(self, session: aiohttp.ClientSession):
        params = {"language": "en_US", "pageSize": "20", "pageNo": "1"}
        async with session.get(_ANNOUNCEMENTS_URL, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data = await resp.json()

        items = data.get("data", {}).get("items") or data.get("data", [])
        if not isinstance(items, list):
            return

        for item in items:
            annc_id = str(item.get("id") or item.get("annId") or "")
            if annc_id in self._seen_annc_ids:
                continue
            self._seen_annc_ids.add(annc_id)

            title: str = (item.get("annTitle") or item.get("title") or "").lower()
            if not any(kw in title for kw in _ANNC_KEYWORDS):
                continue

            tickers = self._extract_tickers(item.get("annTitle") or item.get("title") or "")
            scheduled_at = self._extract_timestamp(item.get("annContent") or item.get("content") or "")

            for ticker in tickers:
                sym = f"{ticker}/USDT"
                if sym in self._known_symbols:
                    continue  # Already live
                listing = NewListing(
                    symbol=sym,
                    base=ticker,
                    source="announcement",
                    scheduled_at=scheduled_at,
                )
                logger.success(f"[ANNC] Upcoming listing detected: {sym} | launch={scheduled_at}")
                asyncio.create_task(self._on_new_listing(listing))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_tickers(text: str) -> list[str]:
        """Extract uppercase coin tickers from an announcement title."""
        # Match patterns like (BTC), BTC/USDT, "BTC", BTC Spot
        raw = re.findall(r'\b([A-Z]{2,10})\b', text.upper())
        blacklist = {"USDT", "USDC", "BTC", "ETH", "SPOT", "THE", "AND", "NEW", "LIST", "WILL", "FOR"}
        return [t for t in raw if t not in blacklist]

    @staticmethod
    def _extract_timestamp(content: str) -> Optional[float]:
        """
        Try to parse an ISO-8601 or epoch timestamp from announcement content.
        Returns a Unix timestamp float, or None.
        """
        from datetime import datetime, timezone
        # Match patterns like "2024-12-01 10:00 (UTC)"
        iso_pattern = re.search(
            r'(\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}(?::\d{2})?)\s*(?:\(UTC\))?',
            content,
        )
        if iso_pattern:
            raw = iso_pattern.group(1).strip().replace(" ", "T")
            try:
                dt = datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except ValueError:
                pass
        return None
