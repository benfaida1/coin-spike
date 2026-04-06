"""
Fetches historical listing data from Bitget for backtesting.

Two sources:
  1. /api/v2/spot/public/symbols  → all current symbols + their listing timestamp
  2. /api/v2/spot/market/candles  → OHLCV klines (min granularity: 1min)

Since the spike happens in <1 second and we only have 1-minute candles, the
backtester uses a conservative model:
  - Entry  = open of candle[0]   (first candle after listing)
  - TP hit = high[0] >= entry * (1 + TP%)
  - SL hit = low[0]  <= entry * (1 - SL%)
  If both TP and SL are hit in the same candle, we assume TP hit first
  (optimistic) OR SL hit first (pessimistic). The report shows both.
"""
import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

import aiohttp
from loguru import logger


_BASE = "https://api.bitget.com"
_SYMBOLS_URL = f"{_BASE}/api/v2/spot/public/symbols"
_CANDLES_URL = f"{_BASE}/api/v2/spot/market/candles"


@dataclass
class ListingCandles:
    symbol: str              # e.g. "NEWTOKENUSDT"
    listed_at_ms: int        # exchange listing timestamp (ms)
    candles: list            # list of [ts, open, high, low, close, vol, quoteVol]


async def fetch_all_listings(
    session: aiohttp.ClientSession,
    min_age_days: int = 1,
    max_age_days: int = 365,
) -> list[dict]:
    """
    Return all USDT spot symbols that were listed within the given age window.
    """
    now_ms = int(time.time() * 1000)
    min_ts = now_ms - max_age_days * 86400_000
    max_ts = now_ms - min_age_days * 86400_000

    async with session.get(_SYMBOLS_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        data = await resp.json()

    symbols = data.get("data", [])
    result = []
    for s in symbols:
        if s.get("quoteCoin") != "USDT":
            continue
        if s.get("status") not in ("online", "halt"):
            continue
        listed_ms = int(s.get("openTime") or s.get("onboardDate") or 0)
        if listed_ms and min_ts <= listed_ms <= max_ts:
            result.append({
                "symbol":     s["symbol"],          # e.g. "NEWTOKENUSDT"
                "baseCoin":   s["baseCoin"],
                "listed_at":  listed_ms,
            })

    logger.info(f"Found {len(result)} listings in the [{min_age_days}d – {max_age_days}d] window.")
    return result


async def fetch_first_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    listed_at_ms: int,
    granularity: str = "1min",
    limit: int = 60,
) -> Optional[ListingCandles]:
    """
    Fetch the first `limit` 1-minute candles starting from the listing timestamp.
    Returns None if no candles are available.
    """
    params = {
        "symbol":      symbol,
        "granularity": granularity,
        "startTime":   str(listed_at_ms),
        "endTime":     str(listed_at_ms + limit * 60_000),
        "limit":       str(limit),
    }
    try:
        async with session.get(
            _CANDLES_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            data = await resp.json()
    except Exception as e:
        logger.warning(f"[{symbol}] Candle fetch error: {e}")
        return None

    candles = data.get("data", [])
    if not candles:
        return None

    # Bitget returns [ts, open, high, low, close, baseVol, quoteVol]
    # Sort ascending by timestamp
    candles.sort(key=lambda c: int(c[0]))

    return ListingCandles(symbol=symbol, listed_at_ms=listed_at_ms, candles=candles)


async def fetch_all_listing_candles(
    min_age_days: int = 1,
    max_age_days: int = 180,
    concurrency: int = 10,
) -> list[ListingCandles]:
    """
    Main entry point: fetch symbols + their first candles with rate-limited concurrency.
    """
    connector = aiohttp.TCPConnector(limit=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        listings = await fetch_all_listings(session, min_age_days, max_age_days)

        sem = asyncio.Semaphore(concurrency)

        async def fetch_one(listing: dict) -> Optional[ListingCandles]:
            async with sem:
                result = await fetch_first_candles(
                    session,
                    listing["symbol"],
                    listing["listed_at"],
                )
                await asyncio.sleep(0.1)  # gentle rate limit
                return result

        tasks = [fetch_one(l) for l in listings]
        results = await asyncio.gather(*tasks)

    valid = [r for r in results if r is not None and r.candles]
    logger.info(f"Fetched candles for {len(valid)}/{len(listings)} listings.")
    return valid
