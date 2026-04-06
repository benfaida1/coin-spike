#!/usr/bin/env python3
"""
coin-spike – Bitget new-listing spike bot
==========================================
Monitors Bitget for new USDT spot pairs and immediately enters a trade
to capture the typical +200% spike that occurs in the first second of trading.

Usage:
  cp .env.example .env          # fill in your API keys
  pip install -r requirements.txt
  python main.py                # DRY_RUN=true by default
"""
import asyncio
import sys

from loguru import logger

from bot.exchange import BitgetExchange
from bot.listing_monitor import ListingMonitor
from bot.strategy import SpikeStrategy
from config import Config


def setup_logging():
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | {message}",
        level="DEBUG" if Config.DRY_RUN else "INFO",
        colorize=True,
    )
    logger.add(
        "logs/coin-spike.log",
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
    )


async def main():
    setup_logging()
    Config.validate()

    mode = "DRY RUN" if Config.DRY_RUN else "LIVE"
    logger.info(f"=== coin-spike starting [{mode}] ===")
    logger.info(f"Trade size : {Config.TRADE_AMOUNT_USDT} USDT")
    logger.info(f"Take profit: +{Config.TAKE_PROFIT_PCT}%")
    logger.info(f"Stop loss  : -{Config.STOP_LOSS_PCT}%")
    logger.info(f"Max hold   : {Config.MAX_HOLD_SECONDS}s")
    logger.info(f"Poll interval: {Config.POLL_INTERVAL_MS} ms")

    exchange = BitgetExchange()
    strategy = SpikeStrategy(exchange)
    monitor = ListingMonitor(exchange, on_new_listing=strategy.on_new_listing)

    try:
        await monitor.start()
    except KeyboardInterrupt:
        logger.info("Shutting down…")
    finally:
        monitor.stop()
        await exchange.close()
        logger.info("Bye.")


if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    asyncio.run(main())
