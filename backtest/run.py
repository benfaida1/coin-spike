#!/usr/bin/env python3
"""
Backtest runner for the coin-spike strategy.

Usage:
  python -m backtest.run                        # default: last 180 days
  python -m backtest.run --days 90              # last 90 days
  python -m backtest.run --tp 200 --sl 20       # custom TP/SL
  python -m backtest.run --csv results.csv      # export to CSV
  python -m backtest.run --days 30 --amount 50  # $50 per trade, 30-day window

Options:
  --days N        How many past days to include (default: 180)
  --min-age N     Exclude coins listed less than N days ago (default: 1)
  --tp PCT        Take profit % (default: 150)
  --sl PCT        Stop loss % (default: 30)
  --hold N        Max hold in 1-min candles (default: 10)
  --slippage PCT  Simulated entry slippage % (default: 1.0)
  --amount USDT   Trade size in USDT (default: 10)
  --csv PATH      Export results to CSV (optional)
"""
import argparse
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from loguru import logger
from backtest.data_fetcher import fetch_all_listing_candles
from backtest.engine import BacktestParams, run_backtest
from backtest.report import print_report, export_csv


def parse_args():
    p = argparse.ArgumentParser(description="Backtest the coin-spike strategy on Bitget listings.")
    p.add_argument("--days",      type=int,   default=180,  help="Look-back window in days")
    p.add_argument("--min-age",   type=int,   default=1,    help="Exclude listings newer than N days")
    p.add_argument("--tp",        type=float, default=150.0, help="Take profit %%")
    p.add_argument("--sl",        type=float, default=30.0,  help="Stop loss %%")
    p.add_argument("--hold",      type=int,   default=10,   help="Max hold in 1-min candles")
    p.add_argument("--slippage",  type=float, default=1.0,  help="Entry slippage %%")
    p.add_argument("--amount",    type=float, default=10.0, help="Trade amount in USDT")
    p.add_argument("--csv",       type=str,   default=None, help="Export CSV path")
    return p.parse_args()


async def main():
    args = parse_args()

    logger.remove()
    logger.add(sys.stderr, format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

    params = BacktestParams(
        trade_amount_usdt = args.amount,
        take_profit_pct   = args.tp,
        stop_loss_pct     = args.sl,
        max_hold_candles  = args.hold,
        slippage_pct      = args.slippage,
    )

    logger.info(
        f"Fetching listings from last {args.days} days "
        f"(excluding last {args.min_age} day(s))…"
    )
    dataset = await fetch_all_listing_candles(
        min_age_days = args.min_age,
        max_age_days = args.days,
    )

    if not dataset:
        logger.error("No listing data found. Check your internet connection.")
        return

    logger.info(f"Running backtest on {len(dataset)} listings…")
    results = run_backtest(dataset, params)

    print_report(results)

    if args.csv:
        export_csv(results, args.csv)


if __name__ == "__main__":
    asyncio.run(main())
