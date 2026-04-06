"""
Backtest report: computes statistics and prints a summary.
"""
from collections import Counter
from backtest.engine import BacktestResults, ExitReason, TradeOutcome


def _stats(trades: list[TradeOutcome], trade_amount: float) -> dict:
    if not trades:
        return {}

    pnls      = [t.pnl_pct for t in trades]
    wins      = [t for t in trades if t.pnl_pct > 0]
    losses    = [t for t in trades if t.pnl_pct <= 0]
    reasons   = Counter(t.exit_reason for t in trades)
    ambiguous = sum(1 for t in trades if t.ambiguous)

    total_pct    = sum(pnls)
    # Convert % PnL per trade to USDT (each trade stakes trade_amount_usdt)
    total_usdt   = sum(t.pnl_pct / 100 * trade_amount for t in trades)
    avg_pnl      = total_pct / len(trades)
    win_rate     = len(wins) / len(trades) * 100
    avg_hold     = sum(t.candles_held for t in trades) / len(trades)

    best  = max(trades, key=lambda t: t.pnl_pct)
    worst = min(trades, key=lambda t: t.pnl_pct)

    return {
        "n_trades":    len(trades),
        "win_rate":    win_rate,
        "total_pct":   total_pct,
        "total_usdt":  total_usdt,
        "avg_pnl":     avg_pnl,
        "avg_hold":    avg_hold,
        "n_wins":      len(wins),
        "n_losses":    len(losses),
        "n_tp":        reasons[ExitReason.TAKE_PROFIT],
        "n_sl":        reasons[ExitReason.STOP_LOSS],
        "n_timeout":   reasons[ExitReason.TIMEOUT],
        "n_ambiguous": ambiguous,
        "best":        best,
        "worst":       worst,
    }


def print_report(results: BacktestResults):
    p = results.params
    opt = _stats(results.optimistic, p.trade_amount_usdt)
    pes = _stats(results.pessimistic, p.trade_amount_usdt)

    if not opt:
        print("No trades to report.")
        return

    bar = "─" * 60

    print(f"\n{'═'*60}")
    print(f"  BACKTEST REPORT – Bitget New Listing Spike Bot")
    print(f"{'═'*60}")
    print(f"  Parameters")
    print(f"  {'Trade size':<25} {p.trade_amount_usdt} USDT")
    print(f"  {'Take profit':<25} +{p.take_profit_pct}%")
    print(f"  {'Stop loss':<25} -{p.stop_loss_pct}%")
    print(f"  {'Max hold':<25} {p.max_hold_candles} candles (1 min each)")
    print(f"  {'Slippage simulated':<25} {p.slippage_pct}%")
    print(bar)

    for label, s in [("OPTIMISTIC (TP before SL)", opt), ("PESSIMISTIC (SL before TP)", pes)]:
        print(f"\n  {label}")
        print(f"  {'Trades':<25} {s['n_trades']}")
        print(f"  {'Win rate':<25} {s['win_rate']:.1f}%")
        print(f"  {'Total PnL (USDT)':<25} {s['total_usdt']:+.2f}")
        print(f"  {'Avg PnL per trade':<25} {s['avg_pnl']:+.2f}%")
        print(f"  {'Avg hold (candles)':<25} {s['avg_hold']:.1f}")
        print(f"  {'TP exits':<25} {s['n_tp']}")
        print(f"  {'SL exits':<25} {s['n_sl']}")
        print(f"  {'Timeout exits':<25} {s['n_timeout']}")
        print(f"  {'Ambiguous candles':<25} {s['n_ambiguous']}")
        print(f"  {'Best trade':<25} {s['best'].symbol} {s['best'].pnl_pct:+.2f}%")
        print(f"  {'Worst trade':<25} {s['worst'].symbol} {s['worst'].pnl_pct:+.2f}%")

    print(f"\n{bar}")
    print(f"  NOTE: 1-min candles cannot capture sub-second spikes precisely.")
    print(f"  The true result lies between optimistic and pessimistic bounds.")
    print(f"{'═'*60}\n")


def export_csv(results: BacktestResults, path: str = "backtest_results.csv"):
    """Export optimistic results to CSV for further analysis."""
    import csv
    from datetime import datetime, timezone

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "symbol", "listed_at", "entry_price", "exit_price_opt", "exit_price_pes",
            "pnl_opt_pct", "pnl_pes_pct", "exit_reason_opt", "exit_reason_pes",
            "candles_held", "ambiguous",
        ])
        for opt, pes in zip(results.optimistic, results.pessimistic):
            dt = datetime.fromtimestamp(opt.listed_at / 1000, tz=timezone.utc).isoformat()
            writer.writerow([
                opt.symbol, dt,
                f"{opt.entry_price:.8f}",
                f"{opt.exit_price:.8f}",
                f"{pes.exit_price:.8f}",
                f"{opt.pnl_pct:.2f}",
                f"{pes.pnl_pct:.2f}",
                opt.exit_reason.name,
                pes.exit_reason.name,
                opt.candles_held,
                opt.ambiguous,
            ])
    print(f"Results exported to {path}")
