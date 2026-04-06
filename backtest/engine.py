"""
Backtest engine: simulates the spike strategy on historical listing candles.

Candle model
────────────
Each candle = [timestamp_ms, open, high, low, close, baseVol, quoteVol]

Simulation per listing
──────────────────────
candle[0]  → first candle after listing starts (our entry candle)
  entry_price = open (we assume we get filled at the open)
  slippage    = entry_price * (1 + SLIPPAGE_PCT/100)  (worst-case fill)

  TP target   = entry_after_slippage * (1 + TP_PCT/100)
  SL target   = entry_after_slippage * (1 - SL_PCT/100)

  Within candle[0]:
    - If high  >= TP  and low > SL  → TP hit  (optimistic)
    - If low   <= SL  and high < TP → SL hit  (pessimistic)
    - If both high>=TP and low<=SL  → ambiguous (report both scenarios)
    - If neither                    → continue to candle[1…MAX_HOLD_CANDLES]

  Subsequent candles follow the same logic until TP/SL/timeout.

Two result modes are returned per trade:
  optimistic  – when ambiguous candle, assume TP hit before SL
  pessimistic – when ambiguous candle, assume SL hit before TP
"""
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional

from backtest.data_fetcher import ListingCandles


class ExitReason(Enum):
    TAKE_PROFIT = auto()
    STOP_LOSS   = auto()
    TIMEOUT     = auto()


@dataclass
class TradeOutcome:
    symbol:      str
    listed_at:   int          # ms
    entry_price: float
    exit_price:  float
    pnl_pct:     float
    exit_reason: ExitReason
    candles_held: int
    ambiguous:   bool = False  # TP and SL both hit in same candle


@dataclass
class BacktestParams:
    trade_amount_usdt: float = 10.0
    take_profit_pct:   float = 150.0
    stop_loss_pct:     float = 30.0
    max_hold_candles:  int   = 10     # 10 × 1-min candles = 10 minutes max hold
    slippage_pct:      float = 1.0    # simulated entry slippage


@dataclass
class BacktestResults:
    params:         BacktestParams
    optimistic:     list[TradeOutcome] = field(default_factory=list)
    pessimistic:    list[TradeOutcome] = field(default_factory=list)


def simulate_trade(
    data: ListingCandles,
    params: BacktestParams,
) -> Optional[tuple[TradeOutcome, TradeOutcome]]:
    """
    Simulate one trade on a listing's candle series.
    Returns (optimistic_outcome, pessimistic_outcome) or None if no candles.
    """
    candles = data.candles
    if not candles:
        return None

    # Entry on first candle open + slippage
    c0 = candles[0]
    raw_entry = float(c0[1])  # open
    if raw_entry <= 0:
        return None

    entry = raw_entry * (1 + params.slippage_pct / 100)
    tp    = entry * (1 + params.take_profit_pct / 100)
    sl    = entry * (1 - params.stop_loss_pct   / 100)

    for i, candle in enumerate(candles[: params.max_hold_candles]):
        high  = float(candle[2])
        low   = float(candle[3])
        close = float(candle[4])

        tp_hit = high >= tp
        sl_hit = low  <= sl

        if tp_hit and not sl_hit:
            outcome = TradeOutcome(
                symbol=data.symbol, listed_at=data.listed_at_ms,
                entry_price=entry, exit_price=tp,
                pnl_pct=params.take_profit_pct,
                exit_reason=ExitReason.TAKE_PROFIT, candles_held=i + 1,
            )
            return outcome, outcome  # same for both scenarios

        if sl_hit and not tp_hit:
            outcome = TradeOutcome(
                symbol=data.symbol, listed_at=data.listed_at_ms,
                entry_price=entry, exit_price=sl,
                pnl_pct=-params.stop_loss_pct,
                exit_reason=ExitReason.STOP_LOSS, candles_held=i + 1,
            )
            return outcome, outcome

        if tp_hit and sl_hit:
            # Ambiguous: both triggered within the same candle
            opt = TradeOutcome(
                symbol=data.symbol, listed_at=data.listed_at_ms,
                entry_price=entry, exit_price=tp,
                pnl_pct=params.take_profit_pct,
                exit_reason=ExitReason.TAKE_PROFIT, candles_held=i + 1,
                ambiguous=True,
            )
            pes = TradeOutcome(
                symbol=data.symbol, listed_at=data.listed_at_ms,
                entry_price=entry, exit_price=sl,
                pnl_pct=-params.stop_loss_pct,
                exit_reason=ExitReason.STOP_LOSS, candles_held=i + 1,
                ambiguous=True,
            )
            return opt, pes

    # Timeout: exit at close of last available candle
    last_close = float(candles[min(params.max_hold_candles, len(candles)) - 1][4])
    pnl = (last_close / entry - 1) * 100
    outcome = TradeOutcome(
        symbol=data.symbol, listed_at=data.listed_at_ms,
        entry_price=entry, exit_price=last_close,
        pnl_pct=pnl,
        exit_reason=ExitReason.TIMEOUT, candles_held=params.max_hold_candles,
    )
    return outcome, outcome


def run_backtest(
    dataset: list[ListingCandles],
    params: BacktestParams,
) -> BacktestResults:
    results = BacktestResults(params=params)

    for data in dataset:
        pair = simulate_trade(data, params)
        if pair is None:
            continue
        opt, pes = pair
        results.optimistic.append(opt)
        results.pessimistic.append(pes)

    return results
