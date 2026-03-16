"""
Console display for cointegration scanner results.
Supports N-token baskets (2-4 tokens per basket).
"""

import time
from typing import List
from cointegration import CointegrationResult


def format_half_life(hl: float, resample_minutes: float = 5.0) -> str:
    """Format half-life in human-readable time units."""
    if hl == float('inf') or hl < 0:
        return 'N/A'
    total_minutes = hl * resample_minutes
    if total_minutes < 60:
        return f"{total_minutes:.0f}m"
    hours = total_minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    days = hours / 24
    return f"{days:.1f}d"


def print_scan_header(num_tokens: int, num_baskets: int, num_cointegrated: int):
    """Print scan results header."""
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'=' * 90}")
    print(f"  COINTEGRATION SCAN  |  {ts}")
    print(f"  Tokens: {num_tokens}  |  Baskets tested: {num_baskets}  |  Cointegrated: {num_cointegrated}")
    print(f"{'=' * 90}")


def print_rankings(results: List[CointegrationResult], top_n: int = 50,
                   resample_minutes: float = 5.0):
    """Print ranked table of cointegration results."""
    if not results:
        print("\n  No results to display.")
        return

    shown = results[:top_n]

    print(f"\n  {'#':>3}  {'Basket':<28} {'Johansen':>10} {'Rank':>5} "
          f"{'Z-Score':>8} {'Half-Life':>10} {'Corr':>6} {'N':>6}")
    print(f"  {'--':>3}  {'------':<28} {'--------':>10} {'----':>5} "
          f"{'-------':>8} {'---------':>10} {'----':>6} {'-':>6}")

    for i, r in enumerate(shown, 1):
        label = r.pair_label

        if r.johansen_is_cointegrated:
            joh = f"Yes (r={r.johansen_rank})"
        else:
            joh = "No"

        hl = format_half_life(r.half_life, resample_minutes)
        z_str = f"{r.current_zscore:+.2f}"

        print(f"  {i:>3}  {label:<28} {joh:>10} {r.johansen_rank:>5} "
              f"{z_str:>8} {hl:>10} {r.correlation:>6.3f} {r.num_observations:>6}")


def print_signals(results: List[CointegrationResult], zscore_threshold: float = 2.0):
    """Print trading signal alerts for baskets with extreme z-scores."""
    signals = [r for r in results
               if r.johansen_is_cointegrated
               and abs(r.current_zscore) >= zscore_threshold]

    if not signals:
        print(f"\n  No signals (|z-score| >= {zscore_threshold})")
        return

    print(f"\n{'=' * 90}")
    print(f"  SIGNALS (|z| >= {zscore_threshold})")
    print(f"{'=' * 90}")

    for r in signals:
        label = r.pair_label
        direction = "LONG spread" if r.current_zscore < 0 else "SHORT spread"

        # Show weighted positions
        positions = []
        for sym, w in zip(r.symbols, r.hedge_ratios):
            # When z < 0 (spread below mean), go long the spread (use weights as-is)
            # When z > 0 (spread above mean), short the spread (flip weights)
            sign = w if r.current_zscore < 0 else -w
            action = "BUY" if sign > 0 else "SELL"
            positions.append(f"{action} {sym}")

        action_str = ", ".join(positions)
        print(f"   {label:<28} z = {r.current_zscore:+.2f}  {direction}  ({action_str})")

    print(f"{'=' * 90}")


def print_scan_summary(results: List[CointegrationResult], elapsed: float,
                       top_n: int = 50, resample_minutes: float = 5.0):
    """Print full scan output: header + rankings + signals + footer."""
    cointegrated = [r for r in results if r.johansen_is_cointegrated]

    # Count unique tokens across all baskets
    tokens = set()
    for r in results:
        tokens.update(r.mints)

    print_scan_header(len(tokens), len(results), len(cointegrated))
    print_rankings(results, top_n, resample_minutes)
    print_signals(results)

    print(f"\n  Scan completed in {elapsed:.1f}s")
    print(f"{'=' * 90}")


def print_zscore_table(results: List[CointegrationResult]):
    """Print z-score update table for previously identified baskets."""
    if not results:
        print("\n  No cointegrated baskets found in database.")
        return

    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'=' * 90}")
    print(f"  Z-SCORE UPDATE  |  {ts}")
    print(f"{'=' * 90}")

    print(f"\n  {'Basket':<28} {'Z-Score':>8} {'Spread':>10} {'Mean':>10} {'Std':>10} {'Signal':>14}")
    print(f"  {'------':<28} {'-------':>8} {'------':>10} {'----':>10} {'---':>10} {'------':>14}")

    for r in results:
        label = r.pair_label
        z_str = f"{r.current_zscore:+.2f}"

        if abs(r.current_zscore) >= 2.0:
            signal = "LONG" if r.current_zscore < 0 else "SHORT"
        elif abs(r.current_zscore) >= 1.5:
            signal = "Approaching"
        else:
            signal = "Neutral"

        print(f"  {label:<28} {z_str:>8} {r.current_spread:>10.4f} "
              f"{r.spread_mean:>10.4f} {r.spread_std:>10.4f} {signal:>14}")

    print(f"\n{'=' * 90}")
