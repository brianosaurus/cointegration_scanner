"""
Console display for cointegration scanner results.
Mirrors display.py formatting conventions.
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


def print_scan_header(num_tokens: int, num_pairs: int, num_cointegrated: int):
    """Print scan results header."""
    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'=' * 80}")
    print(f"  COINTEGRATION SCAN  |  {ts}")
    print(f"  Tokens: {num_tokens}  |  Pairs tested: {num_pairs}  |  Cointegrated: {num_cointegrated}")
    print(f"{'=' * 80}")


def print_rankings(results: List[CointegrationResult], top_n: int = 50,
                   resample_minutes: float = 5.0):
    """Print ranked table of cointegration results."""
    if not results:
        print("\n  No results to display.")
        return

    shown = results[:top_n]

    print(f"\n  {'#':>3}  {'Pair':<16} {'EG p-val':>9} {'Johansen':>10} {'Hedge':>7} "
          f"{'Z-Score':>8} {'Half-Life':>10} {'Corr':>6} {'N':>6}")
    print(f"  {'--':>3}  {'----':<16} {'--------':>9} {'--------':>10} {'-----':>7} "
          f"{'-------':>8} {'---------':>10} {'----':>6} {'-':>6}")

    for i, r in enumerate(shown, 1):
        pair = f"{r.token_a_symbol}/{r.token_b_symbol}"

        # Johansen summary
        if r.johansen_is_cointegrated:
            joh = f"Yes (r={r.johansen_rank})"
        else:
            joh = "No"

        hl = format_half_life(r.half_life, resample_minutes)

        # Color-code z-score direction in the display
        z_str = f"{r.current_zscore:+.2f}"

        print(f"  {i:>3}  {pair:<16} {r.eg_p_value:>9.4f} {joh:>10} {r.hedge_ratio:>7.3f} "
              f"{z_str:>8} {hl:>10} {r.correlation:>6.3f} {r.num_observations:>6}")


def print_signals(results: List[CointegrationResult], zscore_threshold: float = 2.0):
    """Print trading signal alerts for pairs with extreme z-scores."""
    signals = [r for r in results
               if (r.eg_is_cointegrated or r.johansen_is_cointegrated)
               and abs(r.current_zscore) >= zscore_threshold]

    if not signals:
        print(f"\n  No signals (|z-score| >= {zscore_threshold})")
        return

    print(f"\n{'=' * 80}")
    print(f"  SIGNALS (|z| >= {zscore_threshold})")
    print(f"{'=' * 80}")

    for r in signals:
        pair = f"{r.token_a_symbol}/{r.token_b_symbol}"
        if r.current_zscore < 0:
            direction = "LONG spread"
            action = f"buy {r.token_a_symbol}, sell {r.token_b_symbol}"
        else:
            direction = "SHORT spread"
            action = f"sell {r.token_a_symbol}, buy {r.token_b_symbol}"
        print(f"   {pair:<16} z = {r.current_zscore:+.2f}  {direction}  ({action})")

    print(f"{'=' * 80}")


def print_scan_summary(results: List[CointegrationResult], elapsed: float,
                       top_n: int = 50, resample_minutes: float = 5.0):
    """Print full scan output: header + rankings + signals + footer."""
    cointegrated = [r for r in results if r.eg_is_cointegrated or r.johansen_is_cointegrated]

    # Count unique tokens
    tokens = set()
    for r in results:
        tokens.add(r.token_a_mint)
        tokens.add(r.token_b_mint)

    print_scan_header(len(tokens), len(results), len(cointegrated))
    print_rankings(results, top_n, resample_minutes)
    print_signals(results)

    print(f"\n  Scan completed in {elapsed:.1f}s")
    print(f"{'=' * 80}")


def print_zscore_table(results: List[CointegrationResult]):
    """Print z-score update table for previously identified pairs."""
    if not results:
        print("\n  No cointegrated pairs found in database.")
        return

    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'=' * 80}")
    print(f"  Z-SCORE UPDATE  |  {ts}")
    print(f"{'=' * 80}")

    print(f"\n  {'Pair':<16} {'Z-Score':>8} {'Spread':>10} {'Mean':>10} {'Std':>10} {'Signal':>14}")
    print(f"  {'----':<16} {'-------':>8} {'------':>10} {'----':>10} {'---':>10} {'------':>14}")

    for r in results:
        pair = f"{r.token_a_symbol}/{r.token_b_symbol}"
        z_str = f"{r.current_zscore:+.2f}"

        if abs(r.current_zscore) >= 2.0:
            signal = "LONG" if r.current_zscore < 0 else "SHORT"
        elif abs(r.current_zscore) >= 1.5:
            signal = "Approaching"
        else:
            signal = "Neutral"

        print(f"  {pair:<16} {z_str:>8} {r.current_spread:>10.4f} "
              f"{r.spread_mean:>10.4f} {r.spread_std:>10.4f} {signal:>14}")

    print(f"\n{'=' * 80}")
