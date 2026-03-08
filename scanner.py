#!/usr/bin/env python3
"""
Cointegration Scanner — statistical pair analysis of Solana token prices

Usage:
    python scanner.py --scan                          # Full scan of all token pairs
    python scanner.py --scan --tokens SOL,USDC,RAY    # Specific tokens only
    python scanner.py --scan --top 20                 # Show top 20 pairs
    python scanner.py --scan --fetch-external         # Supplement with Birdeye/DexScreener
    python scanner.py --zscore                        # Update z-scores for known pairs
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import time

from config import Config
from constants import WELL_KNOWN_TOKENS
from db import Database
from price_builder import PriceBuilder
from cointegration import CointegrationAnalyzer, CointegrationResult
from scanner_display import print_scan_summary, print_zscore_table

logger = logging.getLogger(__name__)

# CSV columns for cointegration export
COINT_CSV_COLUMNS = [
    'token_a_symbol', 'token_b_symbol', 'token_a_mint', 'token_b_mint',
    'eg_p_value', 'eg_is_cointegrated', 'johansen_rank', 'johansen_is_cointegrated',
    'hedge_ratio', 'spread_mean', 'spread_std', 'current_spread', 'current_zscore',
    'half_life', 'correlation', 'num_observations', 'quote_token',
]


def parse_args():
    parser = argparse.ArgumentParser(
        description='Scan for cointegrated token pairs on Solana',
    )
    parser.add_argument('--scan', action='store_true',
                        help='Run cointegration analysis on all discovered pairs')
    parser.add_argument('--zscore', action='store_true',
                        help='Show current z-scores for previously identified pairs')
    parser.add_argument('--db', type=str, default='arb_tracker.db',
                        help='SQLite database path (default: arb_tracker.db)')
    parser.add_argument('--csv', type=str, default='cointegration.csv',
                        help='CSV output file path (default: cointegration.csv)')
    parser.add_argument('--tokens', type=str, default=None,
                        help='Comma-separated token symbols to analyze (default: all)')
    parser.add_argument('--top', type=int, default=50,
                        help='Show top N pairs by significance (default: 50)')
    parser.add_argument('--min-observations', type=int, default=100,
                        help='Minimum overlapping data points per pair (default: 100)')
    parser.add_argument('--p-threshold', type=float, default=0.05,
                        help='P-value threshold for cointegration (default: 0.05)')
    parser.add_argument('--resample', type=str, default='5min',
                        help='Resample interval for price data (default: 5min)')
    parser.add_argument('--lookback', type=int, default=60,
                        help='Lookback periods for z-score window (default: 60)')
    parser.add_argument('--max-pairs', type=int, default=500,
                        help='Max number of pairs to test (default: 500)')
    parser.add_argument('--fetch-external', action='store_true',
                        help='Fetch external price data from Birdeye/DexScreener')
    parser.add_argument('--loop', action='store_true',
                        help='Run continuously in a loop')
    parser.add_argument('--interval', type=int, default=60,
                        help='Seconds between loop iterations (default: 60)')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable debug logging')
    return parser.parse_args()


def resolve_token_filter(token_str: str) -> set:
    """Convert comma-separated symbols to a set of mint addresses."""
    if not token_str:
        return None

    symbols = [s.strip().upper() for s in token_str.split(',')]
    symbol_to_mint = {v['symbol'].upper(): k for k, v in WELL_KNOWN_TOKENS.items()}

    mints = set()
    for sym in symbols:
        if sym in symbol_to_mint:
            mints.add(symbol_to_mint[sym])
        else:
            # Assume it's a mint address
            mints.add(sym)

    return mints


def resample_to_minutes(interval_str: str) -> float:
    """Convert resample string like '5min' or '1h' to minutes."""
    s = interval_str.lower().strip()
    if s.endswith('min'):
        return float(s[:-3])
    elif s.endswith('h'):
        return float(s[:-1]) * 60
    elif s.endswith('d'):
        return float(s[:-1]) * 1440
    elif s.endswith('s'):
        return float(s[:-1]) / 60
    return 5.0  # default


def write_csv(filepath: str, results: list):
    """Write cointegration results to CSV."""
    write_header = not os.path.exists(filepath) or os.path.getsize(filepath) == 0
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COINT_CSV_COLUMNS)
        writer.writeheader()
        for r in results:
            writer.writerow({
                'token_a_symbol': r.token_a_symbol,
                'token_b_symbol': r.token_b_symbol,
                'token_a_mint': r.token_a_mint,
                'token_b_mint': r.token_b_mint,
                'eg_p_value': f'{r.eg_p_value:.6f}',
                'eg_is_cointegrated': int(r.eg_is_cointegrated),
                'johansen_rank': r.johansen_rank,
                'johansen_is_cointegrated': int(r.johansen_is_cointegrated),
                'hedge_ratio': f'{r.hedge_ratio:.6f}',
                'spread_mean': f'{r.spread_mean:.6f}',
                'spread_std': f'{r.spread_std:.6f}',
                'current_spread': f'{r.current_spread:.6f}',
                'current_zscore': f'{r.current_zscore:.4f}',
                'half_life': f'{r.half_life:.2f}' if r.half_life != float('inf') else 'inf',
                'correlation': f'{r.correlation:.6f}',
                'num_observations': r.num_observations,
                'quote_token': r.quote_token,
            })
    logger.info(f"Wrote {len(results)} results to {filepath}")


async def fetch_external_prices(price_builder: PriceBuilder, token_mints: list):
    """Fetch prices from external APIs."""
    print("  Fetching external prices...")

    # Birdeye (if API key configured)
    birdeye_prices = await price_builder.fetch_birdeye_prices(token_mints)
    if birdeye_prices:
        print(f"    Birdeye: {len(birdeye_prices)} prices fetched")

    # DexScreener (free)
    dex_prices = await price_builder.fetch_dexscreener_prices(token_mints)
    if dex_prices:
        print(f"    DexScreener: {len(dex_prices)} prices fetched")

    # Jupiter (free, current prices only)
    jup_prices = await price_builder.fetch_jupiter_prices(token_mints)
    if jup_prices:
        print(f"    Jupiter: {len(jup_prices)} prices fetched")

    total = len(birdeye_prices) + len(dex_prices) + len(jup_prices)
    print(f"    Total external prices: {total}")


async def run_scan(db: Database, config: Config, args):
    """Run full cointegration scan."""
    start_time = time.time()

    token_filter = resolve_token_filter(args.tokens)
    resample_min = resample_to_minutes(args.resample)

    # Save run to DB
    run_config = {
        'tokens': args.tokens,
        'min_observations': args.min_observations,
        'p_threshold': args.p_threshold,
        'resample': args.resample,
        'lookback': args.lookback,
        'fetch_external': args.fetch_external,
    }
    run_id = db.save_scanner_run(json.dumps(run_config))

    # Build price series
    price_builder = PriceBuilder(db, config)

    # Fetch external prices first if requested
    if args.fetch_external:
        if token_filter:
            ext_mints = list(token_filter)
        else:
            ext_mints = list(WELL_KNOWN_TOKENS.keys())
        await fetch_external_prices(price_builder, ext_mints)

    print(f"\n  Building price series (resample: {args.resample})...")
    series = price_builder.build_all_series(
        resample_interval=args.resample,
        token_filter=token_filter,
        max_gap_fill=3,
    )

    if not series:
        print("\n  No price data available. Run tracker.py first to collect swap data,")
        print("  or use --fetch-external to fetch prices from APIs.")
        db.update_scanner_run(run_id, 0, 0, 0)
        return

    print(f"  Found price series for {len(series)} tokens")
    for mint, df in list(series.items())[:10]:
        from price_builder import token_symbol
        sym = token_symbol(mint)
        print(f"    {sym}: {len(df)} data points "
              f"({df.index[0].strftime('%m/%d %H:%M')} - {df.index[-1].strftime('%m/%d %H:%M')})")
    if len(series) > 10:
        print(f"    ... and {len(series) - 10} more")

    # Run cointegration analysis
    print(f"\n  Running cointegration tests (p < {args.p_threshold})...")
    analyzer = CointegrationAnalyzer(
        min_observations=args.min_observations,
        p_threshold=args.p_threshold,
        lookback=args.lookback,
        max_pairs=args.max_pairs,
    )
    results = analyzer.analyze_all_pairs(series, token_filter)

    # Save results to DB
    for r in results:
        db.save_cointegration_result(r)

    cointegrated = sum(1 for r in results if r.eg_is_cointegrated or r.johansen_is_cointegrated)
    removed = db.delete_stale_cointegration_results()
    if removed:
        print(f"  Removed {removed} pairs that are no longer cointegrated")
    db.update_scanner_run(run_id, len(series), len(results), cointegrated)

    # Display results
    elapsed = time.time() - start_time
    print_scan_summary(results, elapsed, top_n=args.top, resample_minutes=resample_min)

    # Write CSV
    write_csv(args.csv, results)
    print(f"  CSV written to {args.csv}")


async def run_zscore(db: Database, config: Config, args):
    """Update z-scores for previously identified cointegrated pairs."""
    # Get latest cointegration results
    rows = db.get_cointegration_results(cointegrated_only=True)
    if not rows:
        print("\n  No cointegrated pairs in database. Run --scan first.")
        return

    # Rebuild price series to recompute z-scores
    price_builder = PriceBuilder(db, config)
    token_filter = resolve_token_filter(args.tokens)

    series = price_builder.build_all_series(
        resample_interval=args.resample,
        token_filter=token_filter,
        max_gap_fill=3,
    )

    if not series:
        print("\n  No price data available for z-score update.")
        return

    # Re-analyze only previously cointegrated pairs
    analyzer = CointegrationAnalyzer(
        min_observations=args.min_observations,
        p_threshold=args.p_threshold,
        lookback=args.lookback,
    )

    results = []
    for row in rows:
        mint_a = row[1]  # token_a_mint
        mint_b = row[2]  # token_b_mint
        if mint_a in series and mint_b in series:
            result = analyzer.analyze_pair(series[mint_a], series[mint_b], mint_a, mint_b)
            if result:
                results.append(result)
                db.save_cointegration_result(result)

    # Sort by z-score magnitude
    results.sort(key=lambda r: abs(r.current_zscore), reverse=True)

    print_zscore_table(results)


def main():
    args = parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )

    if not args.scan and not args.zscore:
        print("Error: specify --scan or --zscore")
        print("Run with --help for usage info")
        sys.exit(1)

    config = Config()
    db = Database(args.db)

    try:
        if args.scan and args.loop:
            iteration = 0
            while True:
                iteration += 1
                print(f"\n{'='*60}")
                print(f"  Loop iteration {iteration}")
                print(f"{'='*60}")
                asyncio.run(run_scan(db, config, args))
                print(f"\n  Sleeping {args.interval}s until next scan...")
                time.sleep(args.interval)
        elif args.scan:
            asyncio.run(run_scan(db, config, args))
        elif args.zscore:
            asyncio.run(run_zscore(db, config, args))
    except KeyboardInterrupt:
        print("\n\n  Scan interrupted.")
    finally:
        db.close()


if __name__ == '__main__':
    main()
