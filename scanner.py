#!/usr/bin/env python3
"""
Cointegration Scanner — statistical basket analysis of Solana token prices via Jupiter

Usage:
    python scanner.py --scan                          # Fetch Jupiter prices + scan baskets
    python scanner.py --scan --basket-size 2          # Pairs only
    python scanner.py --scan --tokens SOL,USDC,RAY    # Specific tokens only
    python scanner.py --scan --top 20                 # Show top 20 baskets
    python scanner.py --scan --loop --interval 30     # Accumulate prices over time
    python scanner.py --scan --no-fetch               # Use cached prices only
    python scanner.py --zscore                        # Update z-scores for known baskets
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
    'symbols', 'mints', 'basket_size',
    'eg_p_value', 'eg_is_cointegrated', 'johansen_rank', 'johansen_is_cointegrated',
    'hedge_ratios', 'spread_mean', 'spread_std', 'current_spread', 'current_zscore',
    'half_life', 'correlation', 'num_observations', 'quote_token',
]


def parse_args():
    parser = argparse.ArgumentParser(
        description='Scan for cointegrated token baskets on Solana',
    )
    parser.add_argument('--scan', action='store_true',
                        help='Run cointegration analysis on token baskets')
    parser.add_argument('--zscore', action='store_true',
                        help='Show current z-scores for previously identified baskets')
    parser.add_argument('--db', type=str, default='arb_tracker.db',
                        help='SQLite database path (default: arb_tracker.db)')
    parser.add_argument('--csv', type=str, default='cointegration.csv',
                        help='CSV output file path (default: cointegration.csv)')
    parser.add_argument('--tokens', type=str, default=None,
                        help='Comma-separated token symbols to analyze (default: all)')
    parser.add_argument('--top', type=int, default=50,
                        help='Show top N baskets by significance (default: 50)')
    parser.add_argument('--min-observations', type=int, default=100,
                        help='Minimum overlapping data points per basket (default: 100)')
    parser.add_argument('--p-threshold', type=float, default=0.05,
                        help='P-value threshold for cointegration (default: 0.05)')
    parser.add_argument('--lookback', type=int, default=60,
                        help='Lookback periods for z-score window (default: 60)')
    parser.add_argument('--max-baskets', type=int, default=500,
                        help='Max number of baskets to test (default: 500)')
    parser.add_argument('--basket-size', type=int, default=4, choices=[2, 3, 4],
                        help='Number of tokens per basket (default: 4)')
    parser.add_argument('--no-fetch', action='store_true',
                        help='Skip fetching new Jupiter prices (use cached only)')
    parser.add_argument('--loop', action='store_true',
                        help='Run continuously in a loop')
    parser.add_argument('--interval', type=int, default=30,
                        help='Seconds between loop iterations (default: 30)')
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
            mints.add(sym)

    return mints


def write_csv(filepath: str, results: list):
    """Write cointegration results to CSV."""
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COINT_CSV_COLUMNS)
        writer.writeheader()
        for r in results:
            writer.writerow({
                'symbols': '/'.join(r.symbols),
                'mints': ','.join(r.mints),
                'basket_size': r.basket_size,
                'eg_p_value': f'{r.eg_p_value:.6f}' if r.eg_p_value is not None else '',
                'eg_is_cointegrated': int(r.eg_is_cointegrated),
                'johansen_rank': r.johansen_rank,
                'johansen_is_cointegrated': int(r.johansen_is_cointegrated),
                'hedge_ratios': json.dumps([round(w, 6) for w in r.hedge_ratios]),
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


async def fetch_jupiter_prices(price_builder: PriceBuilder, token_mints: list):
    """Fetch current prices from Jupiter API."""
    print("  Fetching Jupiter prices...")
    jup_prices = await price_builder.fetch_jupiter_prices(token_mints)
    print(f"    Jupiter: {len(jup_prices)} prices fetched")
    return jup_prices


async def run_scan(db: Database, config: Config, args):
    """Run full cointegration scan."""
    start_time = time.time()

    token_filter = resolve_token_filter(args.tokens)

    run_config = {
        'tokens': args.tokens,
        'min_observations': args.min_observations,
        'p_threshold': args.p_threshold,
        'interval': args.interval,
        'lookback': args.lookback,
        'basket_size': args.basket_size,
    }
    run_id = db.save_scanner_run(json.dumps(run_config))

    price_builder = PriceBuilder(db, config)

    if not args.no_fetch:
        if token_filter:
            mints = list(token_filter)
        else:
            mints = list(WELL_KNOWN_TOKENS.keys())
        await fetch_jupiter_prices(price_builder, mints)

    print(f"\n  Building price series...")
    series = price_builder.build_all_series(token_filter=token_filter)

    if not series:
        print("\n  No price data available. Run with --loop to accumulate Jupiter prices over time.")
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
    print(f"\n  Running cointegration tests (basket size {args.basket_size}, p < {args.p_threshold})...")
    analyzer = CointegrationAnalyzer(
        min_observations=args.min_observations,
        p_threshold=args.p_threshold,
        lookback=args.lookback,
        max_pairs=args.max_baskets,
        basket_size=args.basket_size,
    )
    results = analyzer.analyze_all_baskets(series, token_filter)

    # Save results to DB
    for r in results:
        db.save_cointegration_result(r)

    cointegrated = sum(1 for r in results if r.johansen_is_cointegrated)
    removed = db.delete_stale_cointegration_results()
    if removed:
        print(f"  Removed {removed} baskets that are no longer cointegrated")
    db.update_scanner_run(run_id, len(series), len(results), cointegrated)

    # Display results
    elapsed = time.time() - start_time
    obs_minutes = args.interval / 60.0
    print_scan_summary(results, elapsed, top_n=args.top, resample_minutes=obs_minutes)

    # Write CSV
    write_csv(args.csv, results)
    print(f"  CSV written to {args.csv}")


async def run_zscore(db: Database, config: Config, args):
    """Update z-scores for previously identified cointegrated baskets."""
    rows = db.get_cointegration_results(cointegrated_only=True)
    if not rows:
        print("\n  No cointegrated baskets in database. Run --scan first.")
        return

    price_builder = PriceBuilder(db, config)
    token_filter = resolve_token_filter(args.tokens)

    if not args.no_fetch:
        # Fetch prices for all tokens in cointegrated baskets
        all_mints = set()
        for row in rows:
            all_mints.update(row['mints'])
        await fetch_jupiter_prices(price_builder, list(all_mints))

    series = price_builder.build_all_series(token_filter=token_filter)

    if not series:
        print("\n  No price data available for z-score update.")
        return

    analyzer = CointegrationAnalyzer(
        min_observations=args.min_observations,
        p_threshold=args.p_threshold,
        lookback=args.lookback,
        basket_size=args.basket_size,
    )

    results = []
    for row in rows:
        basket_mints = row['mints']
        if all(m in series for m in basket_mints):
            series_list = [series[m] for m in basket_mints]
            result = analyzer.analyze_basket(series_list, basket_mints)
            if result:
                results.append(result)
                db.save_cointegration_result(result)

    results.sort(key=lambda r: abs(r.current_zscore), reverse=True)
    print_zscore_table(results)


def main():
    args = parse_args()

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
