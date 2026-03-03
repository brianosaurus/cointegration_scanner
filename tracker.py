#!/usr/bin/env python3
"""
Arbitrage Tracker — read-only post-hoc analysis of Solana arbitrage transactions

Usage:
    python tracker.py --follow                     # Follow new confirmed blocks
    python tracker.py --slot-range 300000000-300001000  # Scan a slot range
    python tracker.py --follow --signer <ADDR>     # Filter by wallet
"""

import argparse
import asyncio
import logging
import signal
import sys
import time

from config import Config
from block_fetcher import BlockFetcher
from transaction_analyzer import TransactionAnalyzer
from swap_detector import SwapDetector
from db import Database
from csv_writer import CsvWriter
from display import print_arbitrage, print_progress, print_summary

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Track arbitrage transactions on Solana',
    )
    parser.add_argument('--slot-range', type=str, default=None,
                        help='Scan slot range: START-END (e.g. 300000000-300001000)')
    parser.add_argument('--follow', action='store_true',
                        help='Follow new confirmed blocks in real-time')
    parser.add_argument('--db', type=str, default='arb_tracker.db',
                        help='SQLite database path (default: arb_tracker.db)')
    parser.add_argument('--min-swaps', type=int, default=2,
                        help='Minimum swaps to qualify as arbitrage (default: 2)')
    parser.add_argument('--signer', type=str, default=None,
                        help='Filter by signer wallet address')
    parser.add_argument('--csv', type=str, default='arb_tracker.csv',
                        help='CSV output file path (default: arb_tracker.csv)')
    parser.add_argument('--duration', type=float, default=None,
                        help='Run for this many minutes then stop')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable debug logging')
    return parser.parse_args()


def _extract_tx_pools(tx, swap_detector: SwapDetector) -> set:
    """Extract pool addresses touched by a transaction (lightweight, for backrun detection)."""
    try:
        swaps = swap_detector.analyze_transaction(tx)
        return {s.get('pool_address') for s in swaps if s.get('pool_address')}
    except Exception:
        return set()


async def process_block(block, slot, analyzer, db, csv_writer, signer_filter, stats):
    """Process all transactions in a block."""
    if not hasattr(block, 'transactions'):
        return

    block_time = block.block_time.timestamp if hasattr(block, 'block_time') and block.block_time else 0

    prev_tx_pools = set()

    for tx_index, tx in enumerate(block.transactions):
        try:
            arb = analyzer.analyze(tx, slot, block_time, tx_index=tx_index)
            if arb is None:
                # Track pools for backrun detection even on non-arb txs
                prev_tx_pools = _extract_tx_pools(tx, analyzer.detector)
                continue

            if signer_filter and arb.signer != signer_filter:
                prev_tx_pools = set()
                continue

            # Backrun detection: did the immediately preceding tx touch the same pool?
            arb_pools = {leg.pool_address for leg in arb.swap_legs if leg.pool_address}
            if prev_tx_pools and arb_pools & prev_tx_pools:
                arb.is_backrun = True

            db.save_arbitrage(arb)
            csv_writer.write_arb(arb)
            print_arbitrage(arb)
            stats['arbs_found'] += 1

            prev_tx_pools = arb_pools

        except Exception as e:
            prev_tx_pools = set()
            if stats.get('verbose'):
                logger.debug(f"Error analyzing tx in slot {slot}: {e}")


async def run_follow(config: Config, args):
    """Follow confirmed blocks in real-time."""
    fetcher = BlockFetcher(config.grpc_endpoint, config.grpc_token)
    analyzer = TransactionAnalyzer(min_swaps=args.min_swaps)
    db = Database(args.db)
    csv_writer = CsvWriter(args.csv)

    stats = {'blocks': 0, 'arbs_found': 0, 'verbose': args.verbose}
    start_time = time.time()
    deadline = start_time + args.duration * 60 if args.duration else None

    print(f"Following confirmed blocks... (Ctrl+C to stop)")
    print(f"gRPC: {config.grpc_endpoint}")
    print(f"DB: {args.db}")
    print(f"CSV: {args.csv}")
    print(f"Min swaps: {args.min_swaps}")
    if args.duration:
        print(f"Duration: {args.duration} minutes")
    if args.signer:
        print(f"Signer filter: {args.signer}")
    print()

    try:
        async for slot, block in fetcher.follow_confirmed():
            await process_block(block, slot, analyzer, db, csv_writer, args.signer, stats)
            stats['blocks'] += 1
            if stats['blocks'] % 10 == 0:
                print_progress(slot, stats['blocks'], stats['arbs_found'], start_time)
            if deadline and time.time() >= deadline:
                print(f"\nDuration limit reached ({args.duration} minutes).")
                break
    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.time() - start_time
        db_stats = db.get_stats()
        print_summary(db_stats, elapsed)
        csv_writer.close()
        db.close()


async def run_slot_range(config: Config, args, start_slot: int, end_slot: int):
    """Scan a range of confirmed slots."""
    fetcher = BlockFetcher(config.grpc_endpoint, config.grpc_token)
    analyzer = TransactionAnalyzer(min_swaps=args.min_swaps)
    db = Database(args.db)
    csv_writer = CsvWriter(args.csv)

    # Check for resumable scan
    last_processed = db.get_scan_progress(start_slot, end_slot)
    if last_processed and last_processed > start_slot:
        print(f"Resuming from slot {last_processed} (previously scanned to here)")
        start_slot = last_processed + 1

    stats = {'blocks': 0, 'arbs_found': 0, 'verbose': args.verbose}
    start_time = time.time()
    deadline = start_time + args.duration * 60 if args.duration else None

    print(f"Scanning slots {start_slot:,} to {end_slot:,}...")
    print(f"gRPC: {config.grpc_endpoint}")
    print(f"DB: {args.db}")
    print(f"CSV: {args.csv}")
    if args.duration:
        print(f"Duration: {args.duration} minutes")
    print()

    try:
        async for slot, block in fetcher.fetch_slot_range(start_slot, end_slot):
            await process_block(block, slot, analyzer, db, csv_writer, args.signer, stats)
            stats['blocks'] += 1

            if stats['blocks'] % 100 == 0:
                print_progress(slot, stats['blocks'], stats['arbs_found'], start_time)
                db.update_scan_progress(start_slot, end_slot, slot)

            if deadline and time.time() >= deadline:
                print(f"\nDuration limit reached ({args.duration} minutes).")
                db.update_scan_progress(start_slot, end_slot, slot)
                break
        else:
            # Completed full range without breaking
            db.update_scan_progress(start_slot, end_slot, end_slot)

    except KeyboardInterrupt:
        pass
    finally:
        elapsed = time.time() - start_time
        db_stats = db.get_stats()
        print_summary(db_stats, elapsed)
        csv_writer.close()
        db.close()


def main():
    args = parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )

    config = Config()

    if args.slot_range:
        try:
            parts = args.slot_range.split('-')
            start_slot = int(parts[0])
            end_slot = int(parts[1])
        except (ValueError, IndexError):
            print(f"Error: --slot-range must be START-END (e.g. 300000000-300001000)")
            sys.exit(1)

        if start_slot >= end_slot:
            print(f"Error: start slot must be less than end slot")
            sys.exit(1)

        asyncio.run(run_slot_range(config, args, start_slot, end_slot))

    elif args.follow:
        asyncio.run(run_follow(config, args))

    else:
        print("Error: specify --follow or --slot-range START-END")
        print("Run with --help for usage info")
        sys.exit(1)


if __name__ == '__main__':
    main()
