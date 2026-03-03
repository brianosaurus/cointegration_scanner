"""
Console display for arbitrage tracker
"""

import sys
import time
from typing import Dict
from transaction_analyzer import ArbitrageTransaction, SwapLeg
from constants import WELL_KNOWN_TOKENS, SOLSCAN_TX_BASE_URL


def token_symbol(mint: str) -> str:
    info = WELL_KNOWN_TOKENS.get(mint)
    if info:
        return info['symbol']
    if mint:
        return mint[:8] + '..'
    return '???'


def format_amount(raw: int, decimals: int) -> str:
    if decimals == 0:
        return str(raw)
    return f"{raw / (10 ** decimals):,.{min(decimals, 6)}f}"


def print_arbitrage(arb: ArbitrageTransaction):
    """Print a detected arbitrage transaction."""
    status = "PROFITABLE" if arb.is_profitable else "UNPROFITABLE"
    tags = []
    if arb.uses_jupiter:
        tags.append("Jupiter")
    if arb.has_jito_tip:
        tags.append("Jito")
    tag_str = f" [{', '.join(tags)}]" if tags else ""

    print(f"\n{'=' * 72}")
    print(f"  ARB DETECTED  |  {status}{tag_str}  |  Slot {arb.slot}")
    print(f"  Signer: {arb.signer}")
    print(f"  {SOLSCAN_TX_BASE_URL}{arb.signature}")
    print(f"  Swap legs ({arb.num_swaps}):")

    for i, leg in enumerate(arb.swap_legs):
        in_sym = token_symbol(leg.token_in_mint)
        out_sym = token_symbol(leg.token_out_mint)
        in_amt = format_amount(leg.amount_in, leg.decimals_in) if leg.amount_in else '?'
        out_amt = format_amount(leg.amount_out, leg.decimals_out) if leg.amount_out else '?'
        pool_short = leg.pool_address[:12] + '..' if leg.pool_address else 'unknown'
        print(f"    {i+1}. [{leg.dex}] {in_sym} ({in_amt}) -> {out_sym} ({out_amt})  pool:{pool_short}")

    if arb.net_profit:
        print(f"  Net P&L:")
        for mint, change in arb.net_profit.items():
            sym = token_symbol(mint)
            sign = '+' if change > 0 else ''
            print(f"    {sym}: {sign}{change:,.9f}")

    print(f"{'=' * 72}")


def print_progress(slot: int, blocks_processed: int, arbs_found: int, start_time: float):
    """Print scan progress on a single line."""
    elapsed = time.time() - start_time
    rate = blocks_processed / elapsed if elapsed > 0 else 0
    sys.stdout.write(
        f"\r  Slot {slot:,} | Blocks: {blocks_processed:,} | Arbs: {arbs_found:,} | {rate:.1f} blk/s"
    )
    sys.stdout.flush()


def print_summary(stats: Dict, elapsed: float):
    """Print session summary statistics."""
    print(f"\n\n{'=' * 72}")
    print(f"  SESSION SUMMARY")
    print(f"{'=' * 72}")
    print(f"  Total arbitrage txs: {stats.get('total_arbs', 0):,}")
    print(f"  Profitable:          {stats.get('profitable', 0):,}")
    print(f"  Using Jupiter:       {stats.get('uses_jupiter', 0):,}")
    print(f"  With Jito tip:       {stats.get('has_jito_tip', 0):,}")
    print(f"  Unique signers:      {stats.get('unique_signers', 0):,}")
    print(f"  Duration:            {elapsed:.1f}s")
    print(f"{'=' * 72}")
