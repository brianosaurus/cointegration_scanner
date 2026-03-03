"""
CSV writer for arbitrage transaction logging.
Appends one row per detected arbitrage to a CSV file.
"""

import csv
import os

from constants import WELL_KNOWN_TOKENS


CSV_COLUMNS = [
    'signature', 'slot', 'tx_index', 'signer',
    'pool_a', 'pool_b', 'dex_a', 'dex_b',
    'jito_tip_sol', 'amount', 'token', 'is_backrun',
]


def _token_symbol(mint: str) -> str:
    """Resolve mint address to human-readable symbol."""
    info = WELL_KNOWN_TOKENS.get(mint)
    if info:
        return info['symbol']
    # Shortened mint as fallback
    return mint[:8] + '...' if mint else 'unknown'


class CsvWriter:
    def __init__(self, filepath: str):
        self.filepath = filepath
        write_header = not os.path.exists(filepath) or os.path.getsize(filepath) == 0
        self._file = open(filepath, 'a', newline='')
        self._writer = csv.DictWriter(self._file, fieldnames=CSV_COLUMNS)
        if write_header:
            self._writer.writeheader()
            self._file.flush()

    def write_arb(self, arb):
        """Write a single ArbitrageTransaction as a CSV row."""
        legs = arb.swap_legs
        if not legs:
            return

        first_leg = legs[0]
        last_leg = legs[-1]

        # Human-readable amount from first leg
        if first_leg.amount_in and first_leg.decimals_in:
            amount = first_leg.amount_in / (10 ** first_leg.decimals_in)
        else:
            amount = first_leg.amount_in

        self._writer.writerow({
            'signature': arb.signature,
            'slot': arb.slot,
            'tx_index': arb.tx_index,
            'signer': arb.signer,
            'pool_a': first_leg.pool_address or '',
            'pool_b': last_leg.pool_address or '',
            'dex_a': first_leg.dex,
            'dex_b': last_leg.dex,
            'jito_tip_sol': f'{arb.jito_tip_amount:.9f}',
            'amount': f'{amount:.6f}',
            'token': _token_symbol(first_leg.token_in_mint),
            'is_backrun': arb.is_backrun,
        })
        self._file.flush()

    def close(self):
        self._file.close()
