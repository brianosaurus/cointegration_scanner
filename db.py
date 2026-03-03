"""
SQLite storage for arbitrage tracker
Tables: arbitrage_transactions, swap_legs, scan_progress
"""

import json
import sqlite3
import logging
from typing import Optional
from transaction_analyzer import ArbitrageTransaction

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = 'arb_tracker.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS arbitrage_transactions (
                signature TEXT PRIMARY KEY,
                slot INTEGER NOT NULL,
                block_time INTEGER NOT NULL DEFAULT 0,
                signer TEXT NOT NULL,
                num_swaps INTEGER NOT NULL,
                is_profitable INTEGER NOT NULL DEFAULT 0,
                net_profit_json TEXT NOT NULL DEFAULT '{}',
                uses_jupiter INTEGER NOT NULL DEFAULT 0,
                has_jito_tip INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS swap_legs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signature TEXT NOT NULL,
                leg_index INTEGER NOT NULL,
                dex TEXT NOT NULL,
                pool_address TEXT,
                token_in_mint TEXT,
                token_out_mint TEXT,
                amount_in INTEGER NOT NULL DEFAULT 0,
                amount_out INTEGER NOT NULL DEFAULT 0,
                decimals_in INTEGER NOT NULL DEFAULT 0,
                decimals_out INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (signature) REFERENCES arbitrage_transactions(signature)
            );

            CREATE TABLE IF NOT EXISTS scan_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_slot INTEGER NOT NULL,
                end_slot INTEGER NOT NULL,
                last_processed_slot INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_arb_slot ON arbitrage_transactions(slot);
            CREATE INDEX IF NOT EXISTS idx_arb_signer ON arbitrage_transactions(signer);
            CREATE INDEX IF NOT EXISTS idx_legs_sig ON swap_legs(signature);
        """)
        self.conn.commit()

    def save_arbitrage(self, arb: ArbitrageTransaction):
        """Save an arbitrage transaction and its swap legs."""
        try:
            self.conn.execute(
                """INSERT OR IGNORE INTO arbitrage_transactions
                   (signature, slot, block_time, signer, num_swaps, is_profitable,
                    net_profit_json, uses_jupiter, has_jito_tip)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (arb.signature, arb.slot, arb.block_time, arb.signer, arb.num_swaps,
                 int(arb.is_profitable), json.dumps(arb.net_profit),
                 int(arb.uses_jupiter), int(arb.has_jito_tip)),
            )

            for i, leg in enumerate(arb.swap_legs):
                self.conn.execute(
                    """INSERT INTO swap_legs
                       (signature, leg_index, dex, pool_address, token_in_mint,
                        token_out_mint, amount_in, amount_out, decimals_in, decimals_out)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (arb.signature, i, leg.dex, leg.pool_address,
                     leg.token_in_mint, leg.token_out_mint,
                     leg.amount_in, leg.amount_out,
                     leg.decimals_in, leg.decimals_out),
                )

            self.conn.commit()
        except sqlite3.IntegrityError:
            pass  # Duplicate signature

    def update_scan_progress(self, start_slot: int, end_slot: int, last_processed: int):
        """Update or insert scan progress."""
        self.conn.execute(
            """INSERT INTO scan_progress (start_slot, end_slot, last_processed_slot)
               VALUES (?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                   last_processed_slot = excluded.last_processed_slot,
                   updated_at = CURRENT_TIMESTAMP""",
            (start_slot, end_slot, last_processed),
        )
        self.conn.commit()

    def get_scan_progress(self, start_slot: int, end_slot: int) -> Optional[int]:
        """Get last processed slot for a given scan range."""
        row = self.conn.execute(
            "SELECT last_processed_slot FROM scan_progress WHERE start_slot = ? AND end_slot = ?",
            (start_slot, end_slot),
        ).fetchone()
        return row[0] if row else None

    def get_stats(self) -> dict:
        """Get summary statistics."""
        total = self.conn.execute("SELECT COUNT(*) FROM arbitrage_transactions").fetchone()[0]
        profitable = self.conn.execute(
            "SELECT COUNT(*) FROM arbitrage_transactions WHERE is_profitable = 1"
        ).fetchone()[0]
        jupiter = self.conn.execute(
            "SELECT COUNT(*) FROM arbitrage_transactions WHERE uses_jupiter = 1"
        ).fetchone()[0]
        jito = self.conn.execute(
            "SELECT COUNT(*) FROM arbitrage_transactions WHERE has_jito_tip = 1"
        ).fetchone()[0]
        unique_signers = self.conn.execute(
            "SELECT COUNT(DISTINCT signer) FROM arbitrage_transactions"
        ).fetchone()[0]
        return {
            'total_arbs': total,
            'profitable': profitable,
            'uses_jupiter': jupiter,
            'has_jito_tip': jito,
            'unique_signers': unique_signers,
        }

    def close(self):
        self.conn.close()
