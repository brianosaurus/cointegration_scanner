"""
SQLite storage for arbitrage tracker and cointegration scanner
Tables: arbitrage_transactions, swap_legs, scan_progress,
        price_cache, cointegration_results, scanner_runs
"""

import json
import sqlite3
import logging
import time
from typing import Optional, List
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

            CREATE TABLE IF NOT EXISTS price_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_mint TEXT NOT NULL,
                quote_mint TEXT NOT NULL,
                price REAL NOT NULL,
                timestamp INTEGER NOT NULL,
                source TEXT NOT NULL,
                slot INTEGER,
                dex TEXT,
                pool_address TEXT,
                UNIQUE(token_mint, quote_mint, timestamp, source)
            );

            CREATE TABLE IF NOT EXISTS cointegration_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_a_mint TEXT NOT NULL,
                token_b_mint TEXT NOT NULL,
                token_a_symbol TEXT NOT NULL,
                token_b_symbol TEXT NOT NULL,
                eg_test_statistic REAL,
                eg_p_value REAL,
                eg_is_cointegrated INTEGER NOT NULL DEFAULT 0,
                johansen_trace_stat REAL,
                johansen_eigen_stat REAL,
                johansen_rank INTEGER,
                johansen_is_cointegrated INTEGER NOT NULL DEFAULT 0,
                hedge_ratio REAL,
                spread_mean REAL,
                spread_std REAL,
                current_spread REAL,
                current_zscore REAL,
                half_life REAL,
                correlation REAL,
                num_observations INTEGER,
                start_time INTEGER,
                end_time INTEGER,
                quote_token TEXT,
                analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS scanner_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                num_tokens INTEGER,
                num_pairs_analyzed INTEGER,
                num_cointegrated INTEGER,
                config_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_price_cache_token ON price_cache(token_mint, timestamp);
            CREATE INDEX IF NOT EXISTS idx_coint_pair ON cointegration_results(token_a_mint, token_b_mint);
            CREATE INDEX IF NOT EXISTS idx_coint_zscore ON cointegration_results(current_zscore);
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

    # --- Price cache methods ---

    def save_price_cache(self, prices: list):
        """Save price points to cache. Each item is a dict or PricePoint-like object."""
        for p in prices:
            try:
                self.conn.execute(
                    """INSERT OR IGNORE INTO price_cache
                       (token_mint, quote_mint, price, timestamp, source, slot, dex, pool_address)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (p['token_mint'], p['quote_mint'], p['price'], p['timestamp'],
                     p['source'], p.get('slot'), p.get('dex'), p.get('pool_address')),
                )
            except (sqlite3.IntegrityError, KeyError):
                pass
        self.conn.commit()

    def get_cached_prices(self, token_mint: str, quote_mint: str = None,
                          start_time: int = 0, end_time: int = None,
                          source: str = None) -> list:
        """Get cached prices for a token, optionally filtered by quote, time range, and source."""
        query = "SELECT token_mint, quote_mint, price, timestamp, source FROM price_cache WHERE token_mint = ?"
        params: list = [token_mint]
        if quote_mint:
            query += " AND quote_mint = ?"
            params.append(quote_mint)
        if source:
            query += " AND source = ?"
            params.append(source)
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)
        query += " ORDER BY timestamp ASC"
        return self.conn.execute(query, params).fetchall()

    def get_swap_prices(self) -> list:
        """Extract implied prices from swap_legs joined with arbitrage_transactions."""
        return self.conn.execute("""
            SELECT
                sl.token_in_mint, sl.token_out_mint,
                sl.amount_in, sl.amount_out,
                sl.decimals_in, sl.decimals_out,
                sl.dex, sl.pool_address,
                at.block_time, at.slot
            FROM swap_legs sl
            JOIN arbitrage_transactions at ON sl.signature = at.signature
            WHERE at.block_time > 0
              AND sl.amount_in > 0 AND sl.amount_out > 0
              AND sl.decimals_in >= 0 AND sl.decimals_out >= 0
            ORDER BY at.block_time ASC
        """).fetchall()

    def get_distinct_tokens(self) -> list:
        """Get all distinct token mints observed in swap_legs."""
        rows = self.conn.execute("""
            SELECT DISTINCT mint FROM (
                SELECT token_in_mint AS mint FROM swap_legs WHERE token_in_mint IS NOT NULL
                UNION
                SELECT token_out_mint AS mint FROM swap_legs WHERE token_out_mint IS NOT NULL
            )
        """).fetchall()
        return [r[0] for r in rows]

    def get_cached_tokens(self) -> list:
        """Get all distinct token mints in price_cache."""
        rows = self.conn.execute(
            "SELECT DISTINCT token_mint FROM price_cache"
        ).fetchall()
        return [r[0] for r in rows]

    # --- Cointegration results methods ---

    def save_cointegration_result(self, result) -> None:
        """Upsert a CointegrationResult — one row per (token_a_mint, token_b_mint) pair."""
        # Delete any existing rows for this pair, then insert fresh
        self.conn.execute(
            "DELETE FROM cointegration_results WHERE token_a_mint = ? AND token_b_mint = ?",
            (result.token_a_mint, result.token_b_mint),
        )
        self.conn.execute(
            """INSERT INTO cointegration_results
               (token_a_mint, token_b_mint, token_a_symbol, token_b_symbol,
                eg_test_statistic, eg_p_value, eg_is_cointegrated,
                johansen_trace_stat, johansen_eigen_stat, johansen_rank, johansen_is_cointegrated,
                hedge_ratio, spread_mean, spread_std, current_spread, current_zscore,
                half_life, correlation, num_observations, start_time, end_time, quote_token, analyzed_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (result.token_a_mint, result.token_b_mint,
             result.token_a_symbol, result.token_b_symbol,
             result.eg_test_statistic, result.eg_p_value, int(result.eg_is_cointegrated),
             result.johansen_trace_stat, result.johansen_eigen_stat,
             result.johansen_rank, int(result.johansen_is_cointegrated),
             result.hedge_ratio, result.spread_mean, result.spread_std,
             result.current_spread, result.current_zscore,
             result.half_life, result.correlation,
             result.num_observations, result.start_time, result.end_time,
             result.quote_token, result.analyzed_at),
        )
        self.conn.commit()

    def delete_stale_cointegration_results(self) -> int:
        """Remove pairs that are no longer cointegrated. Returns count deleted."""
        cursor = self.conn.execute(
            """DELETE FROM cointegration_results
               WHERE eg_is_cointegrated = 0 AND johansen_is_cointegrated = 0""",
        )
        self.conn.commit()
        return cursor.rowcount

    def get_cointegration_results(self, cointegrated_only: bool = False,
                                  latest_only: bool = True) -> list:
        """Get cointegration results, optionally filtered."""
        if latest_only:
            query = """
                SELECT * FROM cointegration_results
                WHERE id IN (
                    SELECT MAX(id) FROM cointegration_results
                    GROUP BY token_a_mint, token_b_mint
                )
            """
        else:
            query = "SELECT * FROM cointegration_results"

        if cointegrated_only:
            if "WHERE" in query:
                query += " AND (eg_is_cointegrated = 1 OR johansen_is_cointegrated = 1)"
            else:
                query += " WHERE (eg_is_cointegrated = 1 OR johansen_is_cointegrated = 1)"

        query += " ORDER BY eg_p_value ASC"
        return self.conn.execute(query).fetchall()

    # --- Scanner run methods ---

    def save_scanner_run(self, config_json: str = '{}') -> int:
        """Start a new scanner run, return its ID."""
        cursor = self.conn.execute(
            "INSERT INTO scanner_runs (config_json) VALUES (?)",
            (config_json,),
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_scanner_run(self, run_id: int, num_tokens: int,
                           num_pairs: int, num_cointegrated: int) -> None:
        """Update a scanner run with results."""
        self.conn.execute(
            """UPDATE scanner_runs
               SET completed_at = CURRENT_TIMESTAMP,
                   num_tokens = ?, num_pairs_analyzed = ?, num_cointegrated = ?
               WHERE id = ?""",
            (num_tokens, num_pairs, num_cointegrated, run_id),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()
