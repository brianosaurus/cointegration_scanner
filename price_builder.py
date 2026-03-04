"""
Price time-series builder for cointegration scanner.
Derives token prices from swap_legs data and supplements with external APIs.
"""

import logging
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set

import aiohttp
import numpy as np
import pandas as pd

from constants import (
    WELL_KNOWN_TOKENS, QUOTE_PRIORITY, STABLECOIN_MINTS, SOL_MINT,
)

logger = logging.getLogger(__name__)


@dataclass
class PricePoint:
    token_mint: str
    quote_mint: str
    price: float
    timestamp: int
    source: str
    slot: Optional[int] = None
    dex: Optional[str] = None
    pool_address: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


def token_symbol(mint: str) -> str:
    info = WELL_KNOWN_TOKENS.get(mint)
    return info['symbol'] if info else mint[:8] + '..'


class PriceBuilder:
    """Builds price time-series from on-chain swap data and external APIs."""

    def __init__(self, db, config=None):
        self.db = db
        self.config = config

    def build_all_series(self, resample_interval: str = '5min',
                         token_filter: Optional[Set[str]] = None,
                         fetch_external: bool = False,
                         max_gap_fill: int = 3) -> Dict[str, pd.DataFrame]:
        """
        Build resampled price series for all discovered tokens.

        Returns dict of {token_mint: DataFrame} where each DataFrame has:
          - index: DatetimeIndex (resampled)
          - columns: ['price'] (in quote token, typically USDC)
        """
        # Step 1: Extract prices from swap_legs
        prices = self._extract_swap_prices(token_filter)
        logger.info(f"Extracted {len(prices)} price points from swap_legs")

        # Step 2: Load cached external prices
        cached = self._load_cached_prices(token_filter)
        prices.extend(cached)
        if cached:
            logger.info(f"Loaded {len(cached)} cached external prices")

        if not prices:
            logger.warning("No price data available")
            return {}

        # Step 3: Group by token mint and build DataFrames
        series = self._build_dataframes(prices, resample_interval, max_gap_fill)
        logger.info(f"Built price series for {len(series)} tokens")

        return series

    def _extract_swap_prices(self, token_filter: Optional[Set[str]] = None) -> List[PricePoint]:
        """Derive implied prices from swap_legs table."""
        rows = self.db.get_swap_prices()
        prices = []

        for row in rows:
            (token_in, token_out, amt_in, amt_out,
             dec_in, dec_out, dex, pool, block_time, slot) = row

            if not token_in or not token_out:
                continue
            if amt_in <= 0 or amt_out <= 0:
                continue

            human_in = amt_in / (10 ** dec_in)
            human_out = amt_out / (10 ** dec_out)

            if human_in == 0:
                continue

            # Determine which token is the "base" and which is the "quote"
            # We want prices denominated in a quote token (USDC > USDT > USDH > SOL)
            quote_mint = self._pick_quote(token_in, token_out)
            if quote_mint is None:
                # Neither token is a preferred quote — skip for now
                continue

            if quote_mint == token_out:
                # token_in is being priced in token_out
                base_mint = token_in
                price = human_out / human_in
            else:
                # token_out is being priced in token_in
                base_mint = token_out
                price = human_in / human_out

            if price <= 0 or not np.isfinite(price):
                continue

            if token_filter and base_mint not in token_filter:
                continue

            prices.append(PricePoint(
                token_mint=base_mint,
                quote_mint=quote_mint,
                price=price,
                timestamp=block_time,
                source='swap_legs',
                slot=slot,
                dex=dex,
                pool_address=pool,
            ))

        return prices

    def _pick_quote(self, token_a: str, token_b: str) -> Optional[str]:
        """Pick the best quote token from a pair based on priority."""
        for quote in QUOTE_PRIORITY:
            if token_a == quote:
                return token_a
            if token_b == quote:
                return token_b
        return None

    def _load_cached_prices(self, token_filter: Optional[Set[str]] = None) -> List[PricePoint]:
        """Load previously cached external prices from the database."""
        tokens = token_filter or set()
        if not tokens:
            # Check both swap_legs and price_cache for discovered tokens
            tokens = set(self.db.get_distinct_tokens())
            cached_tokens = self.db.get_cached_tokens()
            tokens.update(cached_tokens)

        prices = []
        for mint in tokens:
            rows = self.db.get_cached_prices(mint)
            for row in rows:
                prices.append(PricePoint(
                    token_mint=row[0],
                    quote_mint=row[1],
                    price=row[2],
                    timestamp=row[3],
                    source=row[4],
                ))
        return prices

    def _build_dataframes(self, prices: List[PricePoint],
                          resample_interval: str,
                          max_gap_fill: int) -> Dict[str, pd.DataFrame]:
        """Group prices by token and resample to regular intervals."""
        # Group by token_mint
        token_prices: Dict[str, List[PricePoint]] = {}
        for p in prices:
            token_prices.setdefault(p.token_mint, []).append(p)

        series = {}
        for mint, pts in token_prices.items():
            timestamps = [p.timestamp for p in pts]
            price_vals = [p.price for p in pts]

            df = pd.DataFrame({
                'price': price_vals,
            }, index=pd.to_datetime(timestamps, unit='s'))

            # Remove duplicates at same timestamp (keep median)
            df = df.groupby(level=0).median()
            df = df.sort_index()

            if len(df) < 2:
                continue

            # Resample to regular intervals using median price
            resampled = df.resample(resample_interval).median()

            # Count non-null before filling
            non_null = resampled['price'].notna().sum()
            total = len(resampled)
            if total == 0:
                continue

            # Forward-fill small gaps only
            resampled = resampled.ffill(limit=max_gap_fill)

            # Drop remaining NaN rows
            resampled = resampled.dropna()

            # Check data quality — require at least 50% non-null before fill
            fill_ratio = non_null / total
            if fill_ratio < 0.5:
                sym = token_symbol(mint)
                logger.debug(f"Skipping {sym}: only {fill_ratio:.0%} data coverage")
                continue

            if len(resampled) >= 2:
                series[mint] = resampled

        return series

    # --- External API methods ---

    async def fetch_birdeye_prices(self, token_mints: List[str],
                                   interval: str = '15m',
                                   time_from: int = None,
                                   time_to: int = None) -> List[PricePoint]:
        """Fetch historical OHLCV from Birdeye API."""
        if not self.config or not self.config.birdeye_api_key:
            logger.warning("No BIRDEYE_API_KEY configured, skipping Birdeye fetch")
            return []

        if time_to is None:
            time_to = int(time.time())
        if time_from is None:
            time_from = time_to - 86400 * 7  # Last 7 days

        prices = []
        headers = {
            'X-API-KEY': self.config.birdeye_api_key,
            'Accept': 'application/json',
        }

        async with aiohttp.ClientSession() as session:
            for mint in token_mints:
                try:
                    url = (
                        f"https://public-api.birdeye.so/defi/ohlcv"
                        f"?address={mint}&type={interval}"
                        f"&time_from={time_from}&time_to={time_to}"
                    )
                    async with session.get(url, headers=headers) as resp:
                        if resp.status != 200:
                            logger.debug(f"Birdeye {resp.status} for {token_symbol(mint)}")
                            continue
                        data = await resp.json()

                    items = data.get('data', {}).get('items', [])
                    for item in items:
                        close_price = item.get('c', 0)
                        ts = item.get('unixTime', 0)
                        if close_price > 0 and ts > 0:
                            prices.append(PricePoint(
                                token_mint=mint,
                                quote_mint='USD',
                                price=close_price,
                                timestamp=ts,
                                source='birdeye',
                            ))

                except Exception as e:
                    logger.debug(f"Birdeye error for {token_symbol(mint)}: {e}")

        # Cache fetched prices
        if prices:
            self.db.save_price_cache([p.to_dict() for p in prices])
            logger.info(f"Cached {len(prices)} Birdeye prices")

        return prices

    async def fetch_dexscreener_prices(self, token_mints: List[str]) -> List[PricePoint]:
        """Fetch current prices from DexScreener (free, no API key)."""
        prices = []

        async with aiohttp.ClientSession() as session:
            for mint in token_mints:
                try:
                    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()

                    pairs = data.get('pairs', [])
                    if not pairs:
                        continue

                    # Use the pair with highest liquidity
                    best = max(pairs, key=lambda p: float(p.get('liquidity', {}).get('usd', 0) or 0))
                    price_usd = float(best.get('priceUsd', 0))
                    if price_usd > 0:
                        prices.append(PricePoint(
                            token_mint=mint,
                            quote_mint='USD',
                            price=price_usd,
                            timestamp=int(time.time()),
                            source='dexscreener',
                        ))

                except Exception as e:
                    logger.debug(f"DexScreener error for {token_symbol(mint)}: {e}")

        if prices:
            self.db.save_price_cache([p.to_dict() for p in prices])

        return prices

    async def fetch_jupiter_prices(self, token_mints: List[str]) -> List[PricePoint]:
        """Fetch current prices from Jupiter Price API (free, no key)."""
        if not token_mints:
            return []

        prices = []
        ids = ','.join(token_mints)

        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://price.jup.ag/v6/price?ids={ids}"
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json()

                for mint_id, info in data.get('data', {}).items():
                    price = info.get('price', 0)
                    if price and price > 0:
                        prices.append(PricePoint(
                            token_mint=mint_id,
                            quote_mint='USD',
                            price=price,
                            timestamp=int(time.time()),
                            source='jupiter',
                        ))
        except Exception as e:
            logger.debug(f"Jupiter API error: {e}")

        if prices:
            self.db.save_price_cache([p.to_dict() for p in prices])

        return prices
