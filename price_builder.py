"""
Price time-series builder for cointegration scanner.
Fetches token prices from Jupiter Price API and caches them for time-series analysis.
"""

import logging
import time
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set

import aiohttp
import pandas as pd

from constants import WELL_KNOWN_TOKENS

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
    """Builds price time-series from Jupiter Price API data."""

    def __init__(self, db, config=None):
        self.db = db
        self.config = config

    def build_all_series(self, token_filter: Optional[Set[str]] = None,
                         **kwargs) -> Dict[str, pd.DataFrame]:
        """
        Build price series from cached Jupiter prices.

        Returns dict of {token_mint: DataFrame} where each DataFrame has:
          - index: DatetimeIndex
          - columns: ['price'] (USD via Jupiter)

        Data persists across restarts — no warmup needed.
        """
        prices = self._load_cached_prices(token_filter, source='jupiter')
        logger.info(f"Loaded {len(prices)} cached Jupiter prices")

        if not prices:
            logger.warning("No Jupiter price data available")
            return {}

        series = self._build_dataframes(prices)
        logger.info(f"Built price series for {len(series)} tokens")

        return series

    def _load_cached_prices(self, token_filter: Optional[Set[str]] = None,
                            source: Optional[str] = None) -> List[PricePoint]:
        """Load previously cached prices from the database."""
        tokens = token_filter or set()
        if not tokens:
            tokens = set(self.db.get_cached_tokens())

        prices = []
        for mint in tokens:
            rows = self.db.get_cached_prices(mint, source=source)
            for row in rows:
                prices.append(PricePoint(
                    token_mint=row[0],
                    quote_mint=row[1],
                    price=row[2],
                    timestamp=row[3],
                    source=row[4],
                ))
        return prices

    def _build_dataframes(self, prices: List[PricePoint]) -> Dict[str, pd.DataFrame]:
        """Group prices by token into DataFrames.

        Since Jupiter prices are already regular-interval snapshots from polling,
        we use them directly without resampling. This avoids data loss from
        sparse resampling buckets and means the scanner works immediately
        with whatever cached data exists (no warmup needed).
        """
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

            series[mint] = df

        return series

    async def fetch_jupiter_prices(self, token_mints: List[str]) -> List[PricePoint]:
        """Fetch current prices from Jupiter Price API v3."""
        if not token_mints:
            return []

        prices = []
        now = int(time.time())
        api_key = getattr(self.config, 'jupiter_api_key', '') if self.config else ''

        headers = {'Accept': 'application/json'}
        if api_key:
            headers['x-api-key'] = api_key

        try:
            async with aiohttp.ClientSession() as session:
                # Batch in chunks of 100 (URL length limits)
                for i in range(0, len(token_mints), 100):
                    batch = token_mints[i:i + 100]
                    ids = ','.join(batch)
                    url = f"https://api.jup.ag/price/v3?ids={ids}"

                    async with session.get(url, headers=headers) as resp:
                        if resp.status != 200:
                            logger.warning(f"Jupiter API returned {resp.status}")
                            continue
                        data = await resp.json()

                    # v3 response: {mint: {usdPrice: float, ...}, ...}
                    for mint_id in batch:
                        entry = data.get(mint_id)
                        if not entry or not entry.get('usdPrice'):
                            continue
                        price = float(entry['usdPrice'])
                        if price > 0:
                            prices.append(PricePoint(
                                token_mint=mint_id,
                                quote_mint='USD',
                                price=price,
                                timestamp=now,
                                source='jupiter',
                            ))
        except Exception as e:
            logger.warning(f"Jupiter API error: {e}")

        if prices:
            self.db.save_price_cache([p.to_dict() for p in prices])
            logger.info(f"Cached {len(prices)} Jupiter prices")

        return prices
