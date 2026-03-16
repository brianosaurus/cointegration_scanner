"""
Cointegration analysis engine.
Implements Johansen test for identifying cointegrated token baskets (2-4 tokens).
Engle-Granger is used as a supplementary test for pairs (size 2).
"""

import logging
import time
from dataclasses import dataclass, field
from itertools import combinations
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.vector_ar.vecm import coint_johansen
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant

from constants import WELL_KNOWN_TOKENS

logger = logging.getLogger(__name__)


def token_symbol(mint: str) -> str:
    info = WELL_KNOWN_TOKENS.get(mint)
    return info['symbol'] if info else mint[:8] + '..'


@dataclass
class CointegrationResult:
    mints: List[str]              # sorted list of token mints
    symbols: List[str]            # corresponding symbols, same order as mints
    basket_key: str               # ",".join(sorted(mints))
    basket_size: int
    # Engle-Granger (only for size==2)
    eg_test_statistic: Optional[float]
    eg_p_value: Optional[float]
    eg_is_cointegrated: bool
    # Johansen (always present)
    johansen_trace_stat: float
    johansen_eigen_stat: float
    johansen_rank: int
    johansen_is_cointegrated: bool
    # Trading metrics
    hedge_ratios: List[float]     # Johansen eigenvector weights (length N)
    spread_mean: float
    spread_std: float
    current_spread: float
    current_zscore: float
    half_life: float
    # Data quality
    correlation: float            # pairwise for N==2, mean pairwise for N>2
    num_observations: int
    start_time: int
    end_time: int
    quote_token: str
    analyzed_at: int

    @property
    def pair_label(self) -> str:
        return "/".join(self.symbols)


class CointegrationAnalyzer:
    """Runs cointegration tests on token price series."""

    def __init__(self, min_observations: int = 100, p_threshold: float = 0.05,
                 lookback: int = 60, max_pairs: int = 500, basket_size: int = 4):
        self.min_observations = min_observations
        self.p_threshold = p_threshold
        self.lookback = lookback
        self.max_baskets = max_pairs
        self.basket_size = basket_size

    def analyze_all_baskets(self, series: Dict[str, pd.DataFrame],
                            token_filter: Optional[set] = None) -> List[CointegrationResult]:
        """
        Test all token combinations of size basket_size for cointegration.

        Args:
            series: {token_mint: DataFrame with 'price' column}
            token_filter: if set, only include these mints

        Returns:
            List of CointegrationResult sorted by significance (best first)
        """
        mints = list(series.keys())
        if token_filter:
            mints = [m for m in mints if m in token_filter]

        bs = self.basket_size

        # Tiered basket construction
        known = [m for m in mints if m in WELL_KNOWN_TOKENS]
        unknown = [m for m in mints if m not in WELL_KNOWN_TOKENS]
        unknown.sort(key=lambda m: len(series[m]), reverse=True)

        # Tier 1: all-known baskets
        tier1 = list(combinations(known, bs))

        # Tier 2: (bs-1) known + 1 unknown
        tier2 = []
        if unknown and len(known) >= bs - 1:
            for known_combo in combinations(known, bs - 1):
                for u in unknown:
                    tier2.append(tuple(sorted(known_combo + (u,))))

        # Tier 3: (bs-2) known + 2 unknown (only if enough tokens)
        tier3 = []
        if len(unknown) >= 2 and len(known) >= max(bs - 2, 1):
            for known_combo in combinations(known, max(bs - 2, 1)):
                for unknown_combo in combinations(unknown[:30], min(2, bs - max(bs - 2, 1))):
                    basket = tuple(sorted(known_combo + unknown_combo))
                    if len(basket) == bs:
                        tier3.append(basket)

        # Deduplicate (sorted tuples ensure consistency)
        seen = set()
        all_baskets = []
        for basket in tier1 + tier2 + tier3:
            if basket not in seen:
                seen.add(basket)
                all_baskets.append(basket)

        total_possible = len(all_baskets)
        if len(all_baskets) > self.max_baskets:
            # Keep all tier1, then fill from rest
            t1_set = set(tier1)
            rest = [b for b in all_baskets if b not in t1_set]
            all_baskets = list(tier1)[:self.max_baskets] + rest[:self.max_baskets - min(len(tier1), self.max_baskets)]
            all_baskets = all_baskets[:self.max_baskets]
            logger.info(f"Limiting to {len(all_baskets)} baskets (of {total_possible} possible)")

        logger.info(f"Analyzing {len(all_baskets)} baskets of size {bs} from {len(mints)} tokens")

        results = []
        for i, basket in enumerate(all_baskets):
            if (i + 1) % 100 == 0:
                logger.info(f"  Progress: {i + 1}/{len(all_baskets)} baskets analyzed")

            series_list = [series[m] for m in basket]
            result = self.analyze_basket(series_list, list(basket))
            if result is not None:
                results.append(result)

        # Sort: Johansen trace stat descending (higher = more significant)
        results.sort(key=lambda r: r.johansen_trace_stat, reverse=True)

        cointegrated = sum(1 for r in results if r.johansen_is_cointegrated)
        logger.info(f"Found {cointegrated} cointegrated baskets out of {len(results)} analyzed")

        return results

    def analyze_basket(self, series_list: List[pd.DataFrame],
                       mints: List[str]) -> Optional[CointegrationResult]:
        """Run cointegration analysis on a basket of N tokens."""
        # Sort mints and reorder series to match
        sorted_indices = sorted(range(len(mints)), key=lambda i: mints[i])
        mints = [mints[i] for i in sorted_indices]
        series_list = [series_list[i] for i in sorted_indices]
        symbols = [token_symbol(m) for m in mints]

        try:
            # Align all series on shared timestamps
            price_cols = [df['price'].rename(m) for m, df in zip(mints, series_list)]
            aligned = pd.concat(price_cols, axis=1, join='inner').dropna()

            if len(aligned) < self.min_observations:
                return None

            log_prices = np.log(aligned.values)  # shape [T, N]

            # Check for constant series
            for col in range(log_prices.shape[1]):
                if np.std(log_prices[:, col]) == 0:
                    return None

            n = len(mints)

            # Engle-Granger (pairs only)
            eg_stat, eg_pval = None, None
            eg_is_coint = False
            if n == 2:
                eg_stat, eg_pval, _, _ = self._engle_granger(log_prices[:, 0], log_prices[:, 1])
                eg_is_coint = eg_pval < self.p_threshold

            # Johansen test (all sizes)
            j_trace, j_eigen, j_rank, eigvec = self._johansen(log_prices)

            # Spread = eigenvector dot log_prices
            weights = eigvec  # length N
            spread = log_prices @ weights

            # Half-life
            half_life = self._half_life(spread)

            # Z-score
            lookback = min(self.lookback, len(spread))
            recent = spread[-lookback:]
            spread_mean = float(np.mean(recent))
            spread_std = float(np.std(recent))
            current_spread = float(spread[-1])
            current_zscore = (current_spread - spread_mean) / spread_std if spread_std > 0 else 0.0

            # Correlation
            if n == 2:
                correlation = float(np.corrcoef(log_prices[:, 0], log_prices[:, 1])[0, 1])
            else:
                corr_matrix = np.corrcoef(log_prices.T)
                # Mean of upper triangle (excluding diagonal)
                mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)
                correlation = float(np.mean(corr_matrix[mask]))

            start_time = int(aligned.index[0].timestamp())
            end_time = int(aligned.index[-1].timestamp())

            basket_key = ",".join(mints)

            return CointegrationResult(
                mints=mints,
                symbols=symbols,
                basket_key=basket_key,
                basket_size=n,
                eg_test_statistic=eg_stat,
                eg_p_value=eg_pval,
                eg_is_cointegrated=eg_is_coint,
                johansen_trace_stat=j_trace,
                johansen_eigen_stat=j_eigen,
                johansen_rank=j_rank,
                johansen_is_cointegrated=j_rank > 0,
                hedge_ratios=[float(w) for w in weights],
                spread_mean=spread_mean,
                spread_std=spread_std,
                current_spread=current_spread,
                current_zscore=current_zscore,
                half_life=half_life,
                correlation=correlation,
                num_observations=len(aligned),
                start_time=start_time,
                end_time=end_time,
                quote_token='log_ratio',
                analyzed_at=int(time.time()),
            )

        except Exception as e:
            logger.debug(f"Error analyzing {'/'.join(symbols) if 'symbols' in dir() else mints}: {e}")
            return None

    # Keep for backward compat with run_zscore which calls analyze_pair
    def analyze_pair(self, df_a: pd.DataFrame, df_b: pd.DataFrame,
                     mint_a: str, mint_b: str) -> Optional[CointegrationResult]:
        return self.analyze_basket([df_a, df_b], [mint_a, mint_b])

    def _engle_granger(self, log_a: np.ndarray, log_b: np.ndarray
                       ) -> Tuple[float, float, float, np.ndarray]:
        """
        Engle-Granger two-step cointegration test.
        Returns: (adf_stat, p_value, hedge_ratio, spread)
        """
        X = add_constant(log_b)
        model = OLS(log_a, X).fit()
        hedge_ratio = model.params[1]
        residuals = model.resid

        adf_result = adfuller(residuals, maxlag=None, autolag='AIC')
        adf_stat = adf_result[0]
        p_value = adf_result[1]

        spread = log_a - hedge_ratio * log_b
        return adf_stat, p_value, hedge_ratio, spread

    def _johansen(self, log_prices: np.ndarray
                  ) -> Tuple[float, float, int, np.ndarray]:
        """
        Johansen cointegration test for N variables.

        Args:
            log_prices: array of shape [T, N]

        Returns: (trace_stat, eigen_stat, rank, eigenvector_weights)
        """
        n = log_prices.shape[1]
        try:
            result = coint_johansen(log_prices, det_order=0, k_ar_diff=1)

            trace_stat = float(result.lr1[0])
            eigen_stat = float(result.lr2[0])

            # Determine cointegration rank
            rank = 0
            for r in range(min(len(result.lr1), n - 1)):
                if result.lr1[r] > result.cvt[r, 1]:  # 95% critical value
                    rank = r + 1
                else:
                    break

            # First eigenvector gives the cointegrating relationship weights
            # Normalize so the largest absolute weight is 1.0
            evec = result.evec[:, 0].copy()
            max_abs = np.max(np.abs(evec))
            if max_abs > 0:
                evec = evec / max_abs

            return trace_stat, eigen_stat, rank, evec

        except Exception as e:
            logger.debug(f"Johansen test failed: {e}")
            return 0.0, 0.0, 0, np.ones(n) / n

    def _half_life(self, spread: np.ndarray) -> float:
        """
        Calculate half-life of mean reversion via AR(1) model.
        Returns half-life in periods, or float('inf') if not mean-reverting.
        """
        if len(spread) < 10:
            return float('inf')

        try:
            lag = spread[:-1]
            diff = np.diff(spread)

            X = add_constant(lag)
            model = OLS(diff, X).fit()
            phi = model.params[1]

            ar_coeff = 1.0 + phi
            if ar_coeff <= 0 or ar_coeff >= 1:
                return float('inf')

            half_life = -np.log(2) / np.log(ar_coeff)
            return half_life if half_life > 0 else float('inf')

        except Exception:
            return float('inf')
