"""
Cointegration analysis engine.
Implements Engle-Granger and Johansen tests for identifying cointegrated token pairs.
"""

import logging
import time
from dataclasses import dataclass
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
    token_a_mint: str
    token_b_mint: str
    token_a_symbol: str
    token_b_symbol: str
    # Engle-Granger
    eg_test_statistic: float
    eg_p_value: float
    eg_is_cointegrated: bool
    # Johansen
    johansen_trace_stat: float
    johansen_eigen_stat: float
    johansen_rank: int
    johansen_is_cointegrated: bool
    # Trading metrics
    hedge_ratio: float
    spread_mean: float
    spread_std: float
    current_spread: float
    current_zscore: float
    half_life: float
    # Data quality
    correlation: float
    num_observations: int
    start_time: int
    end_time: int
    quote_token: str
    analyzed_at: int


class CointegrationAnalyzer:
    """Runs cointegration tests on token price series."""

    def __init__(self, min_observations: int = 100, p_threshold: float = 0.05,
                 lookback: int = 60, max_pairs: int = 500):
        self.min_observations = min_observations
        self.p_threshold = p_threshold
        self.lookback = lookback
        self.max_pairs = max_pairs

    def analyze_all_pairs(self, series: Dict[str, pd.DataFrame],
                          token_filter: Optional[set] = None) -> List[CointegrationResult]:
        """
        Test all unique pairs from the given price series for cointegration.

        Args:
            series: {token_mint: DataFrame with 'price' column}
            token_filter: if set, only include these mints

        Returns:
            List of CointegrationResult sorted by EG p-value (best first)
        """
        mints = list(series.keys())
        if token_filter:
            mints = [m for m in mints if m in token_filter]

        # Build pairs with diverse coverage, not just SOL/X
        known = [m for m in mints if m in WELL_KNOWN_TOKENS]
        unknown = [m for m in mints if m not in WELL_KNOWN_TOKENS]

        # Sort unknown tokens by data density (most observations first)
        unknown.sort(key=lambda m: len(series[m]), reverse=True)

        # Priority tiers:
        # 1. known-known pairs (e.g. SOL/mSOL, USDC/USDT) — always include
        # 2. known-unknown pairs — sample top unknown tokens against each known
        # 3. unknown-unknown pairs — sample top pairs by data overlap
        tier1 = list(combinations(known, 2))

        tier2 = []
        for k in known:
            for u in unknown:
                tier2.append((k, u))

        tier3 = list(combinations(unknown[:50], 2))  # top 50 unknown by data

        total_possible = len(tier1) + len(tier2) + len(tier3)
        all_pairs = tier1 + tier2 + tier3

        if len(all_pairs) > self.max_pairs:
            # Keep all tier1, then fill from tier2 and tier3
            budget = self.max_pairs - len(tier1)
            all_pairs = tier1 + (tier2 + tier3)[:budget]
            logger.info(f"Limiting to {len(all_pairs)} pairs (of {total_possible} possible)")

        logger.info(f"Analyzing {len(all_pairs)} pairs from {len(mints)} tokens")

        results = []
        for i, (mint_a, mint_b) in enumerate(all_pairs):
            if (i + 1) % 50 == 0:
                logger.info(f"  Progress: {i + 1}/{len(all_pairs)} pairs analyzed")

            result = self.analyze_pair(series[mint_a], series[mint_b], mint_a, mint_b)
            if result is not None:
                results.append(result)

        # Sort by EG p-value (lowest first = most significant)
        results.sort(key=lambda r: r.eg_p_value)

        cointegrated = sum(1 for r in results if r.eg_is_cointegrated or r.johansen_is_cointegrated)
        logger.info(f"Found {cointegrated} cointegrated pairs out of {len(results)} analyzed")

        return results

    def analyze_pair(self, df_a: pd.DataFrame, df_b: pd.DataFrame,
                     mint_a: str, mint_b: str) -> Optional[CointegrationResult]:
        """Run cointegration analysis on a single pair."""
        sym_a = token_symbol(mint_a)
        sym_b = token_symbol(mint_b)

        try:
            # Align the two series on their shared timestamps
            aligned = pd.concat([df_a['price'], df_b['price']], axis=1, join='inner')
            aligned.columns = ['a', 'b']
            aligned = aligned.dropna()

            if len(aligned) < self.min_observations:
                return None

            prices_a = aligned['a'].values
            prices_b = aligned['b'].values

            # Use log prices for regression
            log_a = np.log(prices_a)
            log_b = np.log(prices_b)

            # Check for constant series
            if np.std(log_a) == 0 or np.std(log_b) == 0:
                return None

            # Engle-Granger test
            eg_stat, eg_pval, hedge_ratio, spread = self._engle_granger(log_a, log_b)

            # Johansen test
            j_trace, j_eigen, j_rank = self._johansen(log_a, log_b)

            # Half-life of mean reversion
            half_life = self._half_life(spread)

            # Current z-score (using lookback window)
            lookback = min(self.lookback, len(spread))
            recent_spread = spread[-lookback:]
            spread_mean = np.mean(recent_spread)
            spread_std = np.std(recent_spread)
            current_spread = spread[-1]

            if spread_std > 0:
                current_zscore = (current_spread - spread_mean) / spread_std
            else:
                current_zscore = 0.0

            # Correlation
            correlation = np.corrcoef(log_a, log_b)[0, 1]

            # Timestamps
            start_time = int(aligned.index[0].timestamp())
            end_time = int(aligned.index[-1].timestamp())

            return CointegrationResult(
                token_a_mint=mint_a,
                token_b_mint=mint_b,
                token_a_symbol=sym_a,
                token_b_symbol=sym_b,
                eg_test_statistic=eg_stat,
                eg_p_value=eg_pval,
                eg_is_cointegrated=eg_pval < self.p_threshold,
                johansen_trace_stat=j_trace,
                johansen_eigen_stat=j_eigen,
                johansen_rank=j_rank,
                johansen_is_cointegrated=j_rank > 0,
                hedge_ratio=hedge_ratio,
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
            logger.debug(f"Error analyzing {sym_a}/{sym_b}: {e}")
            return None

    def _engle_granger(self, log_a: np.ndarray, log_b: np.ndarray
                       ) -> Tuple[float, float, float, np.ndarray]:
        """
        Engle-Granger two-step cointegration test.

        Step 1: OLS regression  log_a = alpha + beta * log_b + residual
        Step 2: ADF test on residuals

        Returns: (adf_stat, p_value, hedge_ratio, spread)
        """
        # Step 1: OLS
        X = add_constant(log_b)
        model = OLS(log_a, X).fit()
        hedge_ratio = model.params[1]
        residuals = model.resid

        # Step 2: ADF on residuals
        adf_result = adfuller(residuals, maxlag=None, autolag='AIC')
        adf_stat = adf_result[0]
        p_value = adf_result[1]

        # Spread = log_a - hedge_ratio * log_b
        spread = log_a - hedge_ratio * log_b

        return adf_stat, p_value, hedge_ratio, spread

    def _johansen(self, log_a: np.ndarray, log_b: np.ndarray
                  ) -> Tuple[float, float, int]:
        """
        Johansen cointegration test.

        Returns: (trace_stat, eigen_stat, cointegration_rank)
        """
        try:
            data = np.column_stack([log_a, log_b])

            # det_order=-1 means no deterministic terms, k_ar_diff=1 is standard
            result = coint_johansen(data, det_order=0, k_ar_diff=1)

            # Trace statistic for rank=0 (null: no cointegration)
            trace_stat = result.lr1[0]  # trace stat for r=0
            trace_cv_95 = result.cvt[0, 1]  # 95% critical value for r=0

            # Max eigenvalue statistic for rank=0
            eigen_stat = result.lr2[0]
            eigen_cv_95 = result.cvm[0, 1]

            # Determine rank: how many cointegrating relationships
            rank = 0
            if trace_stat > trace_cv_95:
                rank = 1
                # Check if rank could be 2
                if len(result.lr1) > 1 and result.lr1[1] > result.cvt[1, 1]:
                    rank = 2

            return trace_stat, eigen_stat, rank

        except Exception as e:
            logger.debug(f"Johansen test failed: {e}")
            return 0.0, 0.0, 0

    def _half_life(self, spread: np.ndarray) -> float:
        """
        Calculate half-life of mean reversion via AR(1) model.

        Fits: spread_t - spread_{t-1} = phi * spread_{t-1} + noise
        Half-life = -log(2) / log(1 + phi)

        Returns half-life in periods, or float('inf') if not mean-reverting.
        """
        if len(spread) < 10:
            return float('inf')

        try:
            lag = spread[:-1]
            diff = np.diff(spread)

            # OLS: diff = phi * lag + intercept
            X = add_constant(lag)
            model = OLS(diff, X).fit()
            phi = model.params[1]

            # phi should be negative for mean reversion
            # The AR(1) coefficient is (1 + phi)
            ar_coeff = 1.0 + phi

            if ar_coeff <= 0 or ar_coeff >= 1:
                return float('inf')

            half_life = -np.log(2) / np.log(ar_coeff)

            if half_life <= 0:
                return float('inf')

            return half_life

        except Exception:
            return float('inf')
