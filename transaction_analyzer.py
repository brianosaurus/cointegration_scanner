"""
Transaction analyzer — core arbitrage detection logic
Orchestrates per-transaction analysis: extract swaps, compute balance changes,
detect circular token flows, classify arbitrage.
"""

import base58
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional

from swap_detector import SwapDetector
from grpc_utils import extract_signer, extract_addresses, contains_jito_tip_account
from constants import JUPITER_PROGRAMS, SOL_MINT, WELL_KNOWN_TOKENS, JITO_TIP_ACCOUNTS

logger = logging.getLogger(__name__)


@dataclass
class SwapLeg:
    dex: str
    pool_address: Optional[str]
    token_in_mint: Optional[str] = None
    token_out_mint: Optional[str] = None
    amount_in: int = 0
    amount_out: int = 0
    decimals_in: int = 0
    decimals_out: int = 0


@dataclass
class ArbitrageTransaction:
    signature: str
    slot: int
    block_time: int
    signer: str
    swap_legs: List[SwapLeg] = field(default_factory=list)
    net_profit: Dict[str, float] = field(default_factory=dict)  # mint -> net change (human-readable)
    is_profitable: bool = False
    uses_jupiter: bool = False
    has_jito_tip: bool = False
    jito_tip_amount: float = 0.0  # SOL amount tipped to Jito
    is_backrun: bool = False
    tx_index: int = 0  # position within the block
    num_swaps: int = 0


class TransactionAnalyzer:
    def __init__(self, min_swaps: int = 2):
        self.detector = SwapDetector()
        self.min_swaps = min_swaps

    def analyze(self, transaction_data, slot: int, block_time: int = 0, tx_index: int = 0) -> Optional[ArbitrageTransaction]:
        """Analyze a single transaction for arbitrage patterns.
        Returns ArbitrageTransaction if arb detected, None otherwise."""

        # Extract signature
        try:
            sig_bytes = transaction_data.signature
            signature = base58.b58encode(sig_bytes).decode('utf-8')
        except Exception:
            return None

        # Skip failed transactions
        meta = transaction_data.meta
        if meta.err and len(meta.err) > 0:
            return None

        # Extract signer
        signer = extract_signer(transaction_data)
        if not signer:
            return None

        # Detect swaps
        swaps = self.detector.analyze_transaction(transaction_data)
        if len(swaps) < self.min_swaps:
            return None

        # Check for Jupiter usage
        addresses = extract_addresses(transaction_data, meta)
        uses_jupiter = any(addr in JUPITER_PROGRAMS for addr in addresses)
        has_jito_tip = contains_jito_tip_account(addresses)
        jito_tip_amount = self._calculate_jito_tip(transaction_data) if has_jito_tip else 0.0

        # Build swap legs from detected swaps
        swap_legs = self._build_swap_legs(swaps)

        # Compute net token balance changes for the signer
        net_profit = self._compute_net_balance_changes(transaction_data, signer)

        # Determine if this is an arbitrage pattern
        is_arb = self._is_arbitrage_pattern(swap_legs, net_profit)
        if not is_arb:
            return None

        # Determine profitability
        is_profitable = self._is_profitable(net_profit)

        return ArbitrageTransaction(
            signature=signature,
            slot=slot,
            block_time=block_time,
            signer=signer,
            swap_legs=swap_legs,
            net_profit=net_profit,
            is_profitable=is_profitable,
            uses_jupiter=uses_jupiter,
            has_jito_tip=has_jito_tip,
            jito_tip_amount=jito_tip_amount,
            tx_index=tx_index,
            num_swaps=len(swap_legs),
        )

    def _build_swap_legs(self, swaps: List[Dict]) -> List[SwapLeg]:
        """Convert raw swap dicts into SwapLeg objects with token flow info."""
        legs = []
        for swap in swaps:
            vault_changes = swap.get('vault_balance_changes', {})

            # Identify token in (vault received tokens, negative change for user)
            # and token out (vault sent tokens, positive change for user)
            token_in_mint = None
            token_out_mint = None
            amount_in = 0
            amount_out = 0
            decimals_in = 0
            decimals_out = 0

            for vault_addr, info in vault_changes.items():
                change = info.get('balance_change', 0)
                if change > 0:
                    # Vault received tokens -> user sent these (token_in)
                    token_in_mint = info.get('mint')
                    amount_in = abs(change)
                    decimals_in = info.get('decimals', 0)
                elif change < 0:
                    # Vault sent tokens -> user received these (token_out)
                    token_out_mint = info.get('mint')
                    amount_out = abs(change)
                    decimals_out = info.get('decimals', 0)

            legs.append(SwapLeg(
                dex=swap.get('dex', 'Unknown'),
                pool_address=swap.get('pool_address'),
                token_in_mint=token_in_mint,
                token_out_mint=token_out_mint,
                amount_in=amount_in,
                amount_out=amount_out,
                decimals_in=decimals_in,
                decimals_out=decimals_out,
            ))
        return legs

    def _compute_net_balance_changes(self, transaction_data, signer: str) -> Dict[str, float]:
        """Compute net token balance changes for the signer.
        Returns {mint: human-readable net change}."""
        meta = transaction_data.meta
        net = {}

        # SPL token balance changes
        pre_balances = {}
        post_balances = {}

        for bal in meta.pre_token_balances:
            if bal.owner == signer:
                mint = bal.mint
                try:
                    amount = int(bal.ui_token_amount.amount)
                    decimals = bal.ui_token_amount.decimals
                except (ValueError, TypeError, AttributeError):
                    continue
                pre_balances[mint] = (amount, decimals)

        for bal in meta.post_token_balances:
            if bal.owner == signer:
                mint = bal.mint
                try:
                    amount = int(bal.ui_token_amount.amount)
                    decimals = bal.ui_token_amount.decimals
                except (ValueError, TypeError, AttributeError):
                    continue
                post_balances[mint] = (amount, decimals)

        all_mints = set(pre_balances.keys()) | set(post_balances.keys())
        for mint in all_mints:
            pre_amount, pre_dec = pre_balances.get(mint, (0, 0))
            post_amount, post_dec = post_balances.get(mint, (0, 0))
            decimals = max(pre_dec, post_dec)
            change = post_amount - pre_amount
            if change != 0 and decimals > 0:
                net[mint] = change / (10 ** decimals)

        # SOL balance changes (native lamports)
        try:
            # Find signer's account index
            account_keys = transaction_data.transaction.message.account_keys
            signer_index = None
            for i, key in enumerate(account_keys):
                addr = base58.b58encode(key).decode('utf-8')
                if addr == signer:
                    signer_index = i
                    break

            if signer_index is not None and signer_index < len(meta.pre_balances) and signer_index < len(meta.post_balances):
                pre_sol = meta.pre_balances[signer_index]
                post_sol = meta.post_balances[signer_index]
                sol_change = post_sol - pre_sol
                if sol_change != 0:
                    net[SOL_MINT] = sol_change / 1e9
        except Exception:
            pass

        return net

    def _is_arbitrage_pattern(self, legs: List[SwapLeg], net_profit: Dict[str, float]) -> bool:
        """Detect arbitrage: circular token flow or cross-DEX same-pair."""
        if len(legs) < self.min_swaps:
            return False

        # Check for circular flow: first token_in == last token_out
        first_in = legs[0].token_in_mint
        last_out = legs[-1].token_out_mint
        if first_in and last_out and first_in == last_out:
            return True

        # Check cross-DEX: same token pair on different DEXes
        dexes = set()
        pairs = set()
        for leg in legs:
            if leg.token_in_mint and leg.token_out_mint:
                pair = frozenset([leg.token_in_mint, leg.token_out_mint])
                if pair in pairs and leg.dex not in dexes:
                    return True
                pairs.add(pair)
                dexes.add(leg.dex)

        # Multi-swap with net profit in starting token suggests arb even if we
        # couldn't perfectly trace the flow
        if len(legs) >= 2 and len(net_profit) > 0:
            return True

        return False

    def _is_profitable(self, net_profit: Dict[str, float]) -> bool:
        """Check if the net profit is positive in any token."""
        for mint, change in net_profit.items():
            if change > 0:
                return True
        return False

    def _calculate_jito_tip(self, transaction_data) -> float:
        """Calculate total SOL tipped to Jito tip accounts."""
        meta = transaction_data.meta
        try:
            account_keys = transaction_data.transaction.message.account_keys
            loaded_writable = meta.loaded_writable_addresses if hasattr(meta, 'loaded_writable_addresses') else []
            loaded_readonly = meta.loaded_readonly_addresses if hasattr(meta, 'loaded_readonly_addresses') else []
            all_keys = list(account_keys) + list(loaded_writable) + list(loaded_readonly)

            total_tip = 0
            for i, key in enumerate(all_keys):
                addr = base58.b58encode(key).decode('utf-8')
                if addr in JITO_TIP_ACCOUNTS and i < len(meta.pre_balances) and i < len(meta.post_balances):
                    change = meta.post_balances[i] - meta.pre_balances[i]
                    if change > 0:
                        total_tip += change

            return total_tip / 1e9  # lamports to SOL
        except Exception:
            return 0.0
