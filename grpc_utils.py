"""
Utility functions for gRPC transaction processing — adapted from arbito/grpc_utils.py
"""

import base58
from functools import lru_cache
from constants import JUPITER_PROGRAMS, KNOWN_BOT_WALLETS, JITO_TIP_ACCOUNTS


@lru_cache(maxsize=200000)
def b58encode(byte_data: bytes) -> str:
    """Base58-encode 32-byte account keys, memoized across the whole run.

    The same program IDs and token accounts recur in nearly every transaction,
    so caching avoids re-running the (relatively expensive) encode millions of
    times. `byte_data` must be bytes (hashable) for the cache to work."""
    return base58.b58encode(byte_data).decode('utf-8')


def extract_signer(transaction) -> str:
    """Extract the first signer (account key) from transaction as string"""
    try:
        if hasattr(transaction, 'transaction'):
            tx = transaction.transaction
        else:
            tx = transaction

        message = tx.message
        for account_key in message.account_keys:
            return b58encode(account_key)
    except Exception:
        pass
    return ""


def should_skip_transaction(signer: str) -> bool:
    """Check if transaction should be skipped based on filtering criteria"""
    return signer in KNOWN_BOT_WALLETS


def extract_addresses(transaction, meta) -> list[str]:
    """Extract all account keys from transaction as strings"""
    addresses = []

    if hasattr(transaction, 'transaction'):
        tx = transaction.transaction
    else:
        tx = transaction

    message = tx.message
    for account_key in message.account_keys:
        addresses.append(b58encode(account_key))

    for loaded_writable in meta.loaded_writable_addresses:
        addresses.append(b58encode(loaded_writable))

    for loaded_readonly in meta.loaded_readonly_addresses:
        addresses.append(b58encode(loaded_readonly))

    return addresses


def contains_jito_tip_account(addresses: list) -> bool:
    """Check if any address in the list is a Jito tip account"""
    for address in addresses:
        if address in JITO_TIP_ACCOUNTS:
            return True
    return False
