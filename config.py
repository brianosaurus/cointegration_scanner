"""
Simplified configuration for arbitrage tracker — read-only, no signing/execution
"""

import os
import logging
from dataclasses import dataclass
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

try:
    load_dotenv()
except Exception:
    pass


@dataclass
class Config:
    """Read-only tracker configuration — just needs RPC/gRPC endpoints"""
    rpc_url: str = os.getenv('SOLANA_RPC_URL', 'https://api.mainnet-beta.solana.com')
    grpc_endpoint: str = os.getenv('GRPC_ENDPOINT', 'api.mainnet-beta.solana.com:443')
    grpc_token: str = os.getenv('GRPC_TOKEN', '')

    def print_config_summary(self):
        logger.info(f"Config: RPC={self.rpc_url}, gRPC={self.grpc_endpoint}")


config = Config()
