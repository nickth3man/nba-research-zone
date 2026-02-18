"""Adapters for NBA.com Stats API.

This package provides adapter implementations for accessing NBA.com Stats API
through different underlying libraries. The adapter pattern allows for easy
swapping of implementations without changing the client code.
"""

from nba_vault.ingestion.adapters.nba_stats_adapter import (
    NbaApiAdapter,
    NBAStatsAdapter,
    RateLimitError,
)

__all__ = [
    "NBAStatsAdapter",
    "NbaApiAdapter",
    "RateLimitError",
]
