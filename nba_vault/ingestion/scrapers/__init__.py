"""Web scrapers for data ingestion.

This package contains specialized web scrapers for various data sources.
Each scraper is responsible for fetching and parsing data from a specific source.
"""

from nba_vault.ingestion.scrapers.injury_scrapers import (
    BaseInjuryScraper,
    ESPNInjuryScraper,
    RotowireInjuryScraper,
)

__all__ = [
    "BaseInjuryScraper",
    "ESPNInjuryScraper",
    "RotowireInjuryScraper",
]
