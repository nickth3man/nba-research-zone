"""Base class for data ingestors."""

import sqlite3
from abc import ABC, abstractmethod
from typing import Any

import pydantic
import structlog

from nba_vault.utils.cache import ContentCache
from nba_vault.utils.rate_limit import RateLimiter, retry_with_backoff

logger = structlog.get_logger(__name__)


class BaseIngestor(ABC):
    """
    Abstract base class for data ingestors.

    All ingestors must inherit from this class and implement the required methods.
    """

    entity_type: str  # e.g., "player", "game", "team"

    def __init__(self, cache: ContentCache | None = None, rate_limiter: RateLimiter | None = None):
        """
        Initialize ingestor.

        Args:
            cache: Content cache for API responses. If None, creates default.
            rate_limiter: Rate limiter for API requests. If None, creates default.
        """
        self.cache = cache or ContentCache()
        self.rate_limiter = rate_limiter or RateLimiter(rate=8, per=60)
        self.logger = logger.bind(entity_type=self.entity_type)

    @abstractmethod
    def fetch(self, entity_id: str, **kwargs: Any) -> dict[str, Any]:
        """
        Fetch raw data for an entity.

        Args:
            entity_id: Identifier for the entity (e.g., player_id, game_id).
            **kwargs: Additional parameters for the fetch.

        Returns:
            Raw data as a dictionary.

        Raises:
            Exception: If fetch fails after retries.
        """
        pass

    @abstractmethod
    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        """
        Validate raw data using Pydantic model(s).

        Args:
            raw: Raw data dictionary.

        Returns:
            List of validated Pydantic models.

        Raises:
            pydantic.ValidationError: If validation fails.
        """
        pass

    @abstractmethod
    def upsert(self, model: list[pydantic.BaseModel], conn: Any) -> int:
        """
        Insert or update validated data in database.

        Args:
            model: List of validated Pydantic models.
            conn: SQLite database connection.

        Returns:
            Number of rows affected.
        """
        pass

    def ingest(self, entity_id: str, conn, **kwargs: Any) -> dict[str, Any]:
        """
        Complete ingestion pipeline for an entity.

        This method orchestrates fetch, validate, and upsert operations.

        Args:
            entity_id: Identifier for the entity.
            conn: SQLite database connection.
            **kwargs: Additional parameters for fetch.

        Returns:
            Dictionary with ingestion metadata (rows affected, status, etc.).
        """
        if not getattr(self, "entity_type", None):
            raise AttributeError(f"{type(self).__name__} must define a non-empty 'entity_type'")

        self.logger.info("Starting ingestion", entity_id=entity_id)

        try:
            # Fetch with retry and rate limiting
            def _fetch():
                self.rate_limiter.acquire()
                return self.fetch(entity_id, **kwargs)

            raw_data = retry_with_backoff(_fetch)

            # Validate
            validated = self.validate(raw_data)

            # Upsert
            rows_affected = self.upsert(validated, conn)

            self.logger.info(
                "Ingestion completed",
                entity_id=entity_id,
                rows_affected=rows_affected,
            )

            return {
                "status": "SUCCESS",
                "entity_id": entity_id,
                "rows_affected": rows_affected,
            }

        except pydantic.ValidationError as e:
            self.logger.error(
                "Validation failed",
                entity_id=entity_id,
                errors=str(e),
            )
            # TODO: Write raw data to quarantine directory
            return {
                "status": "FAILED",
                "entity_id": entity_id,
                "error": "ValidationError",
                "error_message": str(e),
            }

        except sqlite3.Error as e:
            self.logger.error(
                "Database error during upsert",
                entity_id=entity_id,
                error_type=type(e).__name__,
                error=str(e),
            )
            return {
                "status": "FAILED",
                "entity_id": entity_id,
                "error": type(e).__name__,
                "error_message": str(e),
            }

        except Exception as e:
            self.logger.error(
                "Ingestion failed",
                entity_id=entity_id,
                error=str(e),
            )
            return {
                "status": "FAILED",
                "entity_id": entity_id,
                "error": type(e).__name__,
                "error_message": str(e),
            }
