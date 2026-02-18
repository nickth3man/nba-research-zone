"""Base class for data ingestors."""

import asyncio
import json
import sqlite3
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pydantic
import structlog

from nba_vault.utils.cache import ContentCache
from nba_vault.utils.config import get_settings
from nba_vault.utils.rate_limit import AsyncRateLimiter, RateLimiter, retry_with_backoff

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
                         Supports both RateLimiter (sync) and AsyncRateLimiter.
        """
        self.cache = cache or ContentCache()

        # Support async rate limiter if provided
        if rate_limiter is None:
            # Check if async context is available
            try:
                asyncio.get_running_loop()
                self.rate_limiter = AsyncRateLimiter(rate=8, per=60)
                self._is_async = True
            except RuntimeError:
                # No running event loop, use sync version
                self.rate_limiter = RateLimiter(rate=8, per=60)
                self._is_async = False
        else:
            self.rate_limiter = rate_limiter
            self._is_async = isinstance(rate_limiter, AsyncRateLimiter)

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
            # Write raw data to quarantine directory
            self._quarantine_data(entity_id, raw_data, str(e))
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

    def _quarantine_data(
        self, entity_id: str, raw_data: dict[str, Any], error_message: str
    ) -> Path:
        """
        Write failed validation data to quarantine directory.

        Args:
            entity_id: Identifier for the entity that failed validation.
            raw_data: Raw data that failed validation.
            error_message: Validation error message.

        Returns:
            Path to the quarantined file.
        """
        settings = get_settings()
        quarantine_base = Path(settings.quarantine_dir)

        # Create entity-type-specific subdirectory
        entity_dir = quarantine_base / self.entity_type
        entity_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename with timestamp and entity_id
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
        # Sanitize entity_id for filename (replace special chars with underscore)
        safe_entity_id = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in entity_id)
        filename = f"{timestamp}_{safe_entity_id}.json"
        filepath = entity_dir / filename

        # Prepare quarantine metadata
        quarantine_data = {
            "entity_id": entity_id,
            "entity_type": self.entity_type,
            "timestamp": datetime.now(UTC).isoformat(),
            "error": error_message,
            "raw_data": raw_data,
        }

        # Write to file with error handling
        try:
            with filepath.open("w", encoding="utf-8") as f:
                json.dump(quarantine_data, f, indent=2, default=str, ensure_ascii=False)

            self.logger.info(
                "Data quarantined",
                entity_id=entity_id,
                quarantine_path=str(filepath),
            )

        except OSError as e:
            self.logger.warning(
                "Failed to write quarantine file",
                entity_id=entity_id,
                error=str(e),
                attempted_path=str(filepath),
            )
            # Return path even if write failed for error tracking
        except (TypeError, ValueError) as e:
            self.logger.warning(
                "Failed to serialize quarantine data",
                entity_id=entity_id,
                error=str(e),
                attempted_path=str(filepath),
            )
            # Try to write without the problematic data
            try:
                minimal_data = {
                    "entity_id": entity_id,
                    "entity_type": self.entity_type,
                    "timestamp": datetime.now(UTC).isoformat(),
                    "error": error_message,
                    "raw_data": {"serialization_failed": str(e)},
                }
                with filepath.open("w", encoding="utf-8") as f:
                    json.dump(minimal_data, f, indent=2, default=str, ensure_ascii=False)
            except Exception as log_e:
                # If even minimal write fails, log and return the path
                self.logger.warning(
                    "Failed to write minimal quarantine data",
                    entity_id=entity_id,
                    error=str(log_e),
                )

        return filepath
