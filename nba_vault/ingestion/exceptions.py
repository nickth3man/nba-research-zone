"""Domain-specific exceptions for ingestion operations."""


class IngestionError(Exception):
    """Base exception for ingestion failures."""

    pass


class ValidationError(IngestionError):
    """Raised when data validation fails."""

    pass


class APIError(IngestionError):
    """Raised when external API request fails."""

    pass


class RateLimitError(APIError):
    """Raised when API rate limit is exceeded."""

    pass


class QuarantineError(IngestionError):
    """Raised when quarantine write fails."""

    pass


class DatabaseError(IngestionError):
    """Raised when database operation fails."""

    pass
