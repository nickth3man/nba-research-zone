"""Registry for data ingestors."""

from typing import Type

from nba_vault.ingestion.base import BaseIngestor

# Registry of ingestor classes
_INGESTOR_REGISTRY: dict[str, Type[BaseIngestor]] = {}


def register_ingestor(cls: Type[BaseIngestor]) -> Type[BaseIngestor]:
    """
    Register an ingestor class.

    This is intended to be used as a decorator.

    Args:
        cls: Ingestor class to register.

    Returns:
        The same class (for decorator chaining).

    Example:
        @register_ingestor
        class PlayerIngestor(BaseIngestor):
            entity_type = "player"
            ...
    """
    if not hasattr(cls, "entity_type"):
        raise ValueError(f"Ingestor class {cls.__name__} must define 'entity_type'")

    _INGESTOR_REGISTRY[cls.entity_type] = cls
    return cls


def get_ingestor(entity_type: str) -> Type[BaseIngestor] | None:
    """
    Get an ingestor class by entity type.

    Args:
        entity_type: Type of entity (e.g., "player", "game").

    Returns:
        Ingestor class if found, None otherwise.
    """
    return _INGESTOR_REGISTRY.get(entity_type)


def list_ingestors() -> list[str]:
    """
    List all registered ingestor types.

    Returns:
        List of entity_type strings.
    """
    return list(_INGESTOR_REGISTRY.keys())


def create_ingestor(entity_type: str, **kwargs) -> BaseIngestor | None:
    """
    Create an ingestor instance by entity type.

    Args:
        entity_type: Type of entity.
        **kwargs: Arguments to pass to ingestor constructor.

    Returns:
        Ingestor instance if found, None otherwise.
    """
    ingestor_class = get_ingestor(entity_type)
    if ingestor_class is None:
        return None
    return ingestor_class(**kwargs)
