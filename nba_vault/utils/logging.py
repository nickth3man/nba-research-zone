"""Structured logging configuration."""

from __future__ import annotations

import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from structlog.types import Processor

from nba_vault.utils.config import get_settings

# Module-level reference so callers can find the active log file path.
_active_log_file: Path | None = None


def get_active_log_file() -> Path | None:
    """Return the path of the log file opened by the current process, if any."""
    return _active_log_file


def setup_logging() -> None:
    """Configure structured logging for the application.

    Output targets
    --------------
    1. **stdout** - console renderer (or JSON if ``log_format=json``).
    2. **logs/nba_vault_YYYYMMDD_HHMMSS.log** - JSON lines, always, for
       machine-readable post-mortems and the pipeline audit trail.
    """
    global _active_log_file  # noqa: PLW0603

    settings = get_settings()

    # ------------------------------------------------------------------
    # 1. Ensure log directory exists
    # ------------------------------------------------------------------
    log_dir = Path(settings.log_dir)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except (PermissionError, OSError) as e:
        print(
            f"Warning: Could not create log directory '{log_dir}': {e}. "
            "Falling back to stdout-only logging.",
            file=sys.stderr,
        )

    # ------------------------------------------------------------------
    # 2. Build Python stdlib root logger with two handlers
    #    structlog will route through it via LoggerFactory.
    # ------------------------------------------------------------------
    numeric_level = getattr(logging, settings.log_level, logging.INFO)
    root = logging.getLogger()
    root.setLevel(numeric_level)

    # Remove any handlers added by a previous call (e.g. pytest re-invocations).
    for h in root.handlers[:]:
        root.removeHandler(h)

    # --- Console handler ------------------------------------------------
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)

    # --- File handler ---------------------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"nba_vault_{timestamp}.log"
    file_handler: logging.FileHandler | None = None
    try:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(numeric_level)
        _active_log_file = log_file
    except (PermissionError, OSError) as e:
        print(
            f"Warning: Could not open log file '{log_file}': {e}. File logging disabled.",
            file=sys.stderr,
        )
        _active_log_file = None

    # ------------------------------------------------------------------
    # 3. Define shared pre-render processors
    # ------------------------------------------------------------------
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        # Renders exc_info tracebacks into the event dict before serialization.
        # Must come before wrap_for_formatter so stdlib doesn't double-render.
        structlog.processors.ExceptionRenderer(),
        structlog.processors.UnicodeDecoder(),
        # Required bridge when using stdlib LoggerFactory + ProcessorFormatter
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    # ------------------------------------------------------------------
    # 4. Attach ProcessorFormatter to each handler
    #    - Console: ConsoleRenderer or JSONRenderer per log_format setting
    #    - File:    always JSONRenderer (machine-readable)
    # ------------------------------------------------------------------
    console_renderer: Processor
    if settings.log_format == "json":
        console_renderer = structlog.processors.JSONRenderer()
    else:
        console_renderer = structlog.dev.ConsoleRenderer()

    console_handler.setFormatter(structlog.stdlib.ProcessorFormatter(processor=console_renderer))
    root.addHandler(console_handler)

    if file_handler is not None:
        file_handler.setFormatter(
            structlog.stdlib.ProcessorFormatter(processor=structlog.processors.JSONRenderer())
        )
        root.addHandler(file_handler)

    # ------------------------------------------------------------------
    # 5. Configure structlog to route through stdlib
    # ------------------------------------------------------------------
    structlog.configure(
        processors=shared_processors,
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Announce where the log file landed (visible in the console immediately).
    if _active_log_file is not None:
        log = structlog.get_logger(__name__)
        log.info("logging_initialized", log_file=str(_active_log_file))


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a structured logger instance.

    Args:
        name: Logger name (typically __name__ of the module).

    Returns:
        Configured bound logger.
    """
    return structlog.get_logger(name).bind(logger=name)


def log_context(**kwargs: Any) -> None:
    """
    Add contextual information to all log messages.

    Args:
        **kwargs: Key-value pairs to add to log context.
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_log_context(*keys: str) -> None:
    """
    Clear contextual information from logs.

    Args:
        *keys: Keys to remove from context. If none provided, clears all.
    """
    if keys:
        structlog.contextvars.unbind_contextvars(*keys)
    else:
        structlog.contextvars.clear_contextvars()
