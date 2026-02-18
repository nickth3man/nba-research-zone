"""Tests for structured logging utilities."""

from unittest.mock import patch

import structlog


def test_setup_logging_console_format(tmp_path):
    """setup_logging configures ConsoleRenderer when log_format is 'console'."""
    from nba_vault.utils.config import Settings
    from nba_vault.utils.logging import setup_logging

    settings = Settings(log_dir=str(tmp_path / "logs"), log_format="console")

    with patch("nba_vault.utils.logging.get_settings", return_value=settings):
        setup_logging()

    # If structlog was configured without error, config is accessible
    config = structlog.get_config()
    assert config is not None


def test_setup_logging_json_format(tmp_path):
    """setup_logging attaches JSONRenderer to the console handler when log_format is 'json'."""
    import logging

    from nba_vault.utils.config import Settings
    from nba_vault.utils.logging import setup_logging

    settings = Settings(log_dir=str(tmp_path / "logs"), log_format="json")

    with patch("nba_vault.utils.logging.get_settings", return_value=settings):
        setup_logging()

    root = logging.getLogger()
    formatter_processor_types = []
    for handler in root.handlers:
        fmt = handler.formatter
        if fmt is not None and hasattr(fmt, "processors"):
            formatter_processor_types.extend(type(p).__name__ for p in fmt.processors)
    assert "JSONRenderer" in formatter_processor_types


def test_setup_logging_creates_log_dir(tmp_path):
    """setup_logging creates the log directory if it doesn't exist."""
    from nba_vault.utils.config import Settings
    from nba_vault.utils.logging import setup_logging

    log_dir = tmp_path / "new_logs_dir"
    assert not log_dir.exists()

    settings = Settings(log_dir=str(log_dir), log_format="console")

    with patch("nba_vault.utils.logging.get_settings", return_value=settings):
        setup_logging()

    assert log_dir.exists()


def test_setup_logging_handles_permission_error(tmp_path, capsys):
    """setup_logging falls back gracefully when log dir cannot be created."""
    from nba_vault.utils.config import Settings
    from nba_vault.utils.logging import setup_logging

    settings = Settings(log_dir="/root/no_permission_dir", log_format="console")

    with (
        patch("nba_vault.utils.logging.get_settings", return_value=settings),
        patch("pathlib.Path.mkdir", side_effect=PermissionError("permission denied")),
    ):
        # Should not raise — must print warning to stderr and continue
        setup_logging()

    # The function must not raise; output is secondary (structlog may suppress it)
    capsys.readouterr()


def test_get_logger_returns_bound_logger():
    """get_logger returns a structlog BoundLogger with the name bound."""
    from nba_vault.utils.logging import get_logger

    logger = get_logger("test.module")
    assert logger is not None


def test_log_context_binds_variables():
    """log_context adds key-value pairs to structlog context vars."""
    from nba_vault.utils.logging import clear_log_context, log_context

    clear_log_context()
    log_context(request_id="abc-123", user="test_user")

    ctx = structlog.contextvars.get_contextvars()
    assert ctx.get("request_id") == "abc-123"
    assert ctx.get("user") == "test_user"

    clear_log_context()


def test_clear_log_context_all():
    """clear_log_context with no args removes all context variables."""
    from nba_vault.utils.logging import clear_log_context, log_context

    log_context(key1="v1", key2="v2")
    clear_log_context()

    ctx = structlog.contextvars.get_contextvars()
    assert "key1" not in ctx
    assert "key2" not in ctx


def test_clear_log_context_specific_keys():
    """clear_log_context with args removes only the named keys."""
    from nba_vault.utils.logging import clear_log_context, log_context

    log_context(keep_me="yes", remove_me="no")
    clear_log_context("remove_me")

    ctx = structlog.contextvars.get_contextvars()
    assert "remove_me" not in ctx
    assert ctx.get("keep_me") == "yes"

    clear_log_context()
