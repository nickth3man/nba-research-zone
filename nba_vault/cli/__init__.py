"""Command-line interface for NBA Vault."""

import typer

from nba_vault.utils.config import ensure_directories
from nba_vault.utils.logging import setup_logging

from .admin import admin_app
from .advanced_stats import advanced_stats_app
from .export import export_app
from .game_data import game_data_app
from .ingestion import ingestion_app
from .scrapers import scrapers_app

ensure_directories()
setup_logging()

app = typer.Typer(
    name="nba-vault",
    help="NBA Vault - Comprehensive Historical NBA Database",
    add_completion=False,
)

app.add_typer(admin_app, name="admin")
app.add_typer(ingestion_app, name="ingestion")
app.add_typer(advanced_stats_app, name="advanced-stats")
app.add_typer(game_data_app, name="game-data")
app.add_typer(scrapers_app, name="scrapers")
app.add_typer(export_app, name="export")


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
