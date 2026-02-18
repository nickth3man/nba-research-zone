"""Command-line interface for NBA Vault."""

from pathlib import Path

import structlog
import typer

from nba_vault.duckdb.builder import build_duckdb_database
from nba_vault.schema.connection import get_db_connection, init_database
from nba_vault.schema.migrations import rollback_migration
from nba_vault.utils.config import ensure_directories, get_settings
from nba_vault.utils.logging import setup_logging

app = typer.Typer(
    name="nba-vault",
    help="NBA Vault - Comprehensive Historical NBA Database",
    add_completion=False,
)

# Ensure directories and logging are set up
ensure_directories()
setup_logging()
logger = structlog.get_logger(__name__)


@app.command()
def init(
    db_path: Path = typer.Option(
        None,
        "--db-path",
        help="Path to SQLite database file",
        envvar="DB_PATH",
    ),
) -> None:
    """
    Initialize the database schema.

    Creates the database file and runs all pending migrations.
    """
    logger.info("Initializing database", db_path=str(db_path))
    try:
        init_database(db_path)
        typer.echo(f"✓ Database initialized at {db_path or 'nba.sqlite'}")
    except Exception as e:
        logger.error("Failed to initialize database", error=str(e))
        typer.echo(f"✗ Failed to initialize database: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command()
def migrate(
    rollback: bool = typer.Option(
        False,
        "--rollback",
        "-r",
        help="Rollback the most recent migration",
    ),
    steps: int = typer.Option(
        1,
        "--steps",
        "-n",
        help="Number of migrations to rollback",
    ),
) -> None:
    """
    Run database migrations.

    Applies pending migrations by default. Use --rollback to undo migrations.
    """
    conn = get_db_connection()

    try:
        if rollback:
            logger.info("Rolling back migrations", steps=steps)
            rollback_migration(conn, steps)
            typer.echo(f"✓ Rolled back {steps} migration(s)")
        else:
            from nba_vault.schema.migrations import run_migrations

            logger.info("Running migrations")
            run_migrations(conn)
            typer.echo("✓ Migrations applied successfully")
    except Exception as e:
        logger.error("Migration failed", error=str(e))
        typer.echo(f"✗ Migration failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@app.command()
def ingest(
    mode: str = typer.Option(
        "incremental",
        "--mode",
        "-m",
        help="Ingestion mode: 'incremental' or 'full'",
    ),
    workers: int = typer.Option(
        1,
        "--workers",
        "-w",
        help="Number of parallel workers (for full backfill)",
    ),
    start_season: int = typer.Option(
        None,
        "--start-season",
        help="Start season for backfill (e.g., 2024)",
    ),
    end_season: int = typer.Option(
        None,
        "--end-season",
        help="End season for backfill (e.g., 1946)",
    ),
) -> None:
    """
    Ingest NBA data from stats.nba.com and other sources.

    By default, performs incremental update for recent games.
    Use --mode full for complete historical backfill.
    """
    settings = get_settings()

    # Set defaults from settings if not provided
    if mode == "full":
        start_season = start_season or settings.backfill_start_season
        end_season = end_season or settings.backfill_end_season
        workers = workers or settings.backfill_workers

    logger.info(
        "Starting ingestion",
        mode=mode,
        workers=workers,
        start_season=start_season,
        end_season=end_season,
    )

    try:
        if mode == "full":
            typer.echo(
                f"Starting full historical backfill from {start_season} to {end_season} "
                f"using {workers} worker(s)..."
            )
            # TODO: Implement full backfill logic
            typer.echo("✗ Full backfill not yet implemented", err=True)
            raise typer.Exit(code=1)
        else:
            typer.echo("Starting incremental update...")
            # TODO: Implement incremental update logic
            typer.echo("✗ Incremental update not yet implemented", err=True)
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Ingestion failed", error=str(e))
        typer.echo(f"✗ Ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command()
def ingest_players(
    season_end_year: int = typer.Option(
        None,
        "--season-end-year",
        "-s",
        help="Season end year (e.g., 2024 for 2023-24 season)",
    ),
    player_id: str = typer.Option(
        None,
        "--player-id",
        "-p",
        help="Basketball Reference player slug (e.g., 'jamesle01')",
    ),
) -> None:
    """
    Ingest player data from Basketball Reference.

    Fetches all players from a season by default.
    Use --player-id to fetch a specific player.
    """
    from nba_vault.ingestion import create_ingestor

    conn = get_db_connection()

    try:
        # Create ingestor
        ingestor = create_ingestor("players")

        if ingestor is None:
            typer.echo("✗ Players ingestor not found", err=True)
            raise typer.Exit(code=1)

        # Determine what to ingest
        if player_id:
            typer.echo(f"Ingesting player: {player_id}...")
            entity_id = player_id
        else:
            if season_end_year is None:
                season_end_year = 2024  # Default to 2023-24 season
            typer.echo(f"Ingesting players from {season_end_year - 1}-{season_end_year} season...")
            entity_id = "season"

        # Perform ingestion
        result = ingestor.ingest(entity_id, conn, season_end_year=season_end_year)

        # Check result
        if result["status"] == "SUCCESS":
            typer.echo(f"✓ Successfully ingested {result['rows_affected']} player(s)")
        else:
            typer.echo(
                f"✗ Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Player ingestion failed", error=str(e))
        typer.echo(f"✗ Player ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@app.command()
def ingest_tracking(
    player_id: int = typer.Option(
        None,
        "--player-id",
        "-p",
        help="NBA.com player ID",
    ),
    team_id: int = typer.Option(
        None,
        "--team-id",
        "-t",
        help="NBA.com team ID (fetch all players on team)",
    ),
    season: str = typer.Option(
        "2023-24",
        "--season",
        "-s",
        help="Season in format YYYY-YY (e.g., 2023-24)",
    ),
    season_type: str = typer.Option(
        "Regular Season",
        "--season-type",
        help="Season type: Regular Season, Playoffs, or Pre Season",
    ),
) -> None:
    """
    Ingest player tracking data from NBA.com Stats API.

    Tracking data includes speed, distance, touches, drives, and other movement metrics.
    Available from 2013-14 season onwards.

    Use --player-id for a specific player or --team-id for all players on a team.
    """
    from nba_vault.ingestion import create_ingestor

    conn = get_db_connection()

    try:
        # Create ingestor
        ingestor = create_ingestor("player_tracking")

        if ingestor is None:
            typer.echo("✗ Player tracking ingestor not found", err=True)
            raise typer.Exit(code=1)

        # Determine what to ingest
        if player_id:
            typer.echo(f"Ingesting tracking data for player {player_id}...")
            entity_id = str(player_id)
        elif team_id:
            typer.echo(f"Ingesting tracking data for team {team_id}...")
            entity_id = f"team:{team_id}"
        else:
            typer.echo("✗ Must specify --player-id or --team-id", err=True)
            raise typer.Exit(code=1)

        # Perform ingestion
        result = ingestor.ingest(entity_id, conn, season=season, season_type=season_type)

        # Check result
        if result["status"] == "SUCCESS":
            typer.echo(f"✓ Successfully ingested {result['rows_affected']} tracking record(s)")
        else:
            typer.echo(
                f"✗ Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Tracking ingestion failed", error=str(e))
        typer.echo(f"✗ Tracking ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@app.command()
def ingest_lineups(
    team_id: int = typer.Option(
        None,
        "--team-id",
        "-t",
        help="NBA.com team ID",
    ),
    scope: str = typer.Option(
        "league",
        "--scope",
        help="Scope: league, team, or game:<game_id>",
    ),
    season: str = typer.Option(
        "2023-24",
        "--season",
        "-s",
        help="Season in format YYYY-YY (e.g., 2023-24)",
    ),
    season_type: str = typer.Option(
        "Regular Season",
        "--season-type",
        help="Season type: Regular Season, Playoffs, or Pre Season",
    ),
) -> None:
    """
    Ingest lineup data from NBA.com Stats API.

    Lineup data includes player combinations, minutes played, and performance metrics.
    Use --scope=league for all teams, --scope=team:<team_id> for specific team.
    """
    from nba_vault.ingestion import create_ingestor

    conn = get_db_connection()

    try:
        # Create ingestor
        ingestor = create_ingestor("lineups")

        if ingestor is None:
            typer.echo("✗ Lineups ingestor not found", err=True)
            raise typer.Exit(code=1)

        # Determine entity_id from scope
        if scope == "league":
            entity_id = "league"
        elif scope.startswith("team:") or scope.startswith("game:"):
            entity_id = scope
        elif team_id:
            entity_id = str(team_id)
        else:
            entity_id = "league"

        typer.echo(f"Ingesting lineup data for {entity_id}...")

        # Perform ingestion
        result = ingestor.ingest(entity_id, conn, season=season, season_type=season_type)

        # Check result
        if result["status"] == "SUCCESS":
            typer.echo(f"✓ Successfully ingested {result['rows_affected']} lineup(s)")
        else:
            typer.echo(
                f"✗ Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Lineup ingestion failed", error=str(e))
        typer.echo(f"✗ Lineup ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@app.command()
def ingest_team_other_stats(
    game_id: str = typer.Option(
        None,
        "--game-id",
        "-g",
        help="NBA.com 10-character game ID",
    ),
    team_id: int = typer.Option(
        None,
        "--team-id",
        "-t",
        help="NBA.com team ID (format: team:<id>:<season>)",
    ),
    season: str = typer.Option(
        "2023-24",
        "--season",
        "-s",
        help="Season in format YYYY-YY (e.g., 2023-24)",
    ),
) -> None:
    """
    Ingest team game other stats from NBA.com Stats API.

    Other stats include paint points, fast break points, second chance points, etc.
    Use --game-id for a specific game or --team-id for all games in a season.
    """
    from nba_vault.ingestion import create_ingestor

    conn = get_db_connection()

    try:
        # Create ingestor
        ingestor = create_ingestor("team_other_stats")

        if ingestor is None:
            typer.echo("✗ Team other stats ingestor not found", err=True)
            raise typer.Exit(code=1)

        # Determine what to ingest
        if game_id:
            typer.echo(f"Ingesting other stats for game {game_id}...")
            entity_id = game_id
        elif team_id:
            typer.echo(f"Ingesting other stats for team {team_id}...")
            entity_id = f"team:{team_id}:{season}"
        else:
            typer.echo("✗ Must specify --game-id or --team-id", err=True)
            raise typer.Exit(code=1)

        # Perform ingestion
        result = ingestor.ingest(entity_id, conn, season=season)

        # Check result
        if result["status"] == "SUCCESS":
            typer.echo(f"✓ Successfully ingested {result['rows_affected']} other stats record(s)")
        else:
            typer.echo(
                f"✗ Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Team other stats ingestion failed", error=str(e))
        typer.echo(f"✗ Team other stats ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@app.command()
def ingest_team_advanced_stats(
    team_id: int = typer.Option(
        None,
        "--team-id",
        "-t",
        help="NBA.com team ID",
    ),
    scope: str = typer.Option(
        "league",
        "--scope",
        help="Scope: league or team",
    ),
    season: str = typer.Option(
        "2023-24",
        "--season",
        "-s",
        help="Season in format YYYY-YY (e.g., 2023-24)",
    ),
    season_type: str = typer.Option(
        "Regular Season",
        "--season-type",
        help="Season type: Regular Season, Playoffs, or Pre Season",
    ),
    measure_type: str = typer.Option(
        "Advanced",
        "--measure-type",
        help="Measure type: Base, Advanced, or Four Factors",
    ),
) -> None:
    """
    Ingest advanced team stats from NBA.com Stats API.

    Advanced stats include offensive/defensive ratings, pace, four factors, etc.
    Use --scope=league for all teams or --scope=team for specific team.
    """
    from nba_vault.ingestion import create_ingestor

    conn = get_db_connection()

    try:
        # Create ingestor
        ingestor = create_ingestor("team_advanced_stats")

        if ingestor is None:
            typer.echo("✗ Team advanced stats ingestor not found", err=True)
            raise typer.Exit(code=1)

        # Determine entity_id from scope
        if scope == "league":
            entity_id = "league"
        elif team_id:
            entity_id = str(team_id)
        else:
            entity_id = "league"

        typer.echo(f"Ingesting advanced stats for {entity_id}...")

        # Perform ingestion
        result = ingestor.ingest(
            entity_id, conn, season=season, season_type=season_type, measure_type=measure_type
        )

        # Check result
        if result["status"] == "SUCCESS":
            typer.echo(
                f"✓ Successfully ingested {result['rows_affected']} advanced stats record(s)"
            )
        else:
            typer.echo(
                f"✗ Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Team advanced stats ingestion failed", error=str(e))
        typer.echo(f"✗ Team advanced stats ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@app.command()
def ingest_injuries(
    team: str = typer.Option(
        None,
        "--team",
        "-t",
        help="Team abbreviation (e.g., LAL)",
    ),
    source: str = typer.Option(
        "espn",
        "--source",
        help="Data source: espn, rotowire, or nba",
    ),
) -> None:
    """
    Ingest injury data from various sources.

    Fetches current injury reports from ESPN, Rotowire, or NBA.com.
    Use --team to filter by specific team.
    """
    from nba_vault.ingestion import create_ingestor

    conn = get_db_connection()

    try:
        # Create ingestor
        ingestor = create_ingestor("injuries")

        if ingestor is None:
            typer.echo("✗ Injury ingestor not found", err=True)
            raise typer.Exit(code=1)

        # Determine entity_id
        if team:
            entity_id = f"team:{team}"
            typer.echo(f"Ingesting injuries for {team} from {source}...")
        else:
            entity_id = "all"
            typer.echo(f"Ingesting all injuries from {source}...")

        # Perform ingestion
        result = ingestor.ingest(entity_id, conn, source=source)

        # Check result
        if result["status"] == "SUCCESS":
            typer.echo(f"✓ Successfully ingested {result['rows_affected']} injury record(s)")
        else:
            typer.echo(
                f"✗ Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Injury ingestion failed", error=str(e))
        typer.echo(f"✗ Injury ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@app.command()
def ingest_contracts(
    team: str = typer.Option(
        None,
        "--team",
        "-t",
        help="Team identifier (name or abbreviation)",
    ),
    source: str = typer.Option(
        "realgm",
        "--source",
        help="Data source: realgm or spotrac",
    ),
) -> None:
    """
    Ingest player contract data from various sources.

    Fetches contract information including salary, contract type, and options.
    Use --team to filter by specific team.
    """
    from nba_vault.ingestion import create_ingestor

    conn = get_db_connection()

    try:
        # Create ingestor
        ingestor = create_ingestor("contracts")

        if ingestor is None:
            typer.echo("✗ Contract ingestor not found", err=True)
            raise typer.Exit(code=1)

        # Determine entity_id
        if team:
            entity_id = f"team:{team}"
            typer.echo(f"Ingesting contracts for {team} from {source}...")
        else:
            entity_id = "all"
            typer.echo(f"Ingesting all contracts from {source}...")

        # Perform ingestion
        result = ingestor.ingest(entity_id, conn, source=source)

        # Check result
        if result["status"] == "SUCCESS":
            typer.echo(f"✓ Successfully ingested {result['rows_affected']} contract record(s)")
        else:
            typer.echo(
                f"✗ Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Contract ingestion failed", error=str(e))
        typer.echo(f"✗ Contract ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@app.command()
def validate(
    checks: list[str] = typer.Option(
        None,
        "--check",
        "-c",
        help="Specific validation checks to run (default: all)",
    ),
) -> None:
    """
    Validate database integrity and data completeness.

    Runs all validation checks by default. Use --check to run specific checks.
    Available checks: fk_integrity, game_coverage, data_availability, schema_version.
    """
    logger.info("Running validation", checks=checks or ["all"])

    conn = get_db_connection()
    try:
        # TODO: Implement validation logic
        typer.echo("✗ Validation not yet implemented", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error("Validation failed", error=str(e))
        typer.echo(f"✗ Validation failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@app.command()
def export(
    format: str = typer.Option(
        "parquet",
        "--format",
        "-f",
        help="Export format: parquet, csv, or duckdb",
    ),
    output_dir: Path = typer.Option(
        Path("exports"),
        "--output-dir",
        "-o",
        help="Output directory for exported files",
    ),
    entities: list[str] = typer.Option(
        None,
        "--entity",
        "-e",
        help="Specific entities to export (default: all)",
    ),
) -> None:
    """
    Export database to various formats.

    Exports all tables by default. Use --entity to export specific tables.
    """
    logger.info(
        "Starting export",
        format=format,
        output_dir=str(output_dir),
        entities=entities or ["all"],
    )

    try:
        if format == "duckdb":
            typer.echo("Building DuckDB analytical database...")
            build_duckdb_database()
            typer.echo("✓ DuckDB database built successfully")
        else:
            # TODO: Implement export logic for other formats
            typer.echo(f"✗ Export format '{format}' not yet implemented", err=True)
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Export failed", error=str(e))
        typer.echo(f"✗ Export failed: {e}", err=True)
        raise typer.Exit(code=1) from e


@app.command()
def status() -> None:
    """Show database status and statistics."""
    settings = get_settings()
    db_path = Path(settings.db_path)

    if not db_path.exists():
        typer.echo("Database not found. Run 'nba-vault init' to create it.")
        raise typer.Exit(code=1)

    conn = get_db_connection()
    try:
        # Get basic statistics
        cursor = conn.execute(
            """
            SELECT name, (SELECT COUNT(*) FROM sqlite_master WHERE name=main.name) as has_data
            FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        )

        tables = cursor.fetchall()
        typer.echo(f"\n📊 Database Status: {db_path}")
        typer.echo(f"   Size: {db_path.stat().st_size / (1024 * 1024):.1f} MB\n")
        typer.echo("Tables:")

        for table_name, has_data in tables:
            if has_data:
                count_cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")  # noqa: S608
                count = count_cursor.fetchone()[0]
                typer.echo(f"  • {table_name}: {count:,} rows")
            else:
                typer.echo(f"  • {table_name}: (empty)")

        # Get ingestion audit stats
        audit_cursor = conn.execute(
            """
            SELECT
                entity_type,
                status,
                COUNT(*) as count
            FROM ingestion_audit
            GROUP BY entity_type, status
            ORDER BY entity_type, status
            """
        )

        typer.echo("\nIngestion Status:")
        for row in audit_cursor.fetchall():
            entity_type, status, count = row
            emoji = "✓" if status == "SUCCESS" else "✗"
            typer.echo(f"  {emoji} {entity_type}: {count:,} {status}")

    except Exception as e:
        logger.error("Failed to get status", error=str(e))
        typer.echo(f"✗ Failed to get status: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


def main() -> None:
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()
