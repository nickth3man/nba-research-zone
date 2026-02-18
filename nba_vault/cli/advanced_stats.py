"""Advanced stats commands: ingest-tracking, ingest-lineups, ingest-team-other-stats, ingest-team-advanced-stats."""

import structlog
import typer

from nba_vault.schema.connection import get_db_connection

advanced_stats_app = typer.Typer(help="NBA.com advanced statistics ingestion commands.")

logger = structlog.get_logger(__name__)


@advanced_stats_app.command(name="ingest-tracking")
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

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        ingestor = create_ingestor("player_tracking")

        if ingestor is None:
            typer.echo("[FAIL] Player tracking ingestor not found", err=True)
            raise typer.Exit(code=1)

        if player_id:
            typer.echo(f"Ingesting tracking data for player {player_id}...")
            entity_id = str(player_id)
        elif team_id:
            typer.echo(f"Ingesting tracking data for team {team_id}...")
            entity_id = f"team:{team_id}"
        else:
            typer.echo("[FAIL] Must specify --player-id or --team-id", err=True)
            raise typer.Exit(code=1)

        result = ingestor.ingest(entity_id, conn, season=season, season_type=season_type)

        if result.get("status") == "SUCCESS":
            typer.echo(
                f"[OK] Successfully ingested {result.get('rows_affected', 0)} tracking record(s)"
            )
        else:
            typer.echo(
                f"[FAIL] Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Tracking ingestion failed", error=str(e))
        typer.echo(f"[FAIL] Tracking ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@advanced_stats_app.command(name="ingest-lineups")
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

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        ingestor = create_ingestor("lineups")

        if ingestor is None:
            typer.echo("[FAIL] Lineups ingestor not found", err=True)
            raise typer.Exit(code=1)

        if scope == "league":
            entity_id = "league"
        elif scope.startswith("team:") or scope.startswith("game:"):
            entity_id = scope
        elif team_id:
            entity_id = str(team_id)
        else:
            entity_id = "league"

        typer.echo(f"Ingesting lineup data for {entity_id}...")

        result = ingestor.ingest(entity_id, conn, season=season, season_type=season_type)

        if result.get("status") == "SUCCESS":
            typer.echo(f"[OK] Successfully ingested {result.get('rows_affected', 0)} lineup(s)")
        else:
            typer.echo(
                f"[FAIL] Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Lineup ingestion failed", error=str(e))
        typer.echo(f"[FAIL] Lineup ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@advanced_stats_app.command(name="ingest-team-other-stats")
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

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        ingestor = create_ingestor("team_other_stats")

        if ingestor is None:
            typer.echo("[FAIL] Team other stats ingestor not found", err=True)
            raise typer.Exit(code=1)

        if game_id:
            typer.echo(f"Ingesting other stats for game {game_id}...")
            entity_id = game_id
        elif team_id:
            typer.echo(f"Ingesting other stats for team {team_id}...")
            entity_id = f"team:{team_id}:{season}"
        else:
            typer.echo("[FAIL] Must specify --game-id or --team-id", err=True)
            raise typer.Exit(code=1)

        result = ingestor.ingest(entity_id, conn, season=season)

        if result.get("status") == "SUCCESS":
            typer.echo(
                f"[OK] Successfully ingested {result.get('rows_affected', 0)} other stats record(s)"
            )
        else:
            typer.echo(
                f"[FAIL] Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Team other stats ingestion failed", error=str(e))
        typer.echo(f"[FAIL] Team other stats ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()


@advanced_stats_app.command(name="ingest-team-advanced-stats")
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

    try:
        conn = get_db_connection()
    except RuntimeError as e:
        logger.error("Cannot open database", error=str(e))
        typer.echo(f"[FAIL] Cannot open database: {e}", err=True)
        raise typer.Exit(code=1) from e

    try:
        ingestor = create_ingestor("team_advanced_stats")

        if ingestor is None:
            typer.echo("[FAIL] Team advanced stats ingestor not found", err=True)
            raise typer.Exit(code=1)

        if scope == "league":
            entity_id = "league"
        elif team_id:
            entity_id = str(team_id)
        else:
            entity_id = "league"

        typer.echo(f"Ingesting advanced stats for {entity_id}...")

        result = ingestor.ingest(
            entity_id, conn, season=season, season_type=season_type, measure_type=measure_type
        )

        if result.get("status") == "SUCCESS":
            typer.echo(
                f"[OK] Successfully ingested {result.get('rows_affected', 0)} advanced stats record(s)"
            )
        else:
            typer.echo(
                f"[FAIL] Ingestion failed: {result.get('error_message', 'Unknown error')}",
                err=True,
            )
            raise typer.Exit(code=1)

    except Exception as e:
        logger.error("Team advanced stats ingestion failed", error=str(e))
        typer.echo(f"[FAIL] Team advanced stats ingestion failed: {e}", err=True)
        raise typer.Exit(code=1) from e
    finally:
        conn.close()
