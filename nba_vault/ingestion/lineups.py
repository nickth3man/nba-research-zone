"""Lineup data ingestor.

This ingestor fetches lineup combination data from NBA.com Stats API,
including performance metrics for specific player combinations.
"""

import hashlib
import sqlite3
from typing import Any, cast

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.models.advanced_stats import LineupCreate

logger = structlog.get_logger(__name__)


def generate_lineup_id(
    player_1_id: int,
    player_2_id: int,
    player_3_id: int,
    player_4_id: int,
    player_5_id: int,
) -> str:
    """
    Generate a unique lineup ID from player IDs.

    The lineup ID is a hash of the sorted player IDs to ensure
    uniqueness regardless of the order of players.

    Args:
        player_1_id through player_5_id: Player IDs in the lineup.

    Returns:
        A unique lineup identifier string.
    """
    # Sort player IDs to ensure consistent ID generation
    players = sorted([player_1_id, player_2_id, player_3_id, player_4_id, player_5_id])
    players_str = "_".join(str(p) for p in players)

    # Generate hash
    return hashlib.sha256(players_str.encode()).hexdigest()


@register_ingestor
class LineupsIngestor(BaseIngestor):
    """
    Ingestor for lineup data from NBA.com Stats API.

    Supports fetching lineup data for specific teams or the entire league.
    Lineup data includes:
    - Player combinations
    - Minutes played together
    - Offensive, defensive, and net ratings
    - Points scored and allowed
    - Possessions

    Note: Lineup data availability varies by season.
    """

    entity_type = "lineups"

    def __init__(self, cache=None, rate_limiter=None):
        """
        Initialize LineupsIngestor.

        Args:
            cache: Content cache for API responses.
            rate_limiter: Rate limiter for API requests.
        """
        super().__init__(cache, rate_limiter)
        self.nba_client = NBAStatsClient(cache, rate_limiter)

    def fetch(
        self,
        entity_id: str,
        season: str = "2023-24",
        season_type: str = "Regular Season",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Fetch lineup data from NBA.com Stats API.

        Args:
            entity_id: Team ID (as string), "league" for all teams, or "game:<game_id>" for specific game.
            season: Season in format "YYYY-YY" (e.g., "2023-24").
            season_type: "Regular Season", "Playoffs", or "Pre Season".
            **kwargs: Additional parameters for the API request.

        Returns:
            Dictionary with lineup data.

        Raises:
            Exception: If fetch fails after retries.
        """
        if entity_id == "league":
            # Fetch all lineups in the league
            self.logger.info("Fetching all lineups for league", season=season)

            data = self.nba_client.get_all_lineups(
                season=season,
                season_type=season_type,
                **kwargs,
            )

            return {
                "scope": "league",
                "season": season,
                "season_type": season_type,
                "data": data,
            }

        elif entity_id.startswith("game:"):
            # Fetch lineups for a specific game
            game_id = entity_id.split(":")[1]
            self.logger.info("Fetching lineups for game", game_id=game_id)

            # Game-specific lineup data requires different endpoint
            # For now, return placeholder
            return {
                "scope": "game",
                "game_id": game_id,
                "season": season,
                "season_type": season_type,
                "data": {},
            }

        else:
            # Fetch lineups for a specific team
            team_id = int(entity_id)
            self.logger.info("Fetching lineups for team", team_id=team_id, season=season)

            data = self.nba_client.get_team_lineups(
                team_id=team_id,
                season=season,
                season_type=season_type,
                **kwargs,
            )

            return {
                "scope": "team",
                "team_id": team_id,
                "season": season,
                "season_type": season_type,
                "data": data,
            }

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        """
        Validate raw lineup data using Pydantic models.

        Args:
            raw: Raw data dictionary from NBA.com API.

        Returns:
            List of validated LineupCreate models.

        Raises:
            pydantic.ValidationError: If validation fails.
        """
        data = raw.get("data", {})
        season: str = raw.get("season") or "2023-24"
        team_id: int | None = raw.get("team_id")

        validated_lineups: list[pydantic.BaseModel] = []

        # Extract season_id from season string (e.g., "2023-24" -> 2023)
        season_year = int(season.split("-", maxsplit=1)[0])
        season_id = season_year

        # Process lineup stats data
        for _, dataset_data in data.items():
            if not isinstance(dataset_data, dict):
                continue

            data_rows = dataset_data.get("data", [])
            headers = dataset_data.get("headers", [])

            for row in data_rows:
                try:
                    # Map row data to field names using headers
                    row_dict = dict(zip(headers, row, strict=False)) if headers else {}

                    # Extract player IDs from the lineup
                    # NBA.com provides player IDs in separate fields or in a combined format
                    player_ids = self._extract_player_ids(row_dict)

                    if len(player_ids) != 5:
                        self.logger.warning(
                            "Skipping lineup: expected 5 player IDs",
                            found=len(player_ids),
                            row_data=row_dict,
                        )
                        continue

                    # Generate lineup ID
                    lineup_id = generate_lineup_id(*player_ids)

                    # Extract team ID from row or use the team_id from request
                    row_team_id = self._safe_int(row_dict.get("TEAM_ID")) or team_id
                    if row_team_id is None:
                        self.logger.warning(
                            "Skipping lineup: could not determine team_id",
                            row_data=row_dict,
                        )
                        continue

                    minutes = cast("float | None", self._safe_float(row_dict.get("MIN")))
                    # Only add if we have meaningful data
                    if not minutes or minutes <= 0:
                        continue

                    validated_lineup = LineupCreate(
                        lineup_id=lineup_id,
                        season_id=season_id,
                        team_id=int(row_team_id),
                        player_1_id=player_ids[0],
                        player_2_id=player_ids[1],
                        player_3_id=player_ids[2],
                        player_4_id=player_ids[3],
                        player_5_id=player_ids[4],
                        minutes_played=minutes,
                        possessions=cast("int", self._safe_int(row_dict.get("POSS")) or 0),
                        points_scored=cast("int", self._safe_int(row_dict.get("PTS")) or 0),
                        points_allowed=cast(
                            "int",
                            self._safe_int(row_dict.get("PTS_ALLOWED", row_dict.get("OPP_PTS")))
                            or 0,
                        ),
                        off_rating=cast(
                            "float | None", self._safe_float(row_dict.get("OFF_RATING"))
                        ),
                        def_rating=cast(
                            "float | None", self._safe_float(row_dict.get("DEF_RATING"))
                        ),
                        net_rating=cast(
                            "float | None", self._safe_float(row_dict.get("NET_RATING"))
                        ),
                    )
                    validated_lineups.append(validated_lineup)

                except pydantic.ValidationError as e:
                    self.logger.error(
                        "Lineup validation failed",
                        row_data=row_dict,
                        errors=str(e),
                    )
                    raise

        self.logger.info("Validated lineups", count=len(validated_lineups))
        return validated_lineups

    def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
        """
        Insert or update validated lineup data in database.

        Args:
            model: List of validated LineupCreate models.
            conn: SQLite database connection.

        Returns:
            Number of rows affected.
        """
        rows_affected = 0

        try:
            conn.execute("BEGIN")

            for lineup in model:
                if not isinstance(lineup, LineupCreate):
                    continue

                # Check if lineup exists
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) FROM lineup
                    WHERE lineup_id = ? AND season_id = ?
                    """,
                    (lineup.lineup_id, lineup.season_id),
                )
                exists = cursor.fetchone()[0] > 0

                if exists:
                    # Update existing lineup
                    self._update_lineup(lineup, conn)
                    rows_affected += 1
                else:
                    # Insert new lineup
                    self._insert_lineup(lineup, conn)
                    rows_affected += 1

                # Log to ingestion_audit
                conn.execute(
                    """
                    INSERT INTO ingestion_audit
                    (entity_type, entity_id, status, source, metadata, ingested_at)
                    VALUES (?, ?, 'SUCCESS', 'nba_stats_api', ?, datetime('now'))
                    """,
                    (
                        self.entity_type,
                        lineup.lineup_id,
                        f"season: {lineup.season_id}, team: {lineup.team_id}",
                    ),
                )

            conn.execute("COMMIT")

        except sqlite3.IntegrityError as exc:
            conn.execute("ROLLBACK")
            self.logger.warning(
                "Integrity error during lineup upsert",
                rows_before_error=rows_affected,
                error=str(exc),
            )
            raise
        except sqlite3.OperationalError as exc:
            conn.execute("ROLLBACK")
            self.logger.error(
                "Operational error during lineup upsert",
                rows_before_error=rows_affected,
                error=str(exc),
            )
            raise

        return rows_affected

    def _insert_lineup(self, lineup: LineupCreate, conn) -> None:
        """
        Insert a new lineup into the database.

        Args:
            lineup: LineupCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            INSERT INTO lineup (
                lineup_id, season_id, team_id, player_1_id, player_2_id,
                player_3_id, player_4_id, player_5_id, minutes_played,
                possessions, points_scored, points_allowed, off_rating,
                def_rating, net_rating
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lineup.lineup_id,
                lineup.season_id,
                lineup.team_id,
                lineup.player_1_id,
                lineup.player_2_id,
                lineup.player_3_id,
                lineup.player_4_id,
                lineup.player_5_id,
                lineup.minutes_played,
                lineup.possessions,
                lineup.points_scored,
                lineup.points_allowed,
                lineup.off_rating,
                lineup.def_rating,
                lineup.net_rating,
            ),
        )

    def _update_lineup(self, lineup: LineupCreate, conn) -> None:
        """
        Update an existing lineup in the database.

        Args:
            lineup: LineupCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            UPDATE lineup SET
                team_id = ?, player_1_id = ?, player_2_id = ?,
                player_3_id = ?, player_4_id = ?, player_5_id = ?,
                minutes_played = ?, possessions = ?, points_scored = ?,
                points_allowed = ?, off_rating = ?, def_rating = ?, net_rating = ?
            WHERE lineup_id = ? AND season_id = ?
            """,
            (
                lineup.team_id,
                lineup.player_1_id,
                lineup.player_2_id,
                lineup.player_3_id,
                lineup.player_4_id,
                lineup.player_5_id,
                lineup.minutes_played,
                lineup.possessions,
                lineup.points_scored,
                lineup.points_allowed,
                lineup.off_rating,
                lineup.def_rating,
                lineup.net_rating,
                lineup.lineup_id,
                lineup.season_id,
            ),
        )

    def _extract_player_ids(self, row_dict: dict[str, Any]) -> list[int]:
        """
        Extract player IDs from a lineup data row.

        NBA.com API provides player IDs in various formats depending on the endpoint.
        This method handles the common formats.

        Args:
            row_dict: Dictionary of row data from the API.

        Returns:
            List of 5 player IDs.
        """
        player_ids = []

        # Try common field names for player IDs
        for i in range(1, 6):
            player_id_field = f"PLAYER_ID_{i}"
            if row_dict.get(player_id_field):
                player_id = self._safe_int(row_dict[player_id_field])
                if player_id:
                    player_ids.append(player_id)

        # If we didn't get all 5 players, try alternative format
        if len(player_ids) < 5:
            # Some endpoints return lineup as a combined string
            # Format: "123/456/789/012/345"  # noqa: ERA001
            lineup_str = row_dict.get("LINEUP", "")
            if lineup_str and "/" in lineup_str:
                ids = lineup_str.split("/")
                player_ids = [self._safe_int(pid) for pid in ids if pid][:5]

        # Filter out None values
        player_ids = [pid for pid in player_ids if pid is not None]

        return player_ids[:5]  # Ensure we only return 5 players max

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        """Safely convert value to float, returning None for empty/invalid values."""
        if value is None or value in {"", "-"}:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        """Safely convert value to int, returning None for empty/invalid values."""
        if value is None or value in {"", "-"}:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
