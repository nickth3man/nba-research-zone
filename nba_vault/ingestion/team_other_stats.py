"""Team other stats ingestor.

This ingestor fetches team game "other stats" from NBA.com Stats API,
including paint points, fast break points, second chance points, etc.
"""

from typing import Any, Optional

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.models.advanced_stats import TeamGameOtherStatsCreate

logger = structlog.get_logger(__name__)


@register_ingestor
class TeamOtherStatsIngestor(BaseIngestor):
    """
    Ingestor for team game "other stats" from NBA.com Stats API.

    Supports fetching other stats for specific games or teams.
    Other stats include:
    - Points in the paint
    - Second chance points
    - Fast break points
    - Largest lead
    - Lead changes
    - Times tied
    - Team turnovers and rebounds
    - Points off turnovers

    Note: This data is typically available in the box score summary.
    """

    entity_type = "team_other_stats"

    def __init__(self, cache=None, rate_limiter=None):
        """
        Initialize TeamOtherStatsIngestor.

        Args:
            cache: Content cache for API responses.
            rate_limiter: Rate limiter for API requests.
        """
        super().__init__(cache, rate_limiter)
        self.nba_client = NBAStatsClient(cache, rate_limiter)

    def fetch(
        self,
        entity_id: str,
        season: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Fetch team other stats from NBA.com Stats API.

        Args:
            entity_id: Game ID (10-character NBA.com game ID) or "team:<team_id>:<season>" for all games.
            season: Season (required if entity_id starts with "team:").
            **kwargs: Additional parameters for the API request.

        Returns:
            Dictionary with other stats data.

        Raises:
            Exception: If fetch fails after retries.
        """
        if entity_id.startswith("team:"):
            # Fetch all games for a team in a season
            parts = entity_id.split(":")
            team_id = int(parts[1])
            team_season = parts[2] if len(parts) > 2 else season

            if not team_season:
                raise ValueError("Season must be provided when fetching by team")

            self.logger.info("Fetching other stats for team games", team_id=team_id, season=team_season)

            # For now, return placeholder - would need game log endpoint
            return {
                "scope": "team_season",
                "team_id": team_id,
                "season": team_season,
                "data": {},
            }

        else:
            # Fetch other stats for a specific game
            game_id = entity_id
            self.logger.info("Fetching other stats for game", game_id=game_id)

            data = self.nba_client.get_box_score_summary(game_id=game_id)

            return {
                "scope": "game",
                "game_id": game_id,
                "season": season,
                "data": data,
            }

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        """
        Validate raw other stats data using Pydantic models.

        Args:
            raw: Raw data dictionary from NBA.com API.

        Returns:
            List of validated TeamGameOtherStatsCreate models.

        Raises:
            pydantic.ValidationError: If validation fails.
        """
        data = raw.get("data", {})
        scope = raw.get("scope", "game")
        game_id = raw.get("game_id")
        season = raw.get("season")

        validated_records = []

        # Extract season_id if provided
        season_id = None
        if season:
            season_year = int(season.split("-")[0])
            season_id = season_year

        # Process box score summary data
        # The data structure contains multiple datasets, we need to find
        # the "OtherStats" dataset which contains paint points, fast break, etc.
        for dataset_name, dataset_data in data.items():
            if not isinstance(dataset_data, dict):
                continue

            # Look for other stats data
            # This might be in a dataset called "OtherStats" or similar
            if "OTHER_STATS" in dataset_name.upper() or "LineScore" in dataset_name:
                data_rows = dataset_data.get("data", [])
                headers = dataset_data.get("headers", [])

                for row in data_rows:
                    try:
                        # Map row data to field names using headers
                        row_dict = dict(zip(headers, row)) if headers else {}

                        # Determine if this is home or away team
                        team_id = row_dict.get("TEAM_ID")
                        if not team_id:
                            continue

                        # Extract other stats fields
                        # These fields may vary, so we need to handle various naming conventions
                        other_stats_data = {
                            "game_id": game_id,
                            "team_id": self._safe_int(team_id),
                            "season_id": season_id,
                            "points_paint": self._safe_int(
                                row_dict.get("PTS_PAINT", row_dict.get("PTS_IN_PAINT"))
                            ),
                            "points_second_chance": self._safe_int(
                                row_dict.get("PTS_2ND_CHANCE", row_dict.get("PTS_2NDCHANCE"))
                            ),
                            "points_fast_break": self._safe_int(
                                row_dict.get("PTS_FB", row_dict.get("PTS_FAST_BREAK"))
                            ),
                            "largest_lead": self._safe_int(row_dict.get("LARGEST_LEAD")),
                            "lead_changes": self._safe_int(row_dict.get("LEAD_CHANGES")),
                            "times_tied": self._safe_int(row_dict.get("TIMES_TIED")),
                            "team_turnovers": self._safe_int(
                                row_dict.get("TEAM_TURNOVERS", row_dict.get("TEAM_TO"))
                            ),
                            "total_turnovers": self._safe_int(
                                row_dict.get("TOT_TO", row_dict.get("TOTAL_TURNOVERS"))
                            ),
                            "team_rebounds": self._safe_int(
                                row_dict.get("TEAM_REBOUNDS", row_dict.get("TEAM_REB"))
                            ),
                            "points_off_turnovers": self._safe_int(
                                row_dict.get("PTS_OFF_TO", row_dict.get("PTS_OFF_TURNOVERS"))
                            ),
                        }

                        # Only add if we have a team_id
                        if other_stats_data["team_id"]:
                            validated_record = TeamGameOtherStatsCreate(**other_stats_data)
                            validated_records.append(validated_record)

                    except pydantic.ValidationError as e:
                        self.logger.error(
                            "Other stats validation failed",
                            row_data=row_dict,
                            errors=str(e),
                        )
                        raise

        self.logger.info("Validated other stats records", count=len(validated_records))
        return validated_records

    def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
        """
        Insert or update validated other stats data in database.

        Args:
            model: List of validated TeamGameOtherStatsCreate models.
            conn: SQLite database connection.

        Returns:
            Number of rows affected.
        """
        rows_affected = 0

        for stats_record in model:
            if not isinstance(stats_record, TeamGameOtherStatsCreate):
                continue

            # Check if record exists
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM team_game_other_stats
                WHERE game_id = ? AND team_id = ?
                """,
                (stats_record.game_id, stats_record.team_id),
            )
            exists = cursor.fetchone()[0] > 0

            if exists:
                # Update existing record
                self._update_other_stats(stats_record, conn)
                rows_affected += 1
            else:
                # Insert new record
                self._insert_other_stats(stats_record, conn)
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
                    f"{stats_record.game_id}_{stats_record.team_id}",
                    f"game: {stats_record.game_id}",
                ),
            )

        return rows_affected

    def _insert_other_stats(self, stats: TeamGameOtherStatsCreate, conn) -> None:
        """
        Insert a new other stats record into the database.

        Args:
            stats: TeamGameOtherStatsCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            INSERT INTO team_game_other_stats (
                game_id, team_id, season_id, points_paint, points_second_chance,
                points_fast_break, largest_lead, lead_changes, times_tied,
                team_turnovers, total_turnovers, team_rebounds, points_off_turnovers
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stats.game_id,
                stats.team_id,
                stats.season_id,
                stats.points_paint,
                stats.points_second_chance,
                stats.points_fast_break,
                stats.largest_lead,
                stats.lead_changes,
                stats.times_tied,
                stats.team_turnovers,
                stats.total_turnovers,
                stats.team_rebounds,
                stats.points_off_turnovers,
            ),
        )

    def _update_other_stats(self, stats: TeamGameOtherStatsCreate, conn) -> None:
        """
        Update an existing other stats record in the database.

        Args:
            stats: TeamGameOtherStatsCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            UPDATE team_game_other_stats SET
                season_id = ?, points_paint = ?, points_second_chance = ?,
                points_fast_break = ?, largest_lead = ?, lead_changes = ?,
                times_tied = ?, team_turnovers = ?, total_turnovers = ?,
                team_rebounds = ?, points_off_turnovers = ?
            WHERE game_id = ? AND team_id = ?
            """,
            (
                stats.season_id,
                stats.points_paint,
                stats.points_second_chance,
                stats.points_fast_break,
                stats.largest_lead,
                stats.lead_changes,
                stats.times_tied,
                stats.team_turnovers,
                stats.total_turnovers,
                stats.team_rebounds,
                stats.points_off_turnovers,
                stats.game_id,
                stats.team_id,
            ),
        )

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        """Safely convert value to int, returning None for empty/invalid values."""
        if value is None or value == "" or value == "-":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
