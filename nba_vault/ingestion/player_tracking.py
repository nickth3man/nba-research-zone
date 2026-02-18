"""Player tracking data ingestor.

This ingestor fetches player tracking data from NBA.com Stats API,
including speed, distance, touches, drives, and other movement metrics.
Tracking data is available from the 2013-14 season onwards.
"""

from typing import Any, Optional

import pydantic
import structlog

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.nba_stats_client import NBAStatsClient
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.models.advanced_stats import PlayerGameTrackingCreate

logger = structlog.get_logger(__name__)


@register_ingestor
class PlayerTrackingIngestor(BaseIngestor):
    """
    Ingestor for player tracking data from NBA.com Stats API.

    Supports fetching tracking data for single players or entire teams.
    Tracking data includes:
    - Distance covered (total, offensive, defensive)
    - Speed (average, maximum)
    - Touches (total, catch & shoot, paint, post-up)
    - Drives and points on drives
    - Pull-up shots

    Note: Tracking data is only available from 2013-14 season onwards.
    """

    entity_type = "player_tracking"

    def __init__(self, cache=None, rate_limiter=None):
        """
        Initialize PlayerTrackingIngestor.

        Args:
            cache: Content cache for API responses.
            rate_limiter: Rate limiter for API requests.
        """
        super().__init__(cache, rate_limiter)
        self.nba_client = NBAStatsClient(cache, rate_limiter)

    def fetch(
        self,
        entity_id: str,
        season: str,
        season_type: str = "Regular Season",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Fetch player tracking data from NBA.com Stats API.

        Args:
            entity_id: Player ID (as string) or "team:<team_id>" for all players on a team.
            season: Season in format "YYYY-YY" (e.g., "2023-24").
            season_type: "Regular Season", "Playoffs", or "Pre Season".
            **kwargs: Additional parameters for the API request.

        Returns:
            Dictionary with tracking data.

        Raises:
            ValueError: If season is before 2013-14 (tracking data not available).
            Exception: If fetch fails after retries.
        """
        # Validate season availability
        season_year = int(season.split("-")[0])
        if season_year < 2013:
            raise ValueError(
                f"Player tracking data is only available from 2013-14 onwards. "
                f"Requested season: {season}"
            )

        if entity_id.startswith("team:"):
            # Fetch all players for a team
            team_id = int(entity_id.split(":")[1])
            self.logger.info("Fetching tracking data for team", team_id=team_id, season=season)
            # Use team stats endpoint to get roster, then fetch tracking for each
            # For now, return empty - this will be implemented with team endpoint
            return {"team_id": team_id, "season": season, "players": []}
        else:
            # Fetch single player
            player_id = int(entity_id)
            self.logger.info("Fetching tracking data for player", player_id=player_id, season=season)

            data = self.nba_client.get_player_tracking(
                player_id=player_id,
                season=season,
                season_type=season_type,
                **kwargs,
            )

            return {
                "player_id": player_id,
                "season": season,
                "season_type": season_type,
                "data": data,
            }

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        """
        Validate raw tracking data using Pydantic models.

        Args:
            raw: Raw data dictionary from NBA.com API.

        Returns:
            List of validated PlayerGameTrackingCreate models.

        Raises:
            pydantic.ValidationError: If validation fails.
        """
        data = raw.get("data", {})
        player_id = raw.get("player_id")
        season = raw.get("season")
        season_type = raw.get("season_type", "Regular Season")

        validated_records = []

        # Extract season_id from season string (e.g., "2023-24" -> 2023)
        season_year = int(season.split("-")[0])
        season_id = season_year

        # Process tracking stats data
        # NBA.com returns multiple data sets, we want the overall stats
        for dataset_name, dataset_data in data.items():
            if not isinstance(dataset_data, dict):
                continue

            data_rows = dataset_data.get("data", [])
            headers = dataset_data.get("headers", [])

            for row in data_rows:
                try:
                    # Map row data to field names using headers
                    row_dict = dict(zip(headers, row)) if headers else {}

                    # Extract relevant tracking fields
                    tracking_data = {
                        "game_id": row_dict.get("GAME_ID", f"season_{season_id}"),
                        "player_id": player_id,
                        "team_id": row_dict.get("TEAM_ID"),
                        "season_id": season_id,
                        "minutes_played": self._safe_float(row_dict.get("MIN")),
                        "distance_miles": self._safe_float(row_dict.get("DIST_MILES")),
                        "distance_miles_offensive": self._safe_float(
                            row_dict.get("DIST_MILES_OFF")
                        ),
                        "distance_miles_defensive": self._safe_float(
                            row_dict.get("DIST_MILES_DEF")
                        ),
                        "speed_mph_avg": self._safe_float(row_dict.get("SPD")),
                        "speed_mph_max": self._safe_float(row_dict.get("MAX_SPEED")),
                        "touches": self._safe_int(row_dict.get("TOUCHES")),
                        "touches_catch_shoot": self._safe_int(row_dict.get("EFC")),
                        "touches_paint": self._safe_int(row_dict.get("PAINT")),
                        "touches_post_up": self._safe_int(row_dict.get("POST")),
                        "drives": self._safe_int(row_dict.get("DRIVES")),
                        "drives_pts": self._safe_int(row_dict.get("DRIVES_PTS")),
                        "pull_up_shots": self._safe_int(row_dict.get("PULL_UP_FGA")),
                        "pull_up_shots_made": self._safe_int(row_dict.get("PULL_UP_FGM")),
                    }

                    # Only add if we have meaningful data
                    if any(v is not None for v in tracking_data.values()):
                        validated_record = PlayerGameTrackingCreate(**tracking_data)
                        validated_records.append(validated_record)

                except pydantic.ValidationError as e:
                    self.logger.error(
                        "Tracking record validation failed",
                        row_data=row_dict,
                        errors=str(e),
                    )
                    raise

        self.logger.info("Validated tracking records", count=len(validated_records))
        return validated_records

    def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
        """
        Insert or update validated tracking data in database.

        Args:
            model: List of validated PlayerGameTrackingCreate models.
            conn: SQLite database connection.

        Returns:
            Number of rows affected.
        """
        rows_affected = 0

        for tracking_record in model:
            if not isinstance(tracking_record, PlayerGameTrackingCreate):
                continue

            # Check if record exists
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM player_game_tracking
                WHERE game_id = ? AND player_id = ?
                """,
                (tracking_record.game_id, tracking_record.player_id),
            )
            exists = cursor.fetchone()[0] > 0

            if exists:
                # Update existing record
                self._update_tracking(tracking_record, conn)
                rows_affected += 1
            else:
                # Insert new record
                self._insert_tracking(tracking_record, conn)
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
                    f"{tracking_record.game_id}_{tracking_record.player_id}",
                    f"season: {tracking_record.season_id}",
                ),
            )

        return rows_affected

    def _insert_tracking(self, tracking: PlayerGameTrackingCreate, conn) -> None:
        """
        Insert a new tracking record into the database.

        Args:
            tracking: PlayerGameTrackingCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            INSERT INTO player_game_tracking (
                game_id, player_id, team_id, season_id, minutes_played,
                distance_miles, distance_miles_offensive, distance_miles_defensive,
                speed_mph_avg, speed_mph_max, touches, touches_catch_shoot,
                touches_paint, touches_post_up, drives, drives_pts,
                pull_up_shots, pull_up_shots_made
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tracking.game_id,
                tracking.player_id,
                tracking.team_id,
                tracking.season_id,
                tracking.minutes_played,
                tracking.distance_miles,
                tracking.distance_miles_offensive,
                tracking.distance_miles_defensive,
                tracking.speed_mph_avg,
                tracking.speed_mph_max,
                tracking.touches,
                tracking.touches_catch_shoot,
                tracking.touches_paint,
                tracking.touches_post_up,
                tracking.drives,
                tracking.drives_pts,
                tracking.pull_up_shots,
                tracking.pull_up_shots_made,
            ),
        )

    def _update_tracking(self, tracking: PlayerGameTrackingCreate, conn) -> None:
        """
        Update an existing tracking record in the database.

        Args:
            tracking: PlayerGameTrackingCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            UPDATE player_game_tracking SET
                team_id = ?, season_id = ?, minutes_played = ?,
                distance_miles = ?, distance_miles_offensive = ?,
                distance_miles_defensive = ?, speed_mph_avg = ?, speed_mph_max = ?,
                touches = ?, touches_catch_shoot = ?, touches_paint = ?,
                touches_post_up = ?, drives = ?, drives_pts = ?,
                pull_up_shots = ?, pull_up_shots_made = ?
            WHERE game_id = ? AND player_id = ?
            """,
            (
                tracking.team_id,
                tracking.season_id,
                tracking.minutes_played,
                tracking.distance_miles,
                tracking.distance_miles_offensive,
                tracking.distance_miles_defensive,
                tracking.speed_mph_avg,
                tracking.speed_mph_max,
                tracking.touches,
                tracking.touches_catch_shoot,
                tracking.touches_paint,
                tracking.touches_post_up,
                tracking.drives,
                tracking.drives_pts,
                tracking.pull_up_shots,
                tracking.pull_up_shots_made,
                tracking.game_id,
                tracking.player_id,
            ),
        )

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        """Safely convert value to float, returning None for empty/invalid values."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        """Safely convert value to int, returning None for empty/invalid values."""
        if value is None or value == "":
            return None
        try:
            return int(float(value))  # Convert to float first to handle "1.0"
        except (ValueError, TypeError):
            return None
