"""Game models."""

from pydantic import BaseModel, Field


class GameBase(BaseModel):
    """Base game model."""

    game_id: str = Field(..., description="NBA.com 10-character game ID")
    season_id: int = Field(..., description="Season ID")
    game_date: str = Field(..., description="Game date (ISO format)")
    game_type: str = Field(..., description="Game type (Regular Season, Playoffs, etc.)")
    game_sequence: int | None = Field(None, description="Game sequence number")
    home_team_id: int = Field(..., description="Home team ID")
    away_team_id: int = Field(..., description="Away team ID")
    home_team_score: int | None = Field(None, description="Home team score")
    away_team_score: int | None = Field(None, description="Away team score")
    winner_team_id: int | None = Field(None, description="Winning team ID")
    overtime_periods: int = Field(default=0, description="Number of overtime periods")
    arena_id: int | None = Field(None, description="Arena ID")
    attendance: int | None = Field(None, description="Attendance")
    game_duration_mins: int | None = Field(None, description="Game duration in minutes")
    playoff_round: str | None = Field(None, description="Playoff round")
    playoff_series_id: str | None = Field(None, description="Playoff series ID")
    national_tv: str | None = Field(None, description="National TV broadcaster")
    data_availability_flags: int = Field(default=0, description="Bitmask of available data types")


class GameCreate(GameBase):
    """Model for creating a game."""

    pass


class Game(GameBase):
    """Complete game model."""

    class Config:
        """Pydantic configuration."""

        from_attributes = True
