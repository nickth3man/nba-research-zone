"""Season models."""

from pydantic import BaseModel, Field


class SeasonBase(BaseModel):
    """Base season model."""

    season_id: int = Field(..., description="Season start year (e.g., 2024 for 2024-25)")
    league_id: str = Field(..., description="League identifier")
    season_label: str = Field(..., description="Human-readable season label (e.g., '2024-25')")
    games_per_team: int | None = Field(None, description="Number of games per team in season")
    schedule_start: str | None = Field(None, description="Season start date (ISO format)")
    schedule_end: str | None = Field(None, description="Season end date (ISO format)")
    champion_franchise_id: int | None = Field(None, description="Franchise ID of champion")
    finals_mvp_player_id: int | None = Field(None, description="Player ID of Finals MVP")


class SeasonCreate(SeasonBase):
    """Model for creating a season."""

    pass


class Season(SeasonBase):
    """Complete season model."""

    class Config:
        """Pydantic configuration."""

        from_attributes = True
