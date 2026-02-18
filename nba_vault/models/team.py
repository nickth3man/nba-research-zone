"""Team models."""

from pydantic import BaseModel, Field


class TeamBase(BaseModel):
    """Base team model."""

    team_id: int = Field(..., description="NBA.com team ID")
    franchise_id: int = Field(..., description="Franchise ID")
    season_id: int = Field(..., description="Season ID")
    team_name: str = Field(..., description="Team name")
    city: str = Field(..., description="City")
    abbreviation: str = Field(..., description="Team abbreviation")
    conference: str = Field(..., description="Conference")
    division: str = Field(..., description="Division")
    arena_name: str | None = Field(None, description="Arena name")
    arena_capacity: int | None = Field(None, description="Arena capacity")
    owner: str | None = Field(None, description="Owner name")
    general_manager: str | None = Field(None, description="General manager name")


class TeamCreate(TeamBase):
    """Model for creating a team."""

    pass


class Team(TeamBase):
    """Complete team model."""

    class Config:
        """Pydantic configuration."""

        from_attributes = True
