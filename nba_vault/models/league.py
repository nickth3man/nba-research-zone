"""League models."""

from pydantic import BaseModel, Field, field_validator


class LeagueBase(BaseModel):
    """Base league model."""

    league_id: str = Field(..., description="League ID (NBA, ABA, BAA)")
    league_name: str = Field(..., description="Full league name")
    founded_year: int | None = Field(None, description="Year league was founded")
    folded_year: int | None = Field(None, description="Year league folded (None if active)")


class LeagueCreate(LeagueBase):
    """Model for creating a league."""

    pass


class League(LeagueBase):
    """Complete league model."""

    class Config:
        """Pydantic configuration."""

        from_attributes = True
