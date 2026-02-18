"""Franchise models."""

from pydantic import BaseModel, Field


class FranchiseBase(BaseModel):
    """Base franchise model."""

    franchise_id: int = Field(..., description="Internal franchise ID")
    nba_franchise_id: int | None = Field(None, description="NBA.com franchise ID")
    current_team_name: str = Field(..., description="Current team name")
    current_city: str = Field(..., description="Current city")
    abbreviation: str = Field(..., description="Team abbreviation")
    conference: str | None = Field(None, description="Conference")
    division: str | None = Field(None, description="Division")
    founded_year: int | None = Field(None, description="Year franchise was founded")
    league_id: str = Field(..., description="League identifier")


class FranchiseCreate(FranchiseBase):
    """Model for creating a franchise."""

    pass


class Franchise(FranchiseBase):
    """Complete franchise model."""

    class Config:
        """Pydantic configuration."""

        from_attributes = True
