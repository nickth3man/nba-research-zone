"""Player models."""

from pydantic import BaseModel, Field


class PlayerBase(BaseModel):
    """Base player model."""

    player_id: int = Field(..., description="NBA.com player ID")
    first_name: str = Field(..., description="First name")
    last_name: str = Field(..., description="Last name")
    full_name: str = Field(..., description="Full name")
    display_name: str | None = Field(None, description="Display name (e.g., 'LeBron James')")
    birthdate: str | None = Field(None, description="Birthdate (ISO format)")
    birthplace_city: str | None = Field(None, description="Birthplace city")
    birthplace_state: str | None = Field(None, description="Birthplace state")
    birthplace_country: str | None = Field(None, description="Birthplace country")
    height_inches: float | None = Field(None, description="Height in inches")
    weight_lbs: float | None = Field(None, description="Weight in pounds")
    position: str | None = Field(None, description="Position(s)")
    primary_position: str | None = Field(None, description="Primary position")
    jersey_number: str | None = Field(None, description="Last known jersey number")
    college: str | None = Field(None, description="College attended")
    country: str | None = Field(None, description="Country")
    draft_year: int | None = Field(None, description="Draft year")
    draft_round: int | None = Field(None, description="Draft round")
    draft_number: int | None = Field(None, description="Draft pick number")
    is_active: bool = Field(default=True, description="Whether player is active")
    from_year: int | None = Field(None, description="First year played")
    to_year: int | None = Field(None, description="Last year played")
    bbref_id: str | None = Field(None, description="Basketball-Reference ID")
    data_availability_flags: int = Field(default=0, description="Bitmask of available data types")


class PlayerCreate(PlayerBase):
    """Model for creating a player."""

    pass


class Player(PlayerBase):
    """Complete player model."""

    class Config:
        """Pydantic configuration."""

        from_attributes = True
