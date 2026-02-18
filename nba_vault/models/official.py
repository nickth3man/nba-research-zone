"""Official models."""

from pydantic import BaseModel, Field


class OfficialBase(BaseModel):
    """Base official model."""

    official_id: int = Field(..., description="NBA.com official ID")
    first_name: str = Field(..., description="First name")
    last_name: str = Field(..., description="Last name")
    full_name: str = Field(..., description="Full name")
    jersey_num: str | None = Field(None, description="Jersey number")
    is_active: bool = Field(default=True, description="Whether official is active")


class OfficialCreate(OfficialBase):
    """Model for creating an official."""

    pass


class Official(OfficialBase):
    """Complete official model."""

    class Config:
        """Pydantic configuration."""

        from_attributes = True
