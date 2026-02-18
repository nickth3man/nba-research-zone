"""Coach models."""

from pydantic import BaseModel, Field


class CoachBase(BaseModel):
    """Base coach model."""

    coach_id: int = Field(..., description="NBA.com coach ID")
    first_name: str = Field(..., description="First name")
    last_name: str = Field(..., description="Last name")
    full_name: str = Field(..., description="Full name")
    birthdate: str | None = Field(None, description="Birthdate (ISO format)")
    college: str | None = Field(None, description="College attended")
    is_active: bool = Field(default=True, description="Whether coach is active")


class CoachCreate(CoachBase):
    """Model for creating a coach."""

    pass


class Coach(CoachBase):
    """Complete coach model."""

    class Config:
        """Pydantic configuration."""

        from_attributes = True
