"""Player models for data ingestion."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class BasketballReferencePlayer(BaseModel):
    """
    Raw player data from Basketball Reference web scraper.

    This model validates the raw data format from basketball_reference_web_scraper.
    """

    slug: str = Field(..., description="Basketball Reference slug (e.g., 'jamesle01')")
    name: str = Field(..., description="Player name")
    position: str = Field(..., description="Position")
    height: str = Field(..., description="Height (format: '6-11')")
    weight: str = Field(..., description="Weight in pounds")
    team_abbreviation: str | None = Field(None, description="Team abbreviation")
    games_played: int = Field(default=0, description="Games played")
    games_started: int = Field(default=0, description="Games started")
    minutes_played: float = Field(default=0.0, description="Minutes played")
    field_goals: int = Field(default=0, description="Field goals made")
    field_goal_attempts: int = Field(default=0, description="Field goal attempts")
    field_goal_percentage: float = Field(default=0.0, description="Field goal percentage")
    three_point_field_goals: int = Field(default=0, description="Three-point field goals made")
    three_point_field_goal_attempts: int = Field(default=0, description="Three-point attempts")
    three_point_field_goal_percentage: float = Field(default=0.0, description="3P percentage")
    two_point_field_goals: int = Field(default=0, description="Two-point field goals made")
    two_point_field_goal_attempts: int = Field(default=0, description="Two-point attempts")
    two_point_field_goal_percentage: float = Field(default=0.0, description="2P percentage")
    effective_field_goal_percentage: float = Field(default=0.0, description="eFG%")
    free_throws: int = Field(default=0, description="Free throws made")
    free_throw_attempts: int = Field(default=0, description="Free throw attempts")
    free_throw_percentage: float = Field(default=0.0, description="Free throw percentage")
    offensive_rebounds: int = Field(default=0, description="Offensive rebounds")
    defensive_rebounds: int = Field(default=0, description="Defensive rebounds")
    rebounds: int = Field(default=0, description="Total rebounds")
    assists: int = Field(default=0, description="Assists")
    steals: int = Field(default=0, description="Steals")
    blocks: int = Field(default=0, description="Blocks")
    turnovers: int = Field(default=0, description="Turnovers")
    personal_fouls: int = Field(default=0, description="Personal fouls")
    points: int = Field(default=0, description="Points scored")
    player_advanced_stats: dict[str, Any] = Field(default_factory=dict, description="Advanced stats")

    @field_validator("height")
    @classmethod
    def validate_height(cls, v: str) -> str:
        """Validate height format."""
        if v and "-" not in str(v):
            # If height is in inches, convert to feet-inches format
            try:
                inches = int(v)
                feet = inches // 12
                remaining_inches = inches % 12
                return f"{feet}-{remaining_inches}"
            except (ValueError, TypeError):
                pass
        return v

    @field_validator("weight")
    @classmethod
    def validate_weight(cls, v: str | int) -> str:
        """Validate weight format."""
        return str(v)


class PlayerCreate(BaseModel):
    """
    Model for creating/updating a player in the database.

    This model maps to the player table schema and includes validation
    for data cleaning and transformation.
    """

    player_id: int | None = Field(None, description="NBA.com player ID")
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

    @field_validator("birthdate")
    @classmethod
    def validate_birthdate(cls, v: str | None) -> str | None:
        """Validate and normalize birthdate to ISO format."""
        if v is None:
            return None
        if isinstance(v, str):
            # Already a string, ensure ISO format
            try:
                # Try parsing various date formats
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):
                    try:
                        dt = datetime.strptime(v, fmt)
                        return dt.isoformat()
                    except ValueError:
                        continue
            except Exception:
                pass
        return v

    @field_validator("height_inches")
    @classmethod
    def validate_height_inches(cls, v: float | None) -> float | None:
        """Validate height in inches."""
        if v is not None and (v < 60 or v > 96):  # Reasonable range: 5'0" to 8'0"
            return None
        return v

    @field_validator("weight_lbs")
    @classmethod
    def validate_weight_lbs(cls, v: float | None) -> float | None:
        """Validate weight in pounds."""
        if v is not None and (v < 100 or v > 400):  # Reasonable range
            return None
        return v

    @model_validator(mode="before")
    @classmethod
    def validate_names(cls, data: Any) -> Any:
        """Ensure name fields are properly populated."""
        if isinstance(data, dict):
            # Ensure full_name is populated if missing
            if "full_name" not in data or not data["full_name"]:
                first = data.get("first_name", "")
                last = data.get("last_name", "")
                if first and last:
                    data["full_name"] = f"{first} {last}"
                elif last:
                    data["full_name"] = last

            # Ensure display_name is populated if missing
            if "display_name" not in data or not data["display_name"]:
                if "full_name" in data and data["full_name"]:
                    data["display_name"] = data["full_name"]

        return data

    @classmethod
    def from_basketball_reference(cls, data: BasketballReferencePlayer) -> "PlayerCreate":
        """
        Create a PlayerCreate from Basketball Reference data.

        Args:
            data: BasketballReferencePlayer model.

        Returns:
            PlayerCreate model with mapped fields.
        """
        # Parse height from "6-11" format to inches
        height_inches = None
        if data.height and "-" in str(data.height):
            try:
                feet, inches = str(data.height).split("-")
                height_inches = (int(feet) * 12) + int(inches)
            except (ValueError, AttributeError):
                pass

        # Parse weight
        weight_lbs = None
        if data.weight:
            try:
                weight_lbs = float(str(data.weight))
            except (ValueError, AttributeError):
                pass

        # Parse name into first and last
        name_parts = data.name.split()
        first_name = name_parts[0] if len(name_parts) > 0 else data.name
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        # Map position
        position_mapping = {
            "PG": "Point Guard",
            "SG": "Shooting Guard",
            "SF": "Small Forward",
            "PF": "Power Forward",
            "C": "Center",
        }
        primary_position = position_mapping.get(data.position.split("-")[0].strip()) if data.position else None

        return cls(
            player_id=None,  # Basketball Reference doesn't provide NBA.com ID
            bbref_id=data.slug,
            first_name=first_name,
            last_name=last_name,
            full_name=data.name,
            display_name=data.name,
            height_inches=height_inches,
            weight_lbs=weight_lbs,
            position=data.position,
            primary_position=primary_position,
            is_active=True,  # Assume active if in season totals
            data_availability_flags=1 << 0,  # Set basic stats flag
        )


class PlayerUpdate(BaseModel):
    """Model for updating a player."""

    first_name: str | None = None
    last_name: str | None = None
    full_name: str | None = None
    display_name: str | None = None
    birthdate: str | None = None
    birthplace_city: str | None = None
    birthplace_state: str | None = None
    birthplace_country: str | None = None
    height_inches: float | None = None
    weight_lbs: float | None = None
    position: str | None = None
    primary_position: str | None = None
    jersey_number: str | None = None
    college: str | None = None
    country: str | None = None
    draft_year: int | None = None
    draft_round: int | None = None
    draft_number: int | None = None
    is_active: bool | None = None
    from_year: int | None = None
    to_year: int | None = None
    bbref_id: str | None = None
    data_availability_flags: int | None = None
