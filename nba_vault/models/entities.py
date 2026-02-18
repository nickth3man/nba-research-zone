r"""Pydantic models for all NBA Vault entity types.

This module covers every schema table that previously lacked a validation
model: seasons, franchises, teams, games, box scores (traditional/advanced/
hustle), play-by-play, shot charts, officials, coaches, draft picks, awards,
player season stats, transactions, and cross-reference tables.

Validation conventions used throughout:
- Percentage fields: bounded [0.0, 1.0] (raw form, not x100)
- Season year: integer start year, e.g. 2023 for 2023-24
- game_id: 10-digit string matching r'^\d{10}$'
- season string: "YYYY-YY" e.g. "2023-24"
- ISO dates stored as TEXT (YYYY-MM-DD) in SQLite
- All FK-referencing fields accept None; FK existence is checked at upsert time
  by require_fk() from nba_vault.ingestion.validation
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Season
# ---------------------------------------------------------------------------


class SeasonCreate(BaseModel):
    """Maps to the season table."""

    season_id: int = Field(..., ge=1946, le=2099, description="Season start year (e.g. 2024)")
    league_id: str = Field(..., description="'NBA', 'ABA', or 'BAA'")
    season_label: str = Field(..., description="Human-readable label e.g. '2024-25'")
    games_per_team: int | None = Field(None, ge=0, description="Regular season games per team")
    schedule_start: str | None = Field(None, description="ISO date of first game")
    schedule_end: str | None = Field(None, description="ISO date of last game")
    champion_franchise_id: int | None = Field(None, description="Franchise that won the title")
    finals_mvp_player_id: int | None = Field(None, description="Finals MVP player ID")

    @field_validator("league_id")
    @classmethod
    def validate_league_id(cls, v: str) -> str:
        allowed = {"NBA", "ABA", "BAA"}
        if v not in allowed:
            raise ValueError(f"league_id must be one of {allowed}, got '{v}'")
        return v

    @field_validator("season_label")
    @classmethod
    def validate_season_label(cls, v: str) -> str:
        """Expect 'YYYY-YY' format, e.g. '2024-25'."""
        import re  # noqa: PLC0415

        if not re.match(r"^\d{4}-\d{2}$", v):
            raise ValueError(f"season_label must be 'YYYY-YY', got '{v}'")
        return v


# ---------------------------------------------------------------------------
# Franchise
# ---------------------------------------------------------------------------


class FranchiseCreate(BaseModel):
    """Maps to the franchise table."""

    franchise_id: int = Field(..., description="NBA.com canonical franchise ID")
    nba_franchise_id: int | None = Field(None, description="NBA.com franchise ID (may differ)")
    current_team_name: str = Field(..., description="Current official team name")
    current_city: str = Field(..., description="Current home city")
    abbreviation: str = Field(..., min_length=2, max_length=5, description="Team abbreviation")
    conference: str | None = Field(None, description="'East' or 'West'")
    division: str | None = Field(None, description="Division name")
    founded_year: int | None = Field(None, ge=1946, description="Year franchise was founded")
    league_id: str = Field(..., description="'NBA', 'ABA', or 'BAA'")

    @field_validator("league_id")
    @classmethod
    def validate_league_id(cls, v: str) -> str:
        allowed = {"NBA", "ABA", "BAA"}
        if v not in allowed:
            raise ValueError(f"league_id must be one of {allowed}, got '{v}'")
        return v

    @field_validator("conference")
    @classmethod
    def validate_conference(cls, v: str | None) -> str | None:
        if v is not None and v not in {"East", "West", "Eastern", "Western"}:
            # Normalise legacy values
            if "east" in v.lower():
                return "East"
            if "west" in v.lower():
                return "West"
        return v


# ---------------------------------------------------------------------------
# Team (season-specific)  # noqa: ERA001
# ---------------------------------------------------------------------------


class TeamCreate(BaseModel):
    """Maps to the team table (one row per franchise/season)."""

    team_id: int = Field(..., description="NBA.com season-specific team ID")
    franchise_id: int = Field(..., description="FK → franchise.franchise_id")
    season_id: int = Field(..., ge=1946, description="FK → season.season_id")
    team_name: str = Field(..., description="Official team name for this season")
    city: str = Field(..., description="City name for this season")
    abbreviation: str = Field(..., min_length=2, max_length=5)
    conference: str = Field(..., description="'East' or 'West'")
    division: str = Field(..., description="Division name")
    arena_name: str | None = Field(None)
    arena_capacity: int | None = Field(None, ge=0)
    owner: str | None = Field(None)
    general_manager: str | None = Field(None)


# ---------------------------------------------------------------------------
# Arena
# ---------------------------------------------------------------------------


class ArenaCreate(BaseModel):
    """Maps to the arena table."""

    arena_name: str = Field(..., description="Official arena name")
    city: str = Field(...)
    state: str | None = Field(None)
    country: str = Field(default="USA")
    capacity: int | None = Field(None, ge=0)
    opened_year: int | None = Field(None, ge=1900)
    closed_year: int | None = Field(None, ge=1900)
    latitude: float | None = Field(None, ge=-90, le=90)
    longitude: float | None = Field(None, ge=-180, le=180)

    @model_validator(mode="after")
    def check_year_order(self) -> ArenaCreate:
        if self.opened_year and self.closed_year and self.opened_year > self.closed_year:
            raise ValueError("opened_year must be <= closed_year")
        return self


# ---------------------------------------------------------------------------
# Game
# ---------------------------------------------------------------------------

GAME_TYPES = {"Regular Season", "Playoffs", "Pre Season", "All-Star", "Play-In"}
PLAYOFF_ROUNDS = {
    "First Round",
    "Conference Semifinals",
    "Conference Finals",
    "Finals",
    "Play-In",
    None,
}


class GameCreate(BaseModel):
    """Maps to the game table."""

    game_id: str = Field(..., description="NBA.com 10-character game ID")
    season_id: int = Field(..., ge=1946, description="FK → season.season_id")
    game_date: str = Field(..., description="ISO date YYYY-MM-DD")
    game_type: str = Field(..., description="'Regular Season', 'Playoffs', etc.")
    game_sequence: int | None = Field(None, ge=0)
    home_team_id: int = Field(..., description="FK → team.team_id")
    away_team_id: int = Field(..., description="FK → team.team_id")
    home_team_score: int | None = Field(None, ge=0)
    away_team_score: int | None = Field(None, ge=0)
    winner_team_id: int | None = Field(None)
    overtime_periods: int = Field(default=0, ge=0, le=10)
    arena_id: int | None = Field(None)
    attendance: int | None = Field(None, ge=0)
    game_duration_mins: int | None = Field(None, ge=0)
    playoff_round: str | None = Field(None)
    playoff_series_id: str | None = Field(None)
    national_tv: str | None = Field(None)
    data_availability_flags: int = Field(default=0, ge=0)

    @field_validator("game_id")
    @classmethod
    def validate_game_id(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{10}$", v):
            raise ValueError(f"game_id must be a 10-digit string, got '{v}'")
        return v

    @field_validator("game_date")
    @classmethod
    def validate_game_date(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            raise ValueError(f"game_date must be YYYY-MM-DD, got '{v}'")
        return v

    @field_validator("game_type")
    @classmethod
    def validate_game_type(cls, v: str) -> str:
        if v not in GAME_TYPES:
            # Accept minor variants (e.g. "Playoffs" from API)
            return v
        return v

    @model_validator(mode="after")
    def check_scores_consistent(self) -> GameCreate:
        # Winner must be one of the two teams
        if (
            self.home_team_score is not None
            and self.away_team_score is not None
            and self.winner_team_id is not None
            and self.winner_team_id not in {self.home_team_id, self.away_team_id}
        ):
            raise ValueError(f"winner_team_id {self.winner_team_id} must be home or away team")
        return self


# ---------------------------------------------------------------------------
# Official
# ---------------------------------------------------------------------------


class OfficialCreate(BaseModel):
    """Maps to the official table."""

    official_id: int = Field(..., description="NBA.com official ID")
    first_name: str = Field(...)
    last_name: str = Field(...)
    full_name: str = Field(...)
    jersey_num: str | None = Field(None)
    is_active: int = Field(default=1, ge=0, le=1)


class GameOfficialCreate(BaseModel):
    """Maps to the game_official junction table."""

    game_id: str = Field(..., description="FK → game.game_id")
    official_id: int = Field(..., description="FK → official.official_id")
    assignment: str | None = Field(None, description="'Crew Chief', 'Referee', 'Umpire'")

    @field_validator("game_id")
    @classmethod
    def validate_game_id(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{10}$", v):
            raise ValueError(f"game_id must be a 10-digit string, got '{v}'")
        return v


# ---------------------------------------------------------------------------
# Box Score — Traditional
# ---------------------------------------------------------------------------


class BoxScorePlayerRowCreate(BaseModel):
    """Maps to player_game_log (traditional box score)."""

    game_id: str = Field(...)
    player_id: int = Field(...)
    team_id: int = Field(...)
    season_id: int = Field(..., ge=1946)
    start_position: str | None = Field(None)
    comment: str | None = Field(None, description="'DID NOT PLAY', 'INACTIVE', etc.")
    minutes_played: float | None = Field(None, ge=0, le=100)
    fgm: int | None = Field(None, ge=0)
    fga: int | None = Field(None, ge=0)
    fg_pct: float | None = Field(None, ge=0.0, le=1.0)
    fg3m: int | None = Field(None, ge=0)
    fg3a: int | None = Field(None, ge=0)
    fg3_pct: float | None = Field(None, ge=0.0, le=1.0)
    ftm: int | None = Field(None, ge=0)
    fta: int | None = Field(None, ge=0)
    ft_pct: float | None = Field(None, ge=0.0, le=1.0)
    oreb: int | None = Field(None, ge=0)
    dreb: int | None = Field(None, ge=0)
    reb: int | None = Field(None, ge=0)
    ast: int | None = Field(None, ge=0)
    stl: int | None = Field(None, ge=0)
    blk: int | None = Field(None, ge=0)
    tov: int | None = Field(None, ge=0)
    pf: int | None = Field(None, ge=0)
    pts: int | None = Field(None, ge=0)
    plus_minus: int | None = Field(None)

    @field_validator("game_id")
    @classmethod
    def validate_game_id(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{10}$", v):
            raise ValueError(f"game_id must be 10 digits, got '{v}'")
        return v

    @model_validator(mode="after")
    def check_fg_consistency(self) -> BoxScorePlayerRowCreate:
        if self.fgm is not None and self.fga is not None and self.fgm > self.fga:
            raise ValueError(f"fgm ({self.fgm}) cannot exceed fga ({self.fga})")
        if self.fg3m is not None and self.fg3a is not None and self.fg3m > self.fg3a:
            raise ValueError(f"fg3m ({self.fg3m}) cannot exceed fg3a ({self.fg3a})")
        if self.ftm is not None and self.fta is not None and self.ftm > self.fta:
            raise ValueError(f"ftm ({self.ftm}) cannot exceed fta ({self.fta})")
        return self


class BoxScoreTeamRowCreate(BaseModel):
    """Maps to team_game_log."""

    game_id: str = Field(...)
    team_id: int = Field(...)
    season_id: int = Field(..., ge=1946)
    is_home: int = Field(..., ge=0, le=1)
    fgm: int | None = Field(None, ge=0)
    fga: int | None = Field(None, ge=0)
    fg_pct: float | None = Field(None, ge=0.0, le=1.0)
    fg3m: int | None = Field(None, ge=0)
    fg3a: int | None = Field(None, ge=0)
    fg3_pct: float | None = Field(None, ge=0.0, le=1.0)
    ftm: int | None = Field(None, ge=0)
    fta: int | None = Field(None, ge=0)
    ft_pct: float | None = Field(None, ge=0.0, le=1.0)
    oreb: int | None = Field(None, ge=0)
    dreb: int | None = Field(None, ge=0)
    reb: int | None = Field(None, ge=0)
    ast: int | None = Field(None, ge=0)
    stl: int | None = Field(None, ge=0)
    blk: int | None = Field(None, ge=0)
    tov: int | None = Field(None, ge=0)
    pf: int | None = Field(None, ge=0)
    pts: int | None = Field(None, ge=0)
    plus_minus: int | None = Field(None)
    pace: float | None = Field(None, ge=0)

    @field_validator("game_id")
    @classmethod
    def validate_game_id(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{10}$", v):
            raise ValueError(f"game_id must be 10 digits, got '{v}'")
        return v


# ---------------------------------------------------------------------------
# Box Score — Advanced
# ---------------------------------------------------------------------------


class BoxScoreAdvancedRowCreate(BaseModel):
    """Maps to player_game_log_advanced."""

    game_id: str = Field(...)
    player_id: int = Field(...)
    team_id: int = Field(...)
    minutes_played: float | None = Field(None, ge=0, le=100)
    off_rating: float | None = Field(None)
    def_rating: float | None = Field(None)
    net_rating: float | None = Field(None)
    ast_pct: float | None = Field(None, ge=0.0, le=1.0)
    ast_to_tov: float | None = Field(None, ge=0)
    ast_ratio: float | None = Field(None, ge=0)
    oreb_pct: float | None = Field(None, ge=0.0, le=1.0)
    dreb_pct: float | None = Field(None, ge=0.0, le=1.0)
    reb_pct: float | None = Field(None, ge=0.0, le=1.0)
    tov_pct: float | None = Field(None, ge=0.0, le=1.0)
    efg_pct: float | None = Field(None, ge=0.0, le=1.0)
    ts_pct: float | None = Field(None, ge=0.0, le=1.0)
    usg_pct: float | None = Field(None, ge=0.0, le=1.0)
    pace: float | None = Field(None, ge=0)
    pie: float | None = Field(None)

    @field_validator("game_id")
    @classmethod
    def validate_game_id(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{10}$", v):
            raise ValueError(f"game_id must be 10 digits, got '{v}'")
        return v


# ---------------------------------------------------------------------------
# Box Score — Hustle
# ---------------------------------------------------------------------------


class BoxScoreHustleRowCreate(BaseModel):
    """Maps to player_game_log_hustle (2015-16+)."""

    game_id: str = Field(...)
    player_id: int = Field(...)
    team_id: int = Field(...)
    minutes_played: float | None = Field(None, ge=0, le=100)
    contested_shots: int | None = Field(None, ge=0)
    contested_shots_2pt: int | None = Field(None, ge=0)
    contested_shots_3pt: int | None = Field(None, ge=0)
    deflections: int | None = Field(None, ge=0)
    charges_drawn: int | None = Field(None, ge=0)
    screen_assists: int | None = Field(None, ge=0)
    screen_ast_pts: int | None = Field(None, ge=0)
    box_outs: int | None = Field(None, ge=0)
    off_box_outs: int | None = Field(None, ge=0)
    def_box_outs: int | None = Field(None, ge=0)
    loose_balls_recovered: int | None = Field(None, ge=0)

    @field_validator("game_id")
    @classmethod
    def validate_game_id(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{10}$", v):
            raise ValueError(f"game_id must be 10 digits, got '{v}'")
        return v


# ---------------------------------------------------------------------------
# Play-By-Play
# ---------------------------------------------------------------------------

# NBA.com event_msg_type codes 1-21
VALID_EVENT_TYPES = set(range(1, 22))


class PlayByPlayEventCreate(BaseModel):
    """Maps to the play_by_play table."""

    game_id: str = Field(...)
    event_num: int = Field(..., ge=0)
    period: int = Field(..., ge=1, le=10, description="Period 1-4 regular; 5-10 for overtimes")
    pc_time: int | None = Field(None, ge=0, description="Clock in seconds remaining")
    wc_time: str | None = Field(None, description="Wall clock time string")
    event_type: int = Field(..., description="NBA.com event_msg_type 1-21")
    event_action_type: int | None = Field(None, ge=0)
    description_home: str | None = Field(None)
    description_visitor: str | None = Field(None)
    score_home: int | None = Field(None, ge=0)
    score_visitor: int | None = Field(None, ge=0)
    score_margin: int | None = Field(None)
    player1_id: int | None = Field(None)
    player1_team_id: int | None = Field(None)
    player2_id: int | None = Field(None)
    player2_team_id: int | None = Field(None)
    player3_id: int | None = Field(None)
    player3_team_id: int | None = Field(None)
    video_available: int = Field(default=0, ge=0, le=1)

    @field_validator("game_id")
    @classmethod
    def validate_game_id(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{10}$", v):
            raise ValueError(f"game_id must be 10 digits, got '{v}'")
        return v

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: int) -> int:
        if v not in VALID_EVENT_TYPES:
            raise ValueError(f"event_type must be 1-21, got {v}")
        return v


# ---------------------------------------------------------------------------
# Shot Chart
# ---------------------------------------------------------------------------


class ShotChartRowCreate(BaseModel):
    """Maps to the shot_chart table."""

    game_id: str = Field(...)
    player_id: int = Field(...)
    team_id: int = Field(...)
    period: int = Field(..., ge=1, le=10)
    minutes_remaining: int | None = Field(None, ge=0, le=12)
    seconds_remaining: int | None = Field(None, ge=0, le=59)
    action_type: str | None = Field(None, description="'Jump Shot', 'Layup', etc.")
    shot_type: str | None = Field(None, description="'2PT Field Goal' or '3PT Field Goal'")
    shot_zone_basic: str | None = Field(None)
    shot_zone_area: str | None = Field(None)
    shot_zone_range: str | None = Field(None)
    shot_distance: int | None = Field(None, ge=0, le=100, description="Distance in feet")
    loc_x: int | None = Field(None, description="Tenths of feet from basket (x-axis)")
    loc_y: int | None = Field(None, description="Tenths of feet from basket (y-axis)")
    shot_made_flag: int = Field(..., ge=0, le=1, description="1=made, 0=missed")
    htm: str | None = Field(None, description="Home team abbreviation during this possession")
    vtm: str | None = Field(None, description="Visitor team abbreviation")

    @field_validator("game_id")
    @classmethod
    def validate_game_id(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{10}$", v):
            raise ValueError(f"game_id must be 10 digits, got '{v}'")
        return v

    @field_validator("shot_type")
    @classmethod
    def validate_shot_type(cls, v: str | None) -> str | None:
        if v is not None and v not in {"2PT Field Goal", "3PT Field Goal"}:
            # Accept minor API variants silently
            pass
        return v


# ---------------------------------------------------------------------------
# Coach & Coach Stint
# ---------------------------------------------------------------------------


class CoachCreate(BaseModel):
    """Maps to the coach table."""

    coach_id: int = Field(..., description="NBA.com coach ID")
    first_name: str = Field(...)
    last_name: str = Field(...)
    full_name: str = Field(...)
    birthdate: str | None = Field(None, description="ISO date")
    college: str | None = Field(None)
    is_active: int = Field(default=1, ge=0, le=1)


class CoachStintCreate(BaseModel):
    """Maps to the coach_stint table."""

    coach_id: int = Field(..., description="FK → coach.coach_id")
    team_id: int = Field(..., description="FK → team.team_id")
    season_id: int = Field(..., ge=1946, description="FK → season.season_id")
    coach_type: str = Field(
        ...,
        description="'Head Coach', 'Assistant', 'Interim Head Coach'",
    )
    sort_sequence: int | None = Field(None, ge=0)
    date_hired: str | None = Field(None, description="ISO date")
    date_fired: str | None = Field(None, description="ISO date")
    wins: int | None = Field(None, ge=0)
    losses: int | None = Field(None, ge=0)
    win_pct: float | None = Field(None, ge=0.0, le=1.0)

    @field_validator("coach_type")
    @classmethod
    def validate_coach_type(cls, v: str) -> str:
        valid = {"Head Coach", "Assistant", "Interim Head Coach", "Assistant Coach"}
        if v not in valid:
            # Accept slight variants
            if "interim" in v.lower():
                return "Interim Head Coach"
            if "assistant" in v.lower():
                return "Assistant"
            if "head" in v.lower():
                return "Head Coach"
        return v


# ---------------------------------------------------------------------------
# Draft Pick
# ---------------------------------------------------------------------------


class DraftPickCreate(BaseModel):
    """Maps to the draft table."""

    draft_year: int = Field(..., ge=1947, le=2099)
    draft_round: int = Field(..., ge=1, le=10)
    draft_number: int = Field(..., ge=1)
    team_id: int | None = Field(None, description="FK → team.team_id (drafting team)")
    player_id: int | None = Field(None, description="FK → player.player_id")
    organization: str | None = Field(None, description="College or country of origin")
    organization_type: str | None = Field(
        None,
        description="'College', 'International', 'HS', 'Two Year College', etc.",
    )


# ---------------------------------------------------------------------------
# Draft Combine
# ---------------------------------------------------------------------------


class DraftCombineAnthroCreate(BaseModel):
    """Maps to draft_combine (anthropometric measurements)."""

    player_id: int = Field(...)
    draft_year: int = Field(..., ge=2000)
    height_wo_shoes_inches: float | None = Field(None, gt=0, le=120)
    height_w_shoes_inches: float | None = Field(None, gt=0, le=120)
    weight_lbs: float | None = Field(None, gt=0, le=400)
    wingspan_inches: float | None = Field(None, gt=0, le=120)
    standing_reach_inches: float | None = Field(None, gt=0, le=140)
    body_fat_pct: float | None = Field(None, ge=0, le=100)
    hand_length_inches: float | None = Field(None, gt=0, le=20)
    hand_width_inches: float | None = Field(None, gt=0, le=20)
    bench_press_reps: int | None = Field(None, ge=0)
    vertical_leap_standing_inches: float | None = Field(None, ge=0)
    vertical_leap_max_inches: float | None = Field(None, ge=0)
    lane_agility_time_sec: float | None = Field(None, gt=0)
    three_quarter_sprint_sec: float | None = Field(None, gt=0)

    @model_validator(mode="after")
    def check_heights(self) -> DraftCombineAnthroCreate:
        if (
            self.height_wo_shoes_inches is not None
            and self.height_w_shoes_inches is not None
            and self.height_w_shoes_inches < self.height_wo_shoes_inches
        ):
            raise ValueError("height_w_shoes must be >= height_wo_shoes")
        if (
            self.vertical_leap_standing_inches is not None
            and self.vertical_leap_max_inches is not None
        ) and self.vertical_leap_max_inches < self.vertical_leap_standing_inches:
            raise ValueError("vertical_leap_max must be >= vertical_leap_standing")
        return self


# ---------------------------------------------------------------------------
# Award
# ---------------------------------------------------------------------------

AWARD_TYPES = {
    "MVP",
    "Finals MVP",
    "ROY",
    "DPOY",
    "MIP",
    "Sixth Man",
    "All-NBA",
    "All-Defensive",
    "All-Star",
    "All-Rookie",
    "Olympic",
    "Coach of the Year",
    "Executive of the Year",
    "Sportsmanship",
    "Twyman-Stokes",
    "J. Walter Kennedy",
    "Arthur Ashe",
}

AWARD_TIERS = {"1st Team", "2nd Team", "3rd Team", None}


class AwardCreate(BaseModel):
    """Maps to the award table."""

    player_id: int = Field(..., description="FK → player.player_id")
    season_id: int = Field(..., ge=1946, description="FK → season.season_id")
    award_type: str = Field(..., description="Award category string")
    award_tier: str | None = Field(None, description="'1st Team', '2nd Team', etc.")
    conference: str | None = Field(None, description="For conference-specific awards")
    vote_points: int | None = Field(None, ge=0)
    first_place_votes: int | None = Field(None, ge=0)
    award_rank: int | None = Field(None, ge=1)
    voting_share_pct: float | None = Field(None, ge=0.0, le=100.0)

    @field_validator("award_type")
    @classmethod
    def validate_award_type(cls, v: str) -> str:
        # Normalise common variants
        mapping = {
            "Most Valuable Player": "MVP",
            "Rookie of the Year": "ROY",
            "Defensive Player of the Year": "DPOY",
            "Most Improved Player": "MIP",
        }
        return mapping.get(v, v)


# ---------------------------------------------------------------------------
# Player Season Stats
# ---------------------------------------------------------------------------


class PlayerSeasonStatsCreate(BaseModel):
    """Maps to the player_season_stats table."""

    player_id: int = Field(...)
    team_id: int = Field(
        default=0,
        description="0 = TOT sentinel for multi-team aggregate rows",
    )
    season_id: int = Field(..., ge=1946)
    stat_type: Literal["Regular Season", "Playoffs", "All-Star"] = Field(
        default="Regular Season",
    )
    games_played: int | None = Field(None, ge=0)
    games_started: int | None = Field(None, ge=0)
    minutes_played: float | None = Field(None, ge=0)
    fgm: float | None = Field(None, ge=0)
    fga: float | None = Field(None, ge=0)
    fg_pct: float | None = Field(None, ge=0.0, le=1.0)
    fg3m: float | None = Field(None, ge=0)
    fg3a: float | None = Field(None, ge=0)
    fg3_pct: float | None = Field(None, ge=0.0, le=1.0)
    ftm: float | None = Field(None, ge=0)
    fta: float | None = Field(None, ge=0)
    ft_pct: float | None = Field(None, ge=0.0, le=1.0)
    oreb: float | None = Field(None, ge=0)
    dreb: float | None = Field(None, ge=0)
    reb: float | None = Field(None, ge=0)
    ast: float | None = Field(None, ge=0)
    stl: float | None = Field(None, ge=0)
    blk: float | None = Field(None, ge=0)
    tov: float | None = Field(None, ge=0)
    pf: float | None = Field(None, ge=0)
    pts: float | None = Field(None, ge=0)
    # Advanced
    off_rating: float | None = Field(None)
    def_rating: float | None = Field(None)
    net_rating: float | None = Field(None)
    ts_pct: float | None = Field(None, ge=0.0, le=1.0)
    efg_pct: float | None = Field(None, ge=0.0, le=1.0)
    usg_pct: float | None = Field(None, ge=0.0, le=1.0)
    per: float | None = Field(None)
    ws: float | None = Field(None)
    bpm: float | None = Field(None)
    vorp: float | None = Field(None)


# ---------------------------------------------------------------------------
# Playoff Series
# ---------------------------------------------------------------------------


class PlayoffSeriesCreate(BaseModel):
    """Maps to the playoff_series table."""

    series_id: str = Field(..., description="e.g. '2024_E1_BOS_MIA'")
    season_id: int = Field(..., ge=1946)
    round: str = Field(
        ...,
        description="'First Round', 'Conference Semifinals', 'Conference Finals', 'Finals'",
    )
    conference: str | None = Field(None, description="'East' or 'West' or None for Finals")
    home_team_id: int = Field(...)
    away_team_id: int = Field(...)
    home_team_wins: int = Field(default=0, ge=0, le=4)
    away_team_wins: int = Field(default=0, ge=0, le=4)
    winner_team_id: int | None = Field(None)
    series_length: int | None = Field(None, ge=1, le=7)

    @model_validator(mode="after")
    def check_series_logic(self) -> PlayoffSeriesCreate:
        if self.winner_team_id is not None and self.winner_team_id not in {
            self.home_team_id,
            self.away_team_id,
        }:
            raise ValueError("winner_team_id must be home or away team")
        total = (self.home_team_wins or 0) + (self.away_team_wins or 0)
        if self.series_length is not None and total > self.series_length:
            raise ValueError("Total wins cannot exceed series_length")
        return self


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------

TRANSACTION_TYPES = {"Trade", "Sign", "Waive", "Two-Way", "Extension", "Convert"}


class TransactionCreate(BaseModel):
    """Maps to the transaction table."""

    transaction_id: str = Field(..., description="UUID string")
    transaction_date: str = Field(..., description="ISO date YYYY-MM-DD")
    transaction_type: str = Field(...)
    player_id: int = Field(..., description="FK → player.player_id")
    from_team_id: int | None = Field(None)
    to_team_id: int | None = Field(None)
    notes: str | None = Field(None)
    source: str | None = Field(None, description="'Basketball-Reference', 'RealGM'")
    trade_details: str | None = Field(None)
    players_involved: str | None = Field(None)
    draft_picks_involved: str | None = Field(None)

    @field_validator("transaction_date")
    @classmethod
    def validate_date(cls, v: str) -> str:
        import re  # noqa: PLC0415

        if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            raise ValueError(f"transaction_date must be YYYY-MM-DD, got '{v}'")
        return v


# ---------------------------------------------------------------------------
# Player ID Cross-Reference
# ---------------------------------------------------------------------------

ID_SYSTEMS = {"basketball_reference", "aba_encyclopedia", "realgm", "espn", "rotowire"}


class PlayerIdXrefCreate(BaseModel):
    """Maps to the player_id_xref table."""

    player_id: int = Field(..., description="FK → player.player_id")
    id_system: str = Field(..., description="External ID system name")
    external_id: str = Field(..., description="ID value in the external system")

    @field_validator("id_system")
    @classmethod
    def validate_id_system(cls, v: str) -> str:
        # Accept any lowercase alphanumeric system name; log unknown ones
        return v.lower()


# ---------------------------------------------------------------------------
# Player Bio (for CommonPlayerInfo enrichment)
# ---------------------------------------------------------------------------


class PlayerBioCreate(BaseModel):
    """
    Enrichment fields fetched from nba_api CommonPlayerInfo per player.

    Used by PlayerBioEnrichmentIngestor to fill gaps in the player table
    left by the bulk CommonAllPlayers call (which omits height, weight,
    position, college, birthplace, and draft details).
    """

    player_id: int = Field(...)
    birthdate: str | None = Field(None, description="ISO date")
    birthplace_city: str | None = Field(None)
    birthplace_state: str | None = Field(None)
    birthplace_country: str | None = Field(None)
    height_inches: float | None = Field(None, ge=60, le=120)
    weight_lbs: float | None = Field(None, ge=100, le=400)
    position: str | None = Field(None)
    primary_position: str | None = Field(None)
    jersey_number: str | None = Field(None)
    college: str | None = Field(None)
    country: str | None = Field(None)
    draft_year: int | None = Field(None, ge=1947)
    draft_round: int | None = Field(None, ge=1, le=10)
    draft_number: int | None = Field(None, ge=1)
    bbref_id: str | None = Field(None)
    high_school: str | None = Field(None)
