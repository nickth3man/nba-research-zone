"""
Pydantic models for advanced NBA statistics and missing features.

This module contains validation models for all the new data types
identified in the schema audit, including tracking data, lineup data,
possession data, and other advanced metrics.
"""

from datetime import date

from pydantic import BaseModel, Field, field_validator


class TeamGameOtherStatsCreate(BaseModel):
    """Model for team game other stats (paint points, fast break, etc.)"""

    game_id: str = Field(..., description="NBA.com 10-char game ID")
    team_id: int = Field(..., description="Team identifier")
    season_id: int = Field(..., description="Season identifier")
    points_paint: int | None = Field(None, description="Points in the paint")
    points_second_chance: int | None = Field(None, description="Second chance points")
    points_fast_break: int | None = Field(None, description="Fast break points")
    largest_lead: int | None = Field(None, description="Largest lead")
    lead_changes: int | None = Field(None, description="Number of lead changes")
    times_tied: int | None = Field(None, description="Number of times tied")
    team_turnovers: int | None = Field(None, description="Team turnovers")
    total_turnovers: int | None = Field(None, description="Total turnovers")
    team_rebounds: int | None = Field(None, description="Team rebounds")
    points_off_turnovers: int | None = Field(None, description="Points off turnovers")


class PlayerGameTrackingCreate(BaseModel):
    """Model for player game tracking data (speed & distance, 2013-14+)"""

    game_id: str = Field(..., description="NBA.com 10-char game ID")
    player_id: int = Field(..., description="Player identifier")
    team_id: int = Field(..., description="Team identifier")
    season_id: int = Field(..., description="Season identifier")
    minutes_played: float | None = Field(None, ge=0, description="Minutes played")
    distance_miles: float | None = Field(None, ge=0, description="Total distance covered")
    distance_miles_offensive: float | None = Field(None, ge=0, description="Offensive distance")
    distance_miles_defensive: float | None = Field(None, ge=0, description="Defensive distance")
    speed_mph_avg: float | None = Field(None, ge=0, description="Average speed")
    speed_mph_max: float | None = Field(None, ge=0, description="Maximum speed")
    touches: int | None = Field(None, ge=0, description="Total touches")
    touches_catch_shoot: int | None = Field(None, ge=0, description="Catch & shoot touches")
    touches_paint: int | None = Field(None, ge=0, description="Paint touches")
    touches_post_up: int | None = Field(None, ge=0, description="Post-up touches")
    drives: int | None = Field(None, ge=0, description="Number of drives")
    drives_pts: int | None = Field(None, ge=0, description="Points on drives")
    pull_up_shots: int | None = Field(None, ge=0, description="Pull-up shot attempts")
    pull_up_shots_made: int | None = Field(None, ge=0, description="Pull-up shots made")


class LineupCreate(BaseModel):
    """Model for lineup combinations"""

    lineup_id: str = Field(..., description="Unique lineup identifier")
    season_id: int = Field(..., description="Season identifier")
    team_id: int = Field(..., description="Team identifier")
    player_1_id: int = Field(..., description="First player ID")
    player_2_id: int = Field(..., description="Second player ID")
    player_3_id: int = Field(..., description="Third player ID")
    player_4_id: int = Field(..., description="Fourth player ID")
    player_5_id: int = Field(..., description="Fifth player ID")
    minutes_played: float = Field(0, ge=0, description="Total minutes played")
    possessions: int = Field(0, ge=0, description="Total possessions")
    points_scored: int = Field(0, ge=0, description="Points scored")
    points_allowed: int = Field(0, ge=0, description="Points allowed")
    off_rating: float | None = Field(None, description="Offensive rating")
    def_rating: float | None = Field(None, description="Defensive rating")
    net_rating: float | None = Field(None, description="Net rating")

    @field_validator("player_1_id", "player_2_id", "player_3_id", "player_4_id", "player_5_id")
    @classmethod
    def validate_unique_players(cls, v, info):
        """Ensure all five players are unique"""
        players = [
            info.data.get("player_1_id"),
            info.data.get("player_2_id"),
            info.data.get("player_3_id"),
            info.data.get("player_4_id"),
            info.data.get("player_5_id"),
        ]
        if len(set(players)) != 5:
            raise ValueError("All five players must be unique")
        return v


class LineupGameLogCreate(BaseModel):
    """Model for lineup performance in individual games"""

    lineup_id: str = Field(..., description="Lineup identifier")
    game_id: str = Field(..., description="NBA.com 10-char game ID")
    team_id: int = Field(..., description="Team identifier")
    minutes_played: float | None = Field(None, ge=0, description="Minutes played")
    plus_minus: int | None = Field(None, description="Plus/minus rating")
    possessions: int | None = Field(None, ge=0, description="Possessions")
    points_scored: int | None = Field(None, ge=0, description="Points scored")
    points_allowed: int | None = Field(None, ge=0, description="Points allowed")


class PossessionCreate(BaseModel):
    """Model for possession-level data"""

    game_id: str = Field(..., description="NBA.com 10-char game ID")
    possession_number: int = Field(..., ge=1, description="Possession number in game")
    period: int = Field(..., ge=1, le=4, description="Game period")
    start_time: float = Field(..., ge=0, le=720, description="Period start time (seconds)")
    end_time: float | None = Field(None, ge=0, le=720, description="Period end time (seconds)")
    team_id: int = Field(..., description="Team identifier")
    points_scored: int = Field(0, ge=0, description="Points scored in possession")
    play_type: str | None = Field(None, description="Type of play (isolation, transition, etc.)")
    shot_clock_start: float | None = Field(None, ge=0, le=24, description="Shot clock start")
    shot_clock_end: float | None = Field(None, ge=0, le=24, description="Shot clock end")
    duration_seconds: float | None = Field(None, ge=0, description="Possession duration")
    outcome_type: str | None = Field(
        None, description="Outcome (made_shot, missed_shot, turnover, foul)"
    )


class InjuryCreate(BaseModel):
    """Model for player injury data"""

    player_id: int = Field(..., description="Player identifier")
    team_id: int | None = Field(None, description="Team identifier")
    injury_date: date = Field(..., description="Date of injury")
    injury_type: str | None = Field(None, description="Type of injury")
    body_part: str | None = Field(None, description="Injured body part")
    status: str = Field(..., description="Injury status")
    games_missed: int = Field(0, ge=0, description="Games missed")
    return_date: date | None = Field(None, description="Expected return date")
    notes: str | None = Field(None, description="Additional notes")


class PlayerContractCreate(BaseModel):
    """Model for player contract data"""

    player_id: int = Field(..., description="Player identifier")
    team_id: int = Field(..., description="Team identifier")
    season_start: int = Field(..., ge=1946, description="Contract start season")
    season_end: int = Field(..., ge=1946, description="Contract end season")
    salary_amount: float | None = Field(None, ge=0, description="Salary amount")
    contract_type: str | None = Field(
        None, description="Contract type (Rookie, Veteran, MLE, etc.)"
    )
    player_option: int = Field(0, ge=0, le=1, description="Player option flag")
    team_option: int = Field(0, ge=0, le=1, description="Team option flag")
    early_termination: int = Field(0, ge=0, le=1, description="Early termination flag")
    guaranteed_money: float | None = Field(None, ge=0, description="Guaranteed money")
    cap_hit: float | None = Field(None, ge=0, description="Salary cap hit")
    dead_money: float | None = Field(None, ge=0, description="Dead money")

    @field_validator("season_end")
    @classmethod
    def validate_season_range(cls, v, info):
        """Ensure contract end season is after start season"""
        if "season_start" in info.data and v < info.data["season_start"]:
            raise ValueError("Contract end season must be after start season")
        return v


class DraftCombineCreate(BaseModel):
    """Model for NBA Draft Combine data"""

    player_id: int = Field(..., description="Player identifier")
    draft_year: int = Field(..., ge=1946, description="Draft year")
    height_wo_shoes_inches: float | None = Field(None, gt=0, description="Height without shoes")
    height_w_shoes_inches: float | None = Field(None, gt=0, description="Height with shoes")
    weight_lbs: float | None = Field(None, gt=0, description="Weight in pounds")
    wingspan_inches: float | None = Field(None, gt=0, description="Wingspan")
    standing_reach_inches: float | None = Field(None, gt=0, description="Standing reach")
    body_fat_pct: float | None = Field(None, ge=0, le=100, description="Body fat percentage")
    hand_length_inches: float | None = Field(None, gt=0, description="Hand length")
    hand_width_inches: float | None = Field(None, gt=0, description="Hand width")
    bench_press_reps: int | None = Field(None, ge=0, description="Bench press reps")
    vertical_leap_standing_inches: float | None = Field(
        None, ge=0, description="Standing vertical leap"
    )
    vertical_leap_max_inches: float | None = Field(None, ge=0, description="Maximum vertical leap")
    lane_agility_time_sec: float | None = Field(None, gt=0, description="Lane agility time")
    three_quarter_sprint_sec: float | None = Field(
        None, gt=0, description="Three-quarter sprint time"
    )


class PlayerGameMiscStatsCreate(BaseModel):
    """Model for miscellaneous player game statistics"""

    game_id: str = Field(..., description="NBA.com 10-char game ID")
    player_id: int = Field(..., description="Player identifier")
    team_id: int = Field(..., description="Team identifier")
    season_id: int = Field(..., description="Season identifier")
    plus_minus: int | None = Field(None, description="Plus/minus rating")
    double_double: int = Field(0, ge=0, le=1, description="Double-double flag")
    triple_double: int = Field(0, ge=0, le=1, description="Triple-double flag")
    quadruple_double: int = Field(0, ge=0, le=1, description="Quadruple-double flag")
    five_by_five: int = Field(0, ge=0, le=1, description="Five-by-five flag")
    points_generated: int | None = Field(None, ge=0, description="Points + assists*2")
    game_score: float | None = Field(None, description="Game score rating")
    efficiency: float | None = Field(None, description="Efficiency rating")


class PlayerSeasonMetadataCreate(BaseModel):
    """Model for player season metadata"""

    player_id: int = Field(..., description="Player identifier")
    season_id: int = Field(..., description="Season identifier")
    team_id: int = Field(..., description="Team identifier")
    age: int | None = Field(None, ge=18, le=50, description="Player age")
    games_started: int | None = Field(None, ge=0, description="Games started")
    minutes_per_game: float | None = Field(None, ge=0, description="Minutes per game")
    experience_years: int | None = Field(None, ge=0, description="Years of experience")
    salary: float | None = Field(None, ge=0, description="Season salary")
    cap_hit: float | None = Field(None, ge=0, description="Cap hit")
    contract_year: int = Field(0, ge=0, le=1, description="Contract year flag")


class TeamSeasonAdvancedCreate(BaseModel):
    """Model for advanced team season statistics"""

    team_id: int = Field(..., description="Team identifier")
    season_id: int = Field(..., description="Season identifier")
    off_rating: float | None = Field(None, description="Offensive rating")
    def_rating: float | None = Field(None, description="Defensive rating")
    net_rating: float | None = Field(None, description="Net rating")
    pace: float | None = Field(None, ge=0, description="Pace factor")
    effective_fg_pct: float | None = Field(None, ge=0, le=1, description="Effective FG%")
    turnover_pct: float | None = Field(None, ge=0, le=1, description="Turnover percentage")
    offensive_rebound_pct: float | None = Field(None, ge=0, le=1, description="Offensive rebound %")
    free_throw_rate: float | None = Field(None, ge=0, description="Free throw rate")
    three_point_rate: float | None = Field(None, ge=0, le=1, description="3-point rate")
    true_shooting_pct: float | None = Field(None, ge=0, le=1, description="True shooting %")
