"""Tests for Pydantic models."""

import pytest
from pydantic import ValidationError


def test_league_model():
    """Test League model validation."""
    from nba_vault.models.league import League, LeagueCreate

    # Valid league
    league = LeagueCreate(
        league_id="NBA",
        league_name="National Basketball Association",
        founded_year=1949,
        folded_year=None,
    )
    assert league.league_id == "NBA"
    assert league.folded_year is None

    # Convert to full model
    full_league = League.model_validate(
        {
            "league_id": "NBA",
            "league_name": "National Basketball Association",
            "founded_year": 1949,
            "folded_year": None,
        }
    )
    assert full_league.league_id == "NBA"


def test_player_model():
    """Test Player model validation."""
    from nba_vault.models.player import PlayerCreate

    # Valid player
    player = PlayerCreate(
        player_id=2544,
        first_name="LeBron",
        last_name="James",
        full_name="LeBron James",
        display_name="LeBron James",
        birthdate="1984-12-30",
        birthplace_city="Akron",
        birthplace_state="Ohio",
        birthplace_country="USA",
        height_inches=80.0,
        weight_lbs=250.0,
        position="F-G",
        primary_position="F",
        jersey_number="23",
        college=None,
        country="USA",
        draft_year=2003,
        draft_round=1,
        draft_number=1,
        is_active=True,
        from_year=2003,
        to_year=None,
        bbref_id="jamesle01",
        data_availability_flags=0,
    )
    assert player.player_id == 2544
    assert player.is_active is True
    assert player.data_availability_flags == 0


def test_game_model():
    """Test Game model validation."""
    from nba_vault.models.game import GameCreate

    # Valid game
    game = GameCreate(
        game_id="0022400001",
        season_id=2024,
        game_date="2024-10-25",
        game_type="Regular Season",
        home_team_id=1610612739,
        away_team_id=1610612738,
        home_team_score=110,
        away_team_score=104,
        winner_team_id=1610612739,
        overtime_periods=0,
        data_availability_flags=1,  # Box score traditional
    )
    assert game.game_id == "0022400001"
    assert game.winner_team_id == game.home_team_id
    assert game.overtime_periods == 0


def test_model_validation_errors():
    """Test that validation catches invalid data."""
    from nba_vault.models.player import PlayerCreate

    # Missing required field
    with pytest.raises(ValidationError):
        PlayerCreate(
            player_id=123,
            first_name="Test",
            # Missing last_name
            full_name="Test Player",
        )

    # Invalid data type
    with pytest.raises(ValidationError):
        PlayerCreate(
            player_id="not_an_int",  # type: ignore[arg-type]  # intentionally invalid
            first_name="Test",
            last_name="Player",
            full_name="Test Player",
        )


# ---------------------------------------------------------------------------
# Additional model tests for full coverage
# ---------------------------------------------------------------------------


def test_coach_model():
    from nba_vault.models.coach import Coach, CoachCreate

    coach = CoachCreate(
        coach_id=1001,
        first_name="Steve",
        last_name="Kerr",
        full_name="Steve Kerr",
        birthdate="1965-09-27",
        college="Arizona",
        is_active=True,
    )
    assert coach.coach_id == 1001
    assert coach.is_active is True

    full = Coach.model_validate(
        {
            "coach_id": 1001,
            "first_name": "Steve",
            "last_name": "Kerr",
            "full_name": "Steve Kerr",
        }
    )
    assert full.is_active is True  # default


def test_franchise_model():
    from nba_vault.models.franchise import Franchise, FranchiseCreate

    franchise = FranchiseCreate(
        franchise_id="LAL",
        full_name="Los Angeles Lakers",
        abbreviation="LAL",
        city="Los Angeles",
        nickname="Lakers",
        league_id="NBA",
        founded_year=1947,
        folded_year=None,
        is_active=True,
    )
    assert franchise.franchise_id == "LAL"
    assert franchise.folded_year is None

    full = Franchise.model_validate(
        {
            "franchise_id": "BOS",
            "full_name": "Boston Celtics",
            "abbreviation": "BOS",
            "city": "Boston",
            "nickname": "Celtics",
            "league_id": "NBA",
            "founded_year": 1946,
        }
    )
    assert full.franchise_id == "BOS"


def test_official_model():
    from nba_vault.models.official import Official, OfficialCreate

    official = OfficialCreate(
        official_id=201,
        first_name="Tony",
        last_name="Brothers",
        full_name="Tony Brothers",
        jersey_num="25",
    )
    assert official.official_id == 201

    full = Official.model_validate(
        {
            "official_id": 202,
            "first_name": "Scott",
            "last_name": "Foster",
            "full_name": "Scott Foster",
        }
    )
    assert full.official_id == 202


def test_season_model():
    from nba_vault.models.season import Season, SeasonCreate

    season = SeasonCreate(
        season_id=2024,
        league_id="NBA",
        season_name="2023-24",
        season_type="Regular Season",
        start_date="2023-10-24",
        end_date="2024-04-14",
    )
    assert season.season_id == 2024
    assert season.league_id == "NBA"

    full = Season.model_validate(
        {
            "season_id": 2023,
            "league_id": "NBA",
            "season_name": "2022-23",
        }
    )
    assert full.season_id == 2023


def test_team_model():
    from nba_vault.models.team import Team, TeamCreate

    team = TeamCreate(
        team_id=1610612747,
        franchise_id="LAL",
        season_id=2024,
        full_name="Los Angeles Lakers",
        abbreviation="LAL",
        city="Los Angeles",
        nickname="Lakers",
        league_id="NBA",
    )
    assert team.team_id == 1610612747
    assert team.abbreviation == "LAL"

    full = Team.model_validate(
        {
            "team_id": 1610612738,
            "franchise_id": "BOS",
            "season_id": 2024,
            "full_name": "Boston Celtics",
            "abbreviation": "BOS",
            "city": "Boston",
            "nickname": "Celtics",
            "league_id": "NBA",
        }
    )
    assert full.team_id == 1610612738


# ---------------------------------------------------------------------------
# advanced_stats.py model tests
# ---------------------------------------------------------------------------


def test_team_game_other_stats_model():
    from nba_vault.models.advanced_stats import TeamGameOtherStatsCreate

    stats = TeamGameOtherStatsCreate(
        game_id="0022300001",
        team_id=1610612747,
        season_id=2023,
        points_paint=50,
        points_fast_break=15,
        largest_lead=22,
    )
    assert stats.game_id == "0022300001"
    assert stats.points_paint == 50
    assert stats.points_second_chance is None  # optional


def test_player_game_tracking_model():
    from nba_vault.models.advanced_stats import PlayerGameTrackingCreate

    tracking = PlayerGameTrackingCreate(
        game_id="0022300001",
        player_id=2544,
        team_id=1610612747,
        season_id=2023,
        distance_miles=3.2,
        speed_mph_avg=4.5,
        touches=80,
    )
    assert tracking.player_id == 2544
    assert tracking.distance_miles == 3.2


def test_lineup_create_unique_players():
    from nba_vault.models.advanced_stats import LineupCreate

    lineup = LineupCreate(
        lineup_id="L001",
        season_id=2023,
        team_id=1610612747,
        player_1_id=1,
        player_2_id=2,
        player_3_id=3,
        player_4_id=4,
        player_5_id=5,
    )
    assert lineup.lineup_id == "L001"


def test_lineup_create_duplicate_players():
    from nba_vault.models.advanced_stats import LineupCreate

    with pytest.raises(ValidationError, match="unique"):
        LineupCreate(
            lineup_id="L001",
            season_id=2023,
            team_id=1610612747,
            player_1_id=1,
            player_2_id=1,  # duplicate
            player_3_id=3,
            player_4_id=4,
            player_5_id=5,
        )


def test_injury_create_model():
    from datetime import date

    from nba_vault.models.advanced_stats import InjuryCreate

    injury = InjuryCreate(
        player_id=2544,
        team_id=1610612747,
        injury_date=date(2024, 1, 15),
        injury_type="Ankle Sprain",
        body_part="Ankle",
        status="Out",
        games_missed=3,
    )
    assert injury.player_id == 2544
    assert injury.games_missed == 3


def test_player_contract_create_valid():
    from nba_vault.models.advanced_stats import PlayerContractCreate

    contract = PlayerContractCreate(
        player_id=2544,
        team_id=1610612747,
        season_start=2023,
        season_end=2025,
        salary_amount=46_000_000.0,
        contract_type="Veteran",
    )
    assert contract.salary_amount == 46_000_000.0


def test_player_contract_end_before_start():
    from nba_vault.models.advanced_stats import PlayerContractCreate

    with pytest.raises(ValidationError, match="after start"):
        PlayerContractCreate(
            player_id=2544,
            team_id=1610612747,
            season_start=2025,
            season_end=2023,  # before start
        )


def test_team_season_advanced_model():
    from nba_vault.models.advanced_stats import TeamSeasonAdvancedCreate

    adv = TeamSeasonAdvancedCreate(
        team_id=1610612747,
        season_id=2023,
        off_rating=115.2,
        def_rating=110.5,
        net_rating=4.7,
        pace=100.1,
        true_shooting_pct=0.582,
    )
    assert adv.net_rating == 4.7
    assert adv.off_rating == 115.2


def test_lineup_game_log_model():
    from nba_vault.models.advanced_stats import LineupGameLogCreate

    log = LineupGameLogCreate(
        lineup_id="L001",
        game_id="0022300001",
        team_id=1610612747,
        minutes_played=12.5,
        plus_minus=8,
        possessions=24,
    )
    assert log.plus_minus == 8


def test_possession_create_model():
    from nba_vault.models.advanced_stats import PossessionCreate

    poss = PossessionCreate(
        game_id="0022300001",
        possession_number=1,
        period=1,
        start_time=0.0,
        team_id=1610612747,
        points_scored=2,
        play_type="isolation",
    )
    assert poss.possession_number == 1
