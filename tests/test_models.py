"""Tests for Pydantic models."""

import pytest
from pydantic import ValidationError


def test_league_model():
    """Test League model validation."""
    from nba_vault.models.league import League, LeagueCreate

    # Valid league
    league_data = {
        "league_id": "NBA",
        "league_name": "National Basketball Association",
        "founded_year": 1949,
        "folded_year": None,
    }
    league = LeagueCreate(**league_data)
    assert league.league_id == "NBA"
    assert league.folded_year is None

    # Convert to full model
    full_league = League.model_validate(league_data)
    assert full_league.league_id == "NBA"


def test_player_model():
    """Test Player model validation."""
    from nba_vault.models.player import PlayerCreate

    # Valid player
    player_data = {
        "player_id": 2544,
        "first_name": "LeBron",
        "last_name": "James",
        "full_name": "LeBron James",
        "display_name": "LeBron James",
        "birthdate": "1984-12-30",
        "birthplace_city": "Akron",
        "birthplace_state": "Ohio",
        "birthplace_country": "USA",
        "height_inches": 80.0,
        "weight_lbs": 250.0,
        "position": "F-G",
        "primary_position": "F",
        "jersey_number": "23",
        "college": None,
        "country": "USA",
        "draft_year": 2003,
        "draft_round": 1,
        "draft_number": 1,
        "is_active": True,
        "from_year": 2003,
        "to_year": None,
        "bbref_id": "jamesle01",
        "data_availability_flags": 0,
    }
    player = PlayerCreate(**player_data)
    assert player.player_id == 2544
    assert player.is_active is True
    assert player.data_availability_flags == 0


def test_game_model():
    """Test Game model validation."""
    from nba_vault.models.game import GameCreate

    # Valid game
    game_data = {
        "game_id": "0022400001",
        "season_id": 2024,
        "game_date": "2024-10-25",
        "game_type": "Regular Season",
        "home_team_id": 1610612739,
        "away_team_id": 1610612738,
        "home_team_score": 110,
        "away_team_score": 104,
        "winner_team_id": 1610612739,
        "overtime_periods": 0,
        "data_availability_flags": 1,  # Box score traditional
    }
    game = GameCreate(**game_data)
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
            player_id="not_an_int",  # Should be int
            first_name="Test",
            last_name="Player",
            full_name="Test Player",
        )
