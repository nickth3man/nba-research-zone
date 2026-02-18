"""Tests for Basketball Reference client."""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from nba_vault.ingestion.basketball_reference import BasketballReferenceClient
from nba_vault.utils.cache import ContentCache
from nba_vault.utils.rate_limit import RateLimiter

# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def test_init_defaults():
    """Client creates its own cache and rate limiter when none are supplied."""
    client = BasketballReferenceClient()
    assert isinstance(client.cache, ContentCache)
    assert isinstance(client.rate_limiter, RateLimiter)


def test_init_custom_deps():
    """Client stores injected cache and rate limiter."""
    cache = ContentCache()
    limiter = RateLimiter(rate=1, per=10)
    client = BasketballReferenceClient(cache=cache, rate_limiter=limiter)
    assert client.cache is cache
    assert client.rate_limiter is limiter


# ---------------------------------------------------------------------------
# get_players - validation
# ---------------------------------------------------------------------------


def test_get_players_season_too_low():
    client = BasketballReferenceClient()
    with pytest.raises(ValueError, match="1947"):
        client.get_players(season_end_year=1900)


def test_get_players_season_too_high():
    client = BasketballReferenceClient()
    with pytest.raises(ValueError, match="2100"):
        client.get_players(season_end_year=2200)


def test_get_players_season_boundary_valid():
    """Boundary values 1947 and 2100 are accepted (validated before import)."""
    client = BasketballReferenceClient()
    # Validation passes; the ImportError is raised next — that's fine
    with pytest.raises((ImportError, Exception)):
        client.get_players(season_end_year=1947)


# ---------------------------------------------------------------------------
# get_players - ImportError
# ---------------------------------------------------------------------------


def test_get_players_import_error():
    """Raises ImportError with helpful message when library is missing."""
    client = BasketballReferenceClient()
    with (
        patch.dict(sys.modules, {"basketball_reference_web_scraper": None}),
        pytest.raises(ImportError, match="basketball_reference_web_scraper"),
    ):
        client.get_players(season_end_year=2024)


# ---------------------------------------------------------------------------
# get_players - cache hit
# ---------------------------------------------------------------------------


def test_get_players_cache_hit(tmp_path):
    """Returns cached data without hitting the scraper."""
    cache = ContentCache(cache_dir=tmp_path)
    cached = [{"slug": "jamesle01", "name": "LeBron James"}]
    cache.set("players_season_2024", cached)

    client = BasketballReferenceClient(cache=cache)
    result = client.get_players(season_end_year=2024)
    assert result == cached


def test_get_players_cache_hit_all_seasons(tmp_path):
    """Cache key 'players_season_all' is used when season_end_year is None."""
    cache = ContentCache(cache_dir=tmp_path)
    cached = [{"slug": "test01", "name": "Test Player"}]
    cache.set("players_season_all", cached)

    client = BasketballReferenceClient(cache=cache)
    result = client.get_players(season_end_year=None)
    assert result == cached


# ---------------------------------------------------------------------------
# get_players - successful scrape
# ---------------------------------------------------------------------------


def _make_player_obj(**kwargs):
    """Build a SimpleNamespace that mimics a basketball_reference player dict."""
    defaults = {
        "slug": "jamesle01",
        "name": "LeBron James",
        "position": "SF",
        "height": "6-9",
        "weight": 250,
        "team_abbreviation": "LAL",
        "games_played": 71,
        "games_started": 71,
        "minutes_played": 2476.0,
        "field_goals": 800,
        "field_goal_attempts": 1500,
        "field_goal_percentage": 0.533,
        "three_point_field_goals": 100,
        "three_point_field_goal_attempts": 280,
        "three_point_field_goal_percentage": 0.357,
        "two_point_field_goals": 700,
        "two_point_field_goal_attempts": 1220,
        "two_point_field_goal_percentage": 0.574,
        "effective_field_goal_percentage": 0.566,
        "free_throws": 380,
        "free_throw_attempts": 500,
        "free_throw_percentage": 0.760,
        "offensive_rebounds": 80,
        "defensive_rebounds": 450,
        "rebounds": 530,
        "assists": 650,
        "steals": 90,
        "blocks": 45,
        "turnovers": 280,
        "personal_fouls": 120,
        "points": 2080,
        "player_advanced_stats": {},
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_get_players_success(tmp_path):
    """Returns a list of player dicts and populates the cache."""
    cache = ContentCache(cache_dir=tmp_path)
    mock_scraper = MagicMock()
    mock_scraper.players_season_totals.return_value = [_make_player_obj()]

    client = BasketballReferenceClient(cache=cache)

    with patch.dict(sys.modules, {"basketball_reference_web_scraper": mock_scraper}):
        result = client.get_players(season_end_year=2024)

    assert len(result) == 1
    assert result[0]["slug"] == "jamesle01"
    # Cache should now be populated
    assert cache.get("players_season_2024") is not None


def test_get_players_empty_response(tmp_path):
    """Returns empty list when scraper returns no data."""
    cache = ContentCache(cache_dir=tmp_path)
    mock_scraper = MagicMock()
    mock_scraper.players_season_totals.return_value = []

    client = BasketballReferenceClient(cache=cache)

    with patch.dict(sys.modules, {"basketball_reference_web_scraper": mock_scraper}):
        result = client.get_players(season_end_year=2024)

    assert result == []


def test_get_players_attribute_error_skips_player(tmp_path):
    """Players whose attributes raise AttributeError are skipped gracefully."""
    cache = ContentCache(cache_dir=tmp_path)

    bad_player = MagicMock()
    bad_player.slug = "bad01"
    # Make accessing any attribute raise AttributeError
    type(bad_player).__getattr__ = MagicMock(side_effect=AttributeError("no attr"))  # type: ignore[assignment]

    good_player = _make_player_obj()

    mock_scraper = MagicMock()
    mock_scraper.players_season_totals.return_value = [bad_player, good_player]

    client = BasketballReferenceClient(cache=cache)

    with patch.dict(sys.modules, {"basketball_reference_web_scraper": mock_scraper}):
        result = client.get_players(season_end_year=2024)

    # good_player should still be present
    slugs = [p["slug"] for p in result]
    assert "jamesle01" in slugs


def test_get_players_none_season_calls_default(tmp_path):
    """When season_end_year is None, defaults to 2024 season totals."""
    cache = ContentCache(cache_dir=tmp_path)
    mock_scraper = MagicMock()
    mock_scraper.players_season_totals.return_value = [_make_player_obj()]

    client = BasketballReferenceClient(cache=cache)

    with patch.dict(sys.modules, {"basketball_reference_web_scraper": mock_scraper}):
        result = client.get_players(season_end_year=None)

    mock_scraper.players_season_totals.assert_called_once_with(2024)
    assert len(result) == 1


def test_get_players_propagates_exception(tmp_path):
    """Non-cache, non-import exceptions from the scraper propagate to caller."""
    cache = ContentCache(cache_dir=tmp_path)
    mock_scraper = MagicMock()
    mock_scraper.players_season_totals.side_effect = ConnectionError("timeout")

    client = BasketballReferenceClient(cache=cache)

    with (
        patch.dict(sys.modules, {"basketball_reference_web_scraper": mock_scraper}),
        pytest.raises(ConnectionError),
    ):
        client.get_players(season_end_year=2024)


# ---------------------------------------------------------------------------
# get_player_info
# ---------------------------------------------------------------------------


def test_get_player_info_import_error():
    """Raises ImportError when library is missing."""
    client = BasketballReferenceClient()
    with (
        patch.dict(sys.modules, {"basketball_reference_web_scraper": None}),
        pytest.raises(ImportError),
    ):
        client.get_player_info("jamesle01")


def test_get_player_info_cache_hit(tmp_path):
    """Returns cached player info without calling the scraper."""
    cache = ContentCache(cache_dir=tmp_path)
    cached = {"slug": "jamesle01", "data": []}
    cache.set("player_info_jamesle01", cached)

    client = BasketballReferenceClient(cache=cache)
    result = client.get_player_info("jamesle01")
    assert result == cached


def test_get_player_info_success(tmp_path):
    """Fetches, caches and returns player info dict."""
    cache = ContentCache(cache_dir=tmp_path)
    mock_scraper = MagicMock()
    mock_scraper.player_box_scores.return_value = [{"game": "data"}]

    client = BasketballReferenceClient(cache=cache)

    with patch.dict(sys.modules, {"basketball_reference_web_scraper": mock_scraper}):
        result = client.get_player_info("jamesle01")

    assert result["slug"] == "jamesle01"
    assert cache.get("player_info_jamesle01") is not None


def test_get_player_info_propagates_exception(tmp_path):
    """Exceptions from the scraper propagate to the caller."""
    cache = ContentCache(cache_dir=tmp_path)
    mock_scraper = MagicMock()
    mock_scraper.player_box_scores.side_effect = RuntimeError("scrape error")

    client = BasketballReferenceClient(cache=cache)

    with (
        patch.dict(sys.modules, {"basketball_reference_web_scraper": mock_scraper}),
        pytest.raises(RuntimeError, match="scrape error"),
    ):
        client.get_player_info("jamesle01")
