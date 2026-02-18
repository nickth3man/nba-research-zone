"""Tests for Basketball Reference client (now NBA.com API-backed)."""

from unittest.mock import MagicMock, patch

import pytest

from nba_vault.ingestion.basketball_reference import BasketballReferenceClient
from nba_vault.utils.cache import ContentCache
from nba_vault.utils.rate_limit import RateLimiter

_PATCH_TARGET = "nba_api.stats.endpoints.commonallplayers.CommonAllPlayers"

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
# get_players - cache hit (no API call needed)
# ---------------------------------------------------------------------------


def test_get_players_cache_hit(tmp_path):
    """Returns cached data without hitting the API."""
    cache = ContentCache(cache_dir=tmp_path)
    cached = [{"slug": "2544", "name": "LeBron James"}]
    cache.set("nba_api_all_players_2023-24", cached)

    client = BasketballReferenceClient(cache=cache)
    # Cache hit — no API call needed, no patch required
    with patch(_PATCH_TARGET) as mock_api:
        result = client.get_players(season_end_year=2024)
        mock_api.assert_not_called()

    assert result == cached


def test_get_players_cache_hit_all_seasons(tmp_path):
    """Cache key uses season string when season_end_year is None (defaults to 2024)."""
    cache = ContentCache(cache_dir=tmp_path)
    cached = [{"slug": "1", "name": "Test Player"}]
    cache.set("nba_api_all_players_2023-24", cached)

    client = BasketballReferenceClient(cache=cache)
    with patch(_PATCH_TARGET) as mock_api:
        result = client.get_players(season_end_year=None)
        mock_api.assert_not_called()

    assert result == cached


# ---------------------------------------------------------------------------
# get_players - successful scrape
# ---------------------------------------------------------------------------


def _make_nba_api_row(**kwargs):
    """Build a mock NBA.com CommonAllPlayers row dict."""
    defaults = {
        "PERSON_ID": 2544,
        "DISPLAY_FIRST_LAST": "LeBron James",
        "PLAYER_SLUG": "lebron_james",
        "TEAM_ID": 1610612747,
        "TEAM_ABBREVIATION": "LAL",
        "FROM_YEAR": "2003",
        "TO_YEAR": "2024",
        "ROSTERSTATUS": 1,
    }
    defaults.update(kwargs)
    return defaults


def _make_mock_result(rows):
    """Build a mock CommonAllPlayers result."""
    mock_result = MagicMock()
    mock_result.get_dict.return_value = {
        "resultSets": [
            {
                "headers": list(rows[0].keys()) if rows else [],
                "rowSet": [list(r.values()) for r in rows],
            }
        ]
    }
    return mock_result


def test_get_players_success(tmp_path):
    """Returns a list of player dicts and populates the cache."""
    cache = ContentCache(cache_dir=tmp_path)
    rows = [_make_nba_api_row()]

    client = BasketballReferenceClient(cache=cache)

    with patch(_PATCH_TARGET, return_value=_make_mock_result(rows)):
        result = client.get_players(season_end_year=2024)

    assert len(result) == 1
    assert result[0]["slug"] == "2544"
    assert result[0]["name"] == "LeBron James"
    assert cache.get("nba_api_all_players_2023-24") is not None


def test_get_players_empty_response(tmp_path):
    """Returns empty list when API returns no data."""
    cache = ContentCache(cache_dir=tmp_path)

    mock_result = MagicMock()
    mock_result.get_dict.return_value = {"resultSets": [{"headers": [], "rowSet": []}]}

    client = BasketballReferenceClient(cache=cache)

    with patch(_PATCH_TARGET, return_value=mock_result):
        result = client.get_players(season_end_year=2024)

    assert result == []


def test_get_players_none_season_defaults_to_2024(tmp_path):
    """When season_end_year is None, defaults to 2024 season."""
    cache = ContentCache(cache_dir=tmp_path)
    rows = [_make_nba_api_row()]

    client = BasketballReferenceClient(cache=cache)

    with patch(_PATCH_TARGET, return_value=_make_mock_result(rows)):
        result = client.get_players(season_end_year=None)

    assert len(result) == 1


def test_get_players_propagates_exception(tmp_path):
    """Non-cache, non-import exceptions from the API propagate to caller."""
    cache = ContentCache(cache_dir=tmp_path)

    client = BasketballReferenceClient(cache=cache)

    with (
        patch(_PATCH_TARGET, side_effect=ConnectionError("timeout")),
        pytest.raises(ConnectionError),
    ):
        client.get_players(season_end_year=2024)


# ---------------------------------------------------------------------------
# get_player_info
# ---------------------------------------------------------------------------


def test_get_player_info_cache_hit(tmp_path):
    """Returns cached player info without calling the API."""
    cache = ContentCache(cache_dir=tmp_path)
    cached = {"slug": "2544", "data": {}}
    cache.set("player_info_2544", cached)

    client = BasketballReferenceClient(cache=cache)
    result = client.get_player_info("2544")
    assert result == cached


def test_get_player_info_success(tmp_path):
    """Fetches, caches and returns player info dict."""
    cache = ContentCache(cache_dir=tmp_path)
    client = BasketballReferenceClient(cache=cache)

    result = client.get_player_info("2544")

    assert result["slug"] == "2544"
    assert cache.get("player_info_2544") is not None


def test_get_player_info_returns_slug(tmp_path):
    """get_player_info always returns a dict with the requested slug."""
    cache = ContentCache(cache_dir=tmp_path)
    client = BasketballReferenceClient(cache=cache)

    result = client.get_player_info("jamesle01")
    assert result["slug"] == "jamesle01"
