"""Tests for injury scrapers."""

from datetime import date
from unittest.mock import MagicMock, Mock

import pytest

from nba_vault.ingestion.scrapers.injury_scrapers import (
    ESPNInjuryScraper,
    RotowireInjuryScraper,
)


class TestBaseInjuryScraper:
    """Tests for BaseInjuryScraper utility methods.

    These tests use ESPNInjuryScraper as a concrete implementation
    to test the inherited utility methods.
    """

    def test_parse_injury_description_with_both(self):
        """Test parsing injury description with both type and body part."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        injury_type, body_part = scraper.parse_injury_description("ACL tear in left knee")

        assert injury_type == "tear"
        assert body_part == "acl"

    def test_parse_injury_description_with_only_body_part(self):
        """Test parsing injury description with only body part."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        injury_type, body_part = scraper.parse_injury_description("Left knee soreness")

        assert injury_type == "soreness"
        assert body_part == "knee"

    def test_parse_injury_description_with_only_type(self):
        """Test parsing injury description with only injury type."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        injury_type, body_part = scraper.parse_injury_description("Ankle sprain")

        assert injury_type == "sprain"
        assert body_part == "ankle"

    def test_parse_injury_description_empty(self):
        """Test parsing empty injury description."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        injury_type, body_part = scraper.parse_injury_description("")

        assert injury_type is None
        assert body_part is None

    def test_parse_injury_description_none(self):
        """Test parsing None injury description."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        injury_type, body_part = scraper.parse_injury_description(None)

        assert injury_type is None
        assert body_part is None

    def test_parse_injury_description_case_insensitive(self):
        """Test that parsing is case-insensitive."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        injury_type, body_part = scraper.parse_injury_description("KNEE SPRAIN")

        assert injury_type == "sprain"
        assert body_part == "knee"

    def test_parse_date_iso_format(self):
        """Test parsing ISO format date."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        result = scraper.parse_date("2024-01-15")

        assert result == date(2024, 1, 15)

    def test_parse_date_us_format(self):
        """Test parsing US format date."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        result = scraper.parse_date("01/15/2024")

        assert result == date(2024, 1, 15)

    def test_parse_date_short_year_format(self):
        """Test parsing short year format date."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        result = scraper.parse_date("01/15/24")

        assert result == date(2024, 1, 15)

    def test_parse_date_month_name_format(self):
        """Test parsing month name format date."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        result = scraper.parse_date("January 15, 2024")

        assert result == date(2024, 1, 15)

    def test_parse_date_invalid(self):
        """Test parsing invalid date string."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        result = scraper.parse_date("invalid date")

        assert result is None

    def test_parse_date_empty(self):
        """Test parsing empty date string."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        result = scraper.parse_date("")

        assert result is None

    def test_normalize_team_name(self):
        """Test team name normalization."""
        rate_limiter = MagicMock()
        session = MagicMock()
        scraper = ESPNInjuryScraper(rate_limiter, session)

        # Test extra whitespace
        assert scraper.normalize_team_name("Lakers  ") == "Lakers"
        assert scraper.normalize_team_name("  Celtics  ") == "Celtics"
        assert scraper.normalize_team_name("Golden  State  Warriors") == "Golden State Warriors"

        # Test empty string
        assert scraper.normalize_team_name("") == ""
        assert scraper.normalize_team_name(None) == ""


class TestESPNInjuryScraper:
    """Tests for ESPNInjuryScraper."""

    def test_fetch_success(self):
        """Test successful fetch from ESPN."""
        # Mock HTML response
        html = """
        <table>
            <tr>
                <th>Player</th>
                <th>Team</th>
                <th>Status</th>
                <th>Description</th>
                <th>Date</th>
            </tr>
            <tr>
                <td>LeBron James</td>
                <td>LAL</td>
                <td>Out</td>
                <td>Left ankle sprain</td>
                <td>01/15/2024</td>
            </tr>
            <tr>
                <td>Stephen Curry</td>
                <td>GSW</td>
                <td>Day-to-Day</td>
                <td>Right knee soreness</td>
                <td>01/16/2024</td>
            </tr>
        </table>
        """

        rate_limiter = MagicMock()
        session = Mock()
        mock_response = Mock()
        mock_response.content = html.encode()
        mock_response.raise_for_status = MagicMock()
        session.get = Mock(return_value=mock_response)

        scraper = ESPNInjuryScraper(rate_limiter, session)
        injuries = scraper.fetch()

        assert len(injuries) == 2
        assert injuries[0]["player_name"] == "LeBron James"
        assert injuries[0]["team"] == "LAL"
        assert injuries[0]["status"] == "Out"
        assert injuries[0]["injury_type"] == "sprain"
        assert injuries[0]["body_part"] == "ankle"
        assert injuries[0]["injury_date"] == date(2024, 1, 15)

        assert injuries[1]["player_name"] == "Stephen Curry"
        assert injuries[1]["team"] == "GSW"
        assert injuries[1]["status"] == "Day-to-Day"
        assert injuries[1]["injury_type"] == "soreness"
        assert injuries[1]["body_part"] == "knee"

    def test_fetch_http_error(self):
        """Test fetch with HTTP error."""
        rate_limiter = MagicMock()
        session = Mock()
        mock_response = Mock()
        mock_response.raise_for_status = Mock(side_effect=Exception("HTTP 404"))
        session.get = Mock(return_value=mock_response)

        scraper = ESPNInjuryScraper(rate_limiter, session)

        with pytest.raises(Exception, match="HTTP 404"):
            scraper.fetch()

    def test_rate_limiting(self):
        """Test that rate limiter is called."""
        html = "<table><tr><td>Player</td><td>Team</td><td>Status</td></tr></table>"

        rate_limiter = MagicMock()
        session = Mock()
        mock_response = Mock()
        mock_response.content = html.encode()
        mock_response.raise_for_status = MagicMock()
        session.get = Mock(return_value=mock_response)

        scraper = ESPNInjuryScraper(rate_limiter, session)
        scraper.fetch()

        rate_limiter.acquire.assert_called_once()


class TestRotowireInjuryScraper:
    """Tests for RotowireInjuryScraper."""

    def test_fetch_success(self):
        """Test successful fetch from Rotowire."""
        # Mock HTML response
        html = """
        <div class="lineup">
            <span class="team-name">Lakers</span>
            <div class="player">
                <a class="player-name">LeBron James</a>
                <span class="status">Out</span>
                <div class="news">Left ankle sprain, will miss 2-3 weeks</div>
            </div>
            <div class="player">
                <a class="player-name">Anthony Davis</a>
                <span class="status">Questionable</span>
                <div class="news">Right hip soreness</div>
            </div>
        </div>
        <div class="lineup">
            <span class="team-name">Warriors</span>
            <div class="player">
                <a class="player-name">Stephen Curry</a>
                <span class="status">Day-to-Day</span>
                <div class="news">Knee contusion</div>
            </div>
        </div>
        """

        rate_limiter = MagicMock()
        session = Mock()
        mock_response = Mock()
        mock_response.content = html.encode()
        mock_response.raise_for_status = MagicMock()
        session.get = Mock(return_value=mock_response)

        scraper = RotowireInjuryScraper(rate_limiter, session)
        injuries = scraper.fetch()

        assert len(injuries) == 3
        assert injuries[0]["player_name"] == "LeBron James"
        assert injuries[0]["team"] == "Lakers"
        assert injuries[0]["status"] == "Out"
        assert injuries[0]["injury_type"] == "sprain"
        assert injuries[0]["body_part"] == "ankle"
        assert injuries[0]["injury_date"] == date.today()  # Rotowire uses today's date

        assert injuries[1]["player_name"] == "Anthony Davis"
        assert injuries[1]["team"] == "Lakers"
        assert injuries[1]["status"] == "Questionable"
        assert injuries[1]["body_part"] == "hip"

        assert injuries[2]["player_name"] == "Stephen Curry"
        assert injuries[2]["team"] == "Warriors"
        assert injuries[2]["injury_type"] == "contusion"

    def test_fetch_http_error(self):
        """Test fetch with HTTP error."""
        rate_limiter = MagicMock()
        session = Mock()
        mock_response = Mock()
        mock_response.raise_for_status = Mock(side_effect=Exception("HTTP 404"))
        session.get = Mock(return_value=mock_response)

        scraper = RotowireInjuryScraper(rate_limiter, session)

        with pytest.raises(Exception, match="HTTP 404"):
            scraper.fetch()

    def test_rate_limiting(self):
        """Test that rate limiter is called."""
        html = "<div class='lineup'><span class='team-name'>Lakers</span></div>"

        rate_limiter = MagicMock()
        session = Mock()
        mock_response = Mock()
        mock_response.content = html.encode()
        mock_response.raise_for_status = MagicMock()
        session.get = Mock(return_value=mock_response)

        scraper = RotowireInjuryScraper(rate_limiter, session)
        scraper.fetch()

        rate_limiter.acquire.assert_called_once()
