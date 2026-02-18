"""Integration tests for players ingestion."""

import sqlite3
from unittest.mock import patch

import pytest

from nba_vault.ingestion.players import PlayersIngestor
from nba_vault.models.players import BasketballReferencePlayer, PlayerCreate


@pytest.fixture
def test_db(tmp_path):
    """Create a temporary test database."""
    db_path = tmp_path / "test_nba.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Create player table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS player (
            player_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            full_name TEXT NOT NULL,
            display_name TEXT,
            birthdate TEXT,
            birthplace_city TEXT,
            birthplace_state TEXT,
            birthplace_country TEXT,
            height_inches REAL,
            weight_lbs REAL,
            position TEXT,
            primary_position TEXT,
            jersey_number TEXT,
            college TEXT,
            country TEXT,
            draft_year INTEGER,
            draft_round INTEGER,
            draft_number INTEGER,
            is_active INTEGER NOT NULL DEFAULT 1,
            from_year INTEGER,
            to_year INTEGER,
            bbref_id TEXT UNIQUE,
            data_availability_flags INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    # Create ingestion_audit table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id TEXT NOT NULL,
            status TEXT NOT NULL,
            source TEXT,
            metadata TEXT,
            error_message TEXT,
            ingested_at TEXT NOT NULL
        )
        """
    )

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def sample_players_data():
    """Sample player data from Basketball Reference format."""
    return [
        {
            "slug": "jamesle01",
            "name": "LeBron James",
            "position": "SF",
            "height": "6-9",
            "weight": "250",
            "team_abbreviation": "LAL",
            "games_played": 71,
            "games_started": 71,
            "minutes_played": 2578.0,
            "field_goals": 743,
            "field_goal_attempts": 1427,
            "field_goal_percentage": 0.521,
            "three_point_field_goals": 116,
            "three_point_field_goal_attempts": 329,
            "three_point_field_goal_percentage": 0.353,
            "two_point_field_goals": 627,
            "two_point_field_goal_attempts": 1098,
            "two_point_field_goal_percentage": 0.571,
            "effective_field_goal_percentage": 0.562,
            "free_throws": 310,
            "free_throw_attempts": 416,
            "free_throw_percentage": 0.745,
            "offensive_rebounds": 59,
            "defensive_rebounds": 406,
            "rebounds": 465,
            "assists": 598,
            "steals": 81,
            "blocks": 45,
            "turnovers": 240,
            "personal_fouls": 129,
            "points": 1912,
            "player_advanced_stats": {},
        },
        {
            "slug": "curryst01",
            "name": "Stephen Curry",
            "position": "PG",
            "height": "6-2",
            "weight": "185",
            "team_abbreviation": "GSW",
            "games_played": 74,
            "games_started": 74,
            "minutes_played": 2675.0,
            "field_goals": 632,
            "field_goal_attempts": 1379,
            "field_goal_percentage": 0.458,
            "three_point_field_goals": 354,
            "three_point_field_goal_attempts": 872,
            "three_point_field_goal_percentage": 0.406,
            "two_point_field_goals": 278,
            "two_point_field_goal_attempts": 507,
            "two_point_field_goal_percentage": 0.548,
            "effective_field_goal_percentage": 0.587,
            "free_throws": 276,
            "free_throw_attempts": 304,
            "free_throw_percentage": 0.908,
            "offensive_rebounds": 35,
            "defensive_rebounds": 325,
            "rebounds": 360,
            "assists": 501,
            "steals": 66,
            "blocks": 44,
            "turnovers": 195,
            "personal_fouls": 143,
            "points": 1894,
            "player_advanced_stats": {},
        },
    ]


class TestBasketballReferencePlayer:
    """Tests for BasketballReferencePlayer model."""

    def test_valid_player_data(self, sample_players_data):
        """Test validation of valid player data."""
        player_data = sample_players_data[0]
        player = BasketballReferencePlayer(**player_data)

        assert player.slug == "jamesle01"
        assert player.name == "LeBron James"
        assert player.position == "SF"
        assert player.height == "6-9"
        assert player.weight == "250"
        assert player.points == 1912

    def test_height_validation(self):
        """Test height format validation."""
        # Valid height
        player = BasketballReferencePlayer(
            slug="test01", name="Test Player", position="C", height="7-0", weight="250"
        )
        assert player.height == "7-0"

        # Height in inches should be converted
        player2 = BasketballReferencePlayer(
            slug="test02", name="Test Player", position="C", height="84", weight="250"
        )
        assert player2.height == "7-0"

    def test_weight_validation(self):
        """Test weight format validation."""
        player = BasketballReferencePlayer(
            slug="test01", name="Test Player", position="C", height="6-9", weight="250"
        )
        assert player.weight == "250"


class TestPlayerCreate:
    """Tests for PlayerCreate model."""

    def test_from_basketball_reference(self, sample_players_data):
        """Test conversion from Basketball Reference data."""
        br_player = BasketballReferencePlayer(**sample_players_data[0])
        player = PlayerCreate.from_basketball_reference(br_player)

        assert player.first_name == "LeBron"
        assert player.last_name == "James"
        assert player.full_name == "LeBron James"
        assert player.display_name == "LeBron James"
        assert player.bbref_id == "jamesle01"
        assert player.height_inches == 81.0  # 6-9 = 81 inches
        assert player.weight_lbs == 250.0
        assert player.position == "SF"
        assert player.primary_position == "Small Forward"
        assert player.is_active is True

    def test_height_conversion(self):
        """Test height conversion from feet-inches to inches."""
        br_player = BasketballReferencePlayer(
            slug="test01", name="Test Player", position="C", height="7-1", weight="250"
        )
        player = PlayerCreate.from_basketball_reference(br_player)

        assert player.height_inches == 85.0  # 7-1 = 85 inches

    def test_name_parsing(self):
        """Test parsing of player names."""
        br_player = BasketballReferencePlayer(
            slug="smithja01", name="Jayson Tatum", position="SF", height="6-8", weight="210"
        )
        player = PlayerCreate.from_basketball_reference(br_player)

        assert player.first_name == "Jayson"
        assert player.last_name == "Tatum"
        assert player.full_name == "Jayson Tatum"

    def test_position_mapping(self):
        """Test position mapping."""
        positions = ["PG", "SG", "SF", "PF", "C"]
        expected = [
            "Point Guard",
            "Shooting Guard",
            "Small Forward",
            "Power Forward",
            "Center",
        ]

        for pos, exp in zip(positions, expected, strict=False):
            br_player = BasketballReferencePlayer(
                slug=f"test{pos}01", name="Test Player", position=pos, height="6-8", weight="220"
            )
            player = PlayerCreate.from_basketball_reference(br_player)
            assert player.primary_position == exp


class TestPlayersIngestor:
    """Tests for PlayersIngestor."""

    @pytest.fixture
    def ingestor(self):
        """Create a PlayersIngestor instance."""
        return PlayersIngestor()

    def test_fetch_season_players(self, ingestor, sample_players_data):
        """Test fetching players for a season."""
        with patch.object(
            ingestor.basketball_reference_client, "get_players", return_value=sample_players_data
        ):
            result = ingestor.fetch("season", season_end_year=2024)

            assert "players" in result
            assert len(result["players"]) == 2
            assert result["season_end_year"] == 2024

    def test_validate_players(self, ingestor, sample_players_data):
        """Test validation of player data."""
        raw_data = {"players": sample_players_data, "season_end_year": 2024}

        validated = ingestor.validate(raw_data)

        assert len(validated) == 2
        assert all(isinstance(p, BasketballReferencePlayer) for p in validated)
        assert validated[0].slug == "jamesle01"
        assert validated[1].slug == "curryst01"

    def test_upsert_new_players(self, ingestor, sample_players_data, test_db):
        """Test inserting new players."""
        # Create validated models
        validated = [BasketballReferencePlayer(**p) for p in sample_players_data]

        # Upsert
        rows_affected = ingestor.upsert(validated, test_db)

        assert rows_affected == 2

        # Verify database
        cursor = test_db.execute("SELECT COUNT(*) FROM player")
        count = cursor.fetchone()[0]
        assert count == 2

        # Check specific player
        cursor = test_db.execute(
            "SELECT first_name, last_name, bbref_id FROM player WHERE bbref_id = ?", ("jamesle01",)
        )
        row = cursor.fetchone()
        assert row["first_name"] == "LeBron"
        assert row["last_name"] == "James"
        assert row["bbref_id"] == "jamesle01"

    def test_upsert_existing_players(self, ingestor, sample_players_data, test_db):
        """Test updating existing players."""
        # Insert initial player
        validated = [BasketballReferencePlayer(**sample_players_data[0])]
        ingestor.upsert(validated, test_db)

        # Get initial player_id
        cursor = test_db.execute("SELECT player_id FROM player WHERE bbref_id = ?", ("jamesle01",))
        initial_id = cursor.fetchone()[0]

        # Update with new data
        updated_data = sample_players_data[0].copy()
        updated_data["weight"] = "260"  # Change weight
        validated_updated = [BasketballReferencePlayer(**updated_data)]
        ingestor.upsert(validated_updated, test_db)

        # Verify player_id didn't change
        cursor = test_db.execute(
            "SELECT player_id, weight_lbs FROM player WHERE bbref_id = ?", ("jamesle01",)
        )
        row = cursor.fetchone()
        assert row["player_id"] == initial_id
        assert row["weight_lbs"] == 260.0

    def test_ingest_pipeline(self, ingestor, sample_players_data, test_db):
        """Test complete ingestion pipeline."""
        with patch.object(
            ingestor.basketball_reference_client, "get_players", return_value=sample_players_data
        ):
            result = ingestor.ingest("season", test_db, season_end_year=2024)

            assert result["status"] == "SUCCESS"
            assert result["rows_affected"] == 2
            assert result["entity_id"] == "season"

            # Verify database
            cursor = test_db.execute("SELECT COUNT(*) FROM player")
            count = cursor.fetchone()[0]
            assert count == 2

            # Check audit log
            cursor = test_db.execute(
                "SELECT COUNT(*) FROM ingestion_audit WHERE entity_type = 'players' AND status = 'SUCCESS'"
            )
            audit_count = cursor.fetchone()[0]
            assert audit_count == 2


@pytest.mark.integration
class TestPlayersIngestionIntegration:
    """Integration tests that require actual database and API (marked as integration)."""

    def test_end_to_end_ingestion(self, test_db):
        """
        Test end-to-end ingestion with mocked API data.

        This test would normally require the actual basketball_reference_web_scraper
        but we mock it for faster, reliable testing.
        """
        ingestor = PlayersIngestor()

        # Mock the API call
        sample_data = [
            {
                "slug": "jamesle01",
                "name": "LeBron James",
                "position": "SF",
                "height": "6-9",
                "weight": "250",
                "team_abbreviation": "LAL",
                "games_played": 71,
                "games_started": 71,
                "minutes_played": 2578.0,
                "field_goals": 743,
                "field_goal_attempts": 1427,
                "field_goal_percentage": 0.521,
                "three_point_field_goals": 116,
                "three_point_field_goal_attempts": 329,
                "three_point_field_goal_percentage": 0.353,
                "two_point_field_goals": 627,
                "two_point_field_goal_attempts": 1098,
                "two_point_field_goal_percentage": 0.571,
                "effective_field_goal_percentage": 0.562,
                "free_throws": 310,
                "free_throw_attempts": 416,
                "free_throw_percentage": 0.745,
                "offensive_rebounds": 59,
                "defensive_rebounds": 406,
                "rebounds": 465,
                "assists": 598,
                "steals": 81,
                "blocks": 45,
                "turnovers": 240,
                "personal_fouls": 129,
                "points": 1912,
                "player_advanced_stats": {},
            }
        ]

        with patch.object(
            ingestor.basketball_reference_client, "get_players", return_value=sample_data
        ):
            # Fetch
            raw_data = ingestor.fetch("season", season_end_year=2024)

            # Validate
            validated = ingestor.validate(raw_data)

            # Upsert
            rows_affected = ingestor.upsert(validated, test_db)

            # Verify
            assert rows_affected == 1

            cursor = test_db.execute("SELECT * FROM player WHERE bbref_id = ?", ("jamesle01",))
            player = cursor.fetchone()

            assert player["first_name"] == "LeBron"
            assert player["last_name"] == "James"
            assert player["full_name"] == "LeBron James"
            assert player["height_inches"] == 81.0
            assert player["weight_lbs"] == 250.0
            assert player["position"] == "SF"
            assert player["primary_position"] == "Small Forward"
            assert player["bbref_id"] == "jamesle01"
