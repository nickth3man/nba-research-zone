"""Tests for InjuryIngestor."""

from datetime import date

import pytest

from nba_vault.ingestion.injuries import InjuryIngestor
from nba_vault.models.advanced_stats import InjuryCreate


class TestInjuryIngestor:
    """Tests for InjuryIngestor."""

    def test_init(self):
        """Test ingestor initialization."""
        ingestor = InjuryIngestor()
        assert ingestor.entity_type == "injuries"
        assert ingestor.session is not None

    def test_fetch_unsupported_source(self):
        """Test that unsupported source raises error."""
        ingestor = InjuryIngestor()
        with pytest.raises(ValueError, match=r"[Uu]nsupported"):
            ingestor.fetch("all", source="invalid")

    def test_parse_injury_description(self):
        """Test parsing injury descriptions."""
        ingestor = InjuryIngestor()

        injury_type, body_part = ingestor._parse_injury_description("Left ACL tear")
        assert injury_type == "tear"
        assert body_part == "acl"

        injury_type, body_part = ingestor._parse_injury_description("Right ankle sprain")
        assert injury_type == "sprain"
        assert body_part == "ankle"

        injury_type, body_part = ingestor._parse_injury_description("")
        assert injury_type is None
        assert body_part is None

    def test_parse_date(self):
        """Test date parsing."""
        ingestor = InjuryIngestor()

        result = ingestor._parse_date("2023-10-15")
        assert result == date(2023, 10, 15)

        result = ingestor._parse_date("10/15/2023")
        assert result == date(2023, 10, 15)

        result = ingestor._parse_date("")
        assert result is None


class TestInjuryUpsert:
    """Integration tests for InjuryIngestor.upsert()."""

    def test_upsert_inserts_new_injury(self, db_connection):
        ingestor = InjuryIngestor()
        models = [
            InjuryCreate(
                player_id=2544,
                team_id=1610612747,
                injury_date=date(2024, 1, 10),
                injury_type="Ankle",
                status="Out",
                games_missed=2,
            )
        ]
        rows = ingestor.upsert(models, db_connection)
        assert rows == 1

    def test_upsert_updates_existing_injury(self, db_connection):
        ingestor = InjuryIngestor()
        model = InjuryCreate(
            player_id=201939,
            injury_date=date(2024, 2, 1),
            status="Questionable",
        )
        ingestor.upsert([model], db_connection)

        updated = InjuryCreate(
            player_id=201939,
            injury_date=date(2024, 2, 1),
            status="Questionable",
            games_missed=1,
        )
        rows = ingestor.upsert([updated], db_connection)
        assert rows == 1

    def test_upsert_skips_non_injury_models(self, db_connection):
        from pydantic import BaseModel

        class Other(BaseModel):
            x: int = 1

        ingestor = InjuryIngestor()
        rows = ingestor.upsert([Other()], db_connection)
        assert rows == 0
