"""Player contract data ingestor.

This ingestor fetches player contract data from sources like RealGM,
Spotrac, or ESPN. Contract data includes salary amounts, contract type,
options, and guaranteed money.
"""

from typing import Any

import pydantic
import requests
import structlog
from bs4 import BeautifulSoup

from nba_vault.ingestion.base import BaseIngestor
from nba_vault.ingestion.registry import register_ingestor
from nba_vault.models.advanced_stats import PlayerContractCreate

logger = structlog.get_logger(__name__)


@register_ingestor
class ContractIngestor(BaseIngestor):
    """
    Ingestor for player contract data from various sources.

    Supports fetching contract data from:
    - RealGM (realgm.com)
    - Spotrac (spotrac.com)
    - ESPN (espn.com)

    Contract data includes:
    - Player and team
    - Contract years (season start and end)
    - Salary amount per season
    - Contract type (rookie, veteran, MLE, etc.)
    - Option types (player option, team option, ETO)
    - Guaranteed money and cap hit

    Note: Historical contract data may be incomplete for older seasons.
    """

    entity_type = "contracts"

    def __init__(self, cache=None, rate_limiter=None):
        """
        Initialize ContractIngestor.

        Args:
            cache: Content cache for API responses.
            rate_limiter: Rate limiter for requests.
        """
        super().__init__(cache, rate_limiter)
        self.session = requests.Session()
        # Set a user agent to avoid being blocked
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
        )

    def fetch(
        self,
        entity_id: str,
        source: str = "realgm",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Fetch contract data from various sources.

        Args:
            entity_id: "all" for all contracts, "team:<team_id>" for specific team,
                      or "player:<player_id>" for specific player.
            source: Data source ("realgm", "spotrac", "espn").
            **kwargs: Additional parameters for the request.

        Returns:
            Dictionary with contract data.

        Raises:
            ValueError: If source is not supported.
            Exception: If fetch fails after retries.
        """
        if entity_id == "all":
            self.logger.info("Fetching all contracts", source=source)

            if source == "realgm":
                contracts = self._fetch_realgm_contracts()
            elif source == "spotrac":
                contracts = self._fetch_spotrac_contracts()
            else:
                raise ValueError(f"Unsupported source: {source}")

            return {
                "scope": "all",
                "source": source,
                "contracts": contracts,
            }

        elif entity_id.startswith("team:"):
            team_identifier = entity_id.split(":", 1)[1]
            self.logger.info("Fetching contracts for team", team=team_identifier, source=source)

            if source == "realgm":
                contracts = self._fetch_realgm_team_contracts(team_identifier)
            elif source == "spotrac":
                contracts = self._fetch_spotrac_team_contracts(team_identifier)
            else:
                contracts = []

            return {
                "scope": "team",
                "team": team_identifier,
                "source": source,
                "contracts": contracts,
            }

        elif entity_id.startswith("player:"):
            player_identifier = entity_id.split(":", 1)[1]
            self.logger.info(
                "Fetching contracts for player", player=player_identifier, source=source
            )

            if source == "realgm":
                contracts = self._fetch_realgm_player_contracts(player_identifier)
            elif source == "spotrac":
                contracts = self._fetch_spotrac_player_contracts(player_identifier)
            else:
                contracts = []

            return {
                "scope": "player",
                "player": player_identifier,
                "source": source,
                "contracts": contracts,
            }

        else:
            raise ValueError(f"Invalid entity_id format: {entity_id}")

    def _fetch_realgm_contracts(self) -> list[dict[str, Any]]:
        """
        Fetch all contracts from RealGM.

        Returns:
            List of contract dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        # RealGM doesn't have a single page for all contracts
        # Would need to iterate through teams
        self.logger.warning("RealGM all-contracts fetch not fully implemented")
        return []

    def _fetch_realgm_team_contracts(self, team_identifier: str) -> list[dict[str, Any]]:
        """
        Fetch contracts for a team from RealGM.

        Args:
            team_identifier: Team name or abbreviation.

        Returns:
            List of contract dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        # RealGM URL structure for team contracts
        # Format: https://basketball.realgm.com/nba/teams/[Team_Name]/[Team_ID]/contracts

        # Would need to map team_identifier to RealGM team ID and URL
        self.logger.warning("RealGM team contracts fetch not yet implemented")
        return []

    def _fetch_realgm_player_contracts(self, player_identifier: str) -> list[dict[str, Any]]:
        """
        Fetch contracts for a player from RealGM.

        Args:
            player_identifier: Player name or RealGM player ID.

        Returns:
            List of contract dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        # RealGM URL structure for player contracts
        # Format: https://basketball.realgm.com/nba/players/[Player_Name]/[Player_ID]/summary

        self.logger.warning("RealGM player contracts fetch not yet implemented")
        return []

    def _fetch_spotrac_contracts(self) -> list[dict[str, Any]]:
        """
        Fetch all contracts from Spotrac.

        Returns:
            List of contract dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        url = "https://www.spotrac.com/nba/contracts/"

        self.rate_limiter.acquire()

        try:
            self.logger.info("Fetching contracts from Spotrac", url=url)
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            contracts = []

            # Spotrac contracts page structure
            # Look for contract tables
            table = soup.find("table", class_="players-table")
            if table:
                rows = table.find_all("tr")[1:]  # Skip header
                for row in rows:
                    cols = row.find_all("td")
                    if len(cols) >= 5:
                        # Extract player info
                        player_link = cols[0].find("a")
                        if player_link:
                            player_name = player_link.get_text(strip=True)
                            player_url = player_link.get("href", "")

                            # Extract team
                            team = cols[1].get_text(strip=True) if len(cols) > 1 else ""

                            # Extract contract details
                            # This is a simplified version - actual implementation would
                            # need to handle more complex contract structures

                            contracts.append(
                                {
                                    "player_name": player_name,
                                    "player_url": player_url,
                                    "team": team,
                                }
                            )

            self.logger.info("Fetched contracts from Spotrac", count=len(contracts))
            return contracts

        except Exception as e:
            self.logger.error("Failed to fetch Spotrac contracts", error=str(e))
            raise

    def _fetch_spotrac_team_contracts(self, team_identifier: str) -> list[dict[str, Any]]:
        """
        Fetch contracts for a team from Spotrac.

        Args:
            team_identifier: Team name or abbreviation.

        Returns:
            List of contract dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        # Spotrac URL structure for team contracts
        # Format: https://www.spotrac.com/nba/[Team_Name]/contracts/

        # Would need to map team_identifier to Spotrac team name
        self.logger.warning("Spotrac team contracts fetch not yet implemented")
        return []

    def _fetch_spotrac_player_contracts(self, player_identifier: str) -> list[dict[str, Any]]:
        """
        Fetch contracts for a player from Spotrac.

        Args:
            player_identifier: Player name or Spotrac player ID.

        Returns:
            List of contract dictionaries.

        Raises:
            Exception: If fetch fails.
        """
        # Spotrac URL structure for player contracts
        # Format: https://www.spotrac.com/nba/players/[Player_Name]/[Player_ID]/contracts/

        self.logger.warning("Spotrac player contracts fetch not yet implemented")
        return []

    def validate(self, raw: dict[str, Any]) -> list[pydantic.BaseModel]:
        """
        Validate raw contract data using Pydantic models.

        Args:
            raw: Raw data dictionary with 'contracts' key containing list of contract dicts.

        Returns:
            List of validated PlayerContractCreate models.

        Raises:
            pydantic.ValidationError: If validation fails.
        """
        contracts_data = raw.get("contracts", [])

        validated_contracts = []

        for contract_data in contracts_data:
            try:
                # Extract player_id and team_id
                # These would need to be looked up from names/IDs
                player_id = contract_data.get("player_id")
                team_id = contract_data.get("team_id")

                if not player_id or not team_id:
                    # Skip contracts without IDs
                    continue

                contract_record = {
                    "player_id": player_id,
                    "team_id": team_id,
                    "season_start": contract_data.get("season_start", 2024),
                    "season_end": contract_data.get("season_end", 2025),
                    "salary_amount": contract_data.get("salary_amount"),
                    "contract_type": contract_data.get("contract_type"),
                    "player_option": contract_data.get("player_option", 0),
                    "team_option": contract_data.get("team_option", 0),
                    "early_termination": contract_data.get("early_termination", 0),
                    "guaranteed_money": contract_data.get("guaranteed_money"),
                    "cap_hit": contract_data.get("cap_hit"),
                    "dead_money": contract_data.get("dead_money"),
                }

                validated_contract = PlayerContractCreate(**contract_record)
                validated_contracts.append(validated_contract)

            except pydantic.ValidationError as e:
                self.logger.error(
                    "Contract validation failed",
                    contract_data=contract_data,
                    errors=str(e),
                )
                # Don't raise - skip invalid records
                continue

        self.logger.info("Validated contracts", count=len(validated_contracts))
        return validated_contracts

    def upsert(self, model: list[pydantic.BaseModel], conn) -> int:
        """
        Insert or update validated contract data in database.

        Args:
            model: List of validated PlayerContractCreate models.
            conn: SQLite database connection.

        Returns:
            Number of rows affected.
        """
        rows_affected = 0

        for contract in model:
            if not isinstance(contract, PlayerContractCreate):
                continue

            # Check if contract exists
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM player_contract
                WHERE player_id = ? AND season_start = ? AND team_id = ?
                """,
                (contract.player_id, contract.season_start, contract.team_id),
            )
            exists = cursor.fetchone()[0] > 0

            if exists:
                # Update existing contract
                self._update_contract(contract, conn)
                rows_affected += 1
            else:
                # Insert new contract
                self._insert_contract(contract, conn)
                rows_affected += 1

            # Log to ingestion_audit
            conn.execute(
                """
                INSERT INTO ingestion_audit
                (entity_type, entity_id, status, source, metadata, ingested_at)
                VALUES (?, ?, 'SUCCESS', ?, ?, datetime('now'))
                """,
                (
                    self.entity_type,
                    str(contract.player_id),
                    "web_scraping",
                    f"player: {contract.player_id}, seasons: {contract.season_start}-{contract.season_end}",
                ),
            )

        return rows_affected

    def _insert_contract(self, contract: PlayerContractCreate, conn) -> None:
        """
        Insert a new contract into the database.

        Args:
            contract: PlayerContractCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            INSERT INTO player_contract (
                player_id, team_id, season_start, season_end, salary_amount,
                contract_type, player_option, team_option, early_termination,
                guaranteed_money, cap_hit, dead_money
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                contract.player_id,
                contract.team_id,
                contract.season_start,
                contract.season_end,
                contract.salary_amount,
                contract.contract_type,
                contract.player_option,
                contract.team_option,
                contract.early_termination,
                contract.guaranteed_money,
                contract.cap_hit,
                contract.dead_money,
            ),
        )

    def _update_contract(self, contract: PlayerContractCreate, conn) -> None:
        """
        Update an existing contract in the database.

        Args:
            contract: PlayerContractCreate model.
            conn: SQLite database connection.
        """
        conn.execute(
            """
            UPDATE player_contract SET
                season_end = ?, salary_amount = ?, contract_type = ?,
                player_option = ?, team_option = ?, early_termination = ?,
                guaranteed_money = ?, cap_hit = ?, dead_money = ?
            WHERE player_id = ? AND season_start = ? AND team_id = ?
            """,
            (
                contract.season_end,
                contract.salary_amount,
                contract.contract_type,
                contract.player_option,
                contract.team_option,
                contract.early_termination,
                contract.guaranteed_money,
                contract.cap_hit,
                contract.dead_money,
                contract.player_id,
                contract.season_start,
                contract.team_id,
            ),
        )
