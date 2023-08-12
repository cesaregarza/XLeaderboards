import base64
import sqlite3
import time
from typing import Any, Literal

from splatnet3_scraper.query import QueryHandler, QueryResponse


def base64_decode(string: str) -> str:
    return base64.b64decode(string).decode("utf-8")


class XRankScraper:
    query = "XRankingQuery"
    modes = ("Ar", "Cl", "Gl", "Lf")
    current_season_path = ("xRanking", "currentSeason", "id")
    detailed_x_query = "DetailTabViewXRanking%sRefetchQuery"
    detailed_weapon_query = "DetailTabViewWeaponTops%sRefetchQuery"
    regions = ("ATLANTIC", "PACIFIC")
    region_map = {
        "ATLANTIC": "Tentatek",
        "PACIFIC": "Takoroka",
    }

    def __init__(self, scraper: QueryHandler, db_path: str = None) -> None:
        self.scraper = scraper
        self.db_path = db_path if db_path else "x_rank.db"

    def end_cursor_path_x_rank(self, mode: str) -> str:
        return ("node", f"xRanking{mode}", "pageInfo", "endCursor")

    def get_current_season(self, region: Literal["ATLANTIC", "PACIFIC"]) -> str:
        response = self.scraper.query(self.query, variables={"region": region})
        return response[self.current_season_path]

    def get_detailed_data(
        self,
        season_id: str,
        mode: str,
        page: int,
        cursor: str,
        weapons: bool = False,
    ) -> QueryResponse:
        variables = {
            "id": season_id,
            "mode": mode,
            "page": page,
            "cursor": cursor,
        }
        base_query = (
            self.detailed_weapon_query if weapons else self.detailed_x_query
        )
        detailed_query = base_query % mode
        response = self.scraper.query(detailed_query, variables=variables)
        return response

    def parse_player_data(self, data: QueryResponse) -> dict[str, Any]:
        return {
            "id": base64_decode(data["id"]).split(":")[-1],
            "name": data["name"],
            "name_id": data["nameId"],
            "rank": data["rank"],
            "x_power": data["xPower"],
            "weapon": data["weapon", "name"],
            "weapon_id": data["weapon", "id"],
            "weapon_sub": data["weapon", "subWeapon", "name"],
            "weapon_sub_id": data["weapon", "subWeapon", "id"],
            "weapon_special": data["weapon", "specialWeapon", "name"],
            "weapon_special_id": data["weapon", "specialWeapon", "id"],
        }

    def parse_players_in_mode(
        self, data: QueryResponse, mode: str
    ) -> list[dict[str, Any]]:
        players = []
        for player_node in data["edges"]:
            player_data = self.parse_player_data(player_node["node"])
            player_data["mode"] = mode
            players.append(player_data)
        return players

    def scrape_all_players_in_mode(
        self, season_id: str, mode: str
    ) -> list[dict[str, Any]]:
        players = []
        for page in range(1, 6):
            has_next_page = True
            cursor = None
            while has_next_page:
                response = self.get_detailed_data(
                    season_id=season_id,
                    mode=mode,
                    page=page,
                    cursor=cursor,
                )
                subresponse = response["node", f"xRanking{mode}"]
                players.extend(self.parse_players_in_mode(subresponse, mode))

                has_next_page = subresponse["pageInfo", "hasNextPage"]
                cursor = subresponse["pageInfo", "endCursor"]
        return players

    def scrape_all_players_in_season(
        self,
        season_id: str,
    ) -> list[dict[str, Any]]:
        players = []
        for mode in self.modes:
            players.extend(self.scrape_all_players_in_mode(season_id, mode))
        return players

    def scrape_all_players_current_season(
        self, region: Literal["ATLANTIC", "PACIFIC"]
    ) -> list[dict[str, Any]]:
        season_id = self.get_current_season(region=region)
        timestamp = time.time()
        players = []
        for region in self.regions:
            players.extend(self.scrape_all_players_in_season(season_id))
        for player in players:
            player["timestamp"] = timestamp
            player["region"] = self.region_map[region]
        return players
