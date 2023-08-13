import base64
import datetime as dt
import sqlite3
from typing import Any, Literal

from splatnet3_scraper.query import QueryHandler, QueryResponse

from x_scraper.types import Mode, ModeName, Player, Region, RegionName, Schedule


def base64_decode(string: str) -> str:
    return base64.b64decode(string).decode("utf-8")


class XRankScraper:
    query = "XRankingQuery"
    schedule_query = "StageScheduleQuery"
    modes = ("Ar", "Cl", "Gl", "Lf")
    mode_map: dict[Mode, ModeName] = {
        "Ar": "Splat Zones",
        "Cl": "Clam Blitz",
        "Gl": "Rainmaker",
        "Lf": "Tower Control",
    }
    current_season_path = ("xRanking", "currentSeason", "id")
    detailed_x_query = "DetailTabViewXRanking%sRefetchQuery"
    detailed_weapon_query = "DetailTabViewWeaponTops%sRefetchQuery"
    regions = ("ATLANTIC", "PACIFIC")
    region_map: dict[Region, RegionName] = {
        "ATLANTIC": "Tentatek",
        "PACIFIC": "Takoroka",
    }
    schedule_path = ("xSchedules", "nodes")

    def __init__(self, scraper: QueryHandler, db_path: str = None) -> None:
        self.scraper = scraper
        self.db_path = db_path if db_path else "x_rank.db"
        self.timestamp = dt.datetime.utcnow()

    def end_cursor_path_x_rank(self, mode: str) -> str:
        return ("node", f"xRanking{mode}", "pageInfo", "endCursor")

    def get_current_season(self, region: Region) -> str:
        response = self.scraper.query(self.query, variables={"region": region})
        return response[self.current_season_path]

    def get_detailed_data(
        self,
        season_id: str,
        mode: Mode,
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

    def parse_player_data(self, data: QueryResponse) -> Player:
        return Player(
            id=base64_decode(data["id"]).split(":")[-1],
            name=data["name"],
            name_id=data["nameId"],
            rank=data["rank"],
            x_power=data["xPower"],
            weapon=data["weapon", "name"],
            weapon_id=data["weapon", "id"],
            weapon_sub=data["weapon", "subWeapon", "name"],
            weapon_sub_id=data["weapon", "subWeapon", "id"],
            weapon_special=data["weapon", "specialWeapon", "name"],
            weapon_special_id=data["weapon", "specialWeapon", "id"],
        )

    def parse_players_in_mode(
        self, data: QueryResponse, mode: str
    ) -> list[Player]:
        players = []
        for player_node in data["edges"]:
            player_data = self.parse_player_data(player_node["node"])
            player_data["mode"] = mode
            players.append(player_data)
        return players

    def scrape_all_players_in_mode(
        self, season_id: str, mode: str
    ) -> list[Player]:
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

    def scrape_all_players_in_mode(
        self,
        mode: Mode,
    ) -> list[Player]:
        out = []
        for region in self.regions:
            season_id = self.get_current_season(region=region)

            players = []
            players.extend(self.scrape_all_players_in_mode(season_id, mode))
            for player in players:
                player["timestamp"] = self.timestamp
                player["region"] = self.region_map[region]
                player["mode"] = self.mode_map[mode]
            out.extend(players)
        return out

    def parse_time(self, time: str) -> dt.datetime:
        return dt.datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=dt.timezone.utc
        )

    def get_schedule(self) -> list[Schedule]:
        response = self.scraper.query(self.schedule_query)
        responses = response[self.schedule_path].data
        schedule = []
        for response in responses:
            setting = response["xMatchSetting"]
            fest = setting is None
            base = Schedule(
                start_time=self.parse_time(response["startTime"]),
                end_time=self.parse_time(response["endTime"]),
                splatfest=fest,
            )
            if fest:
                schedule.append(base)
                continue

            base["mode"] = setting["vsRule", "name"]
            base["stage_1_id"] = setting["vsStages", 0, "vsStageId"]
            base["stage_1_name"] = setting["vsStages", 0, "name"]
            base["stage_2_id"] = setting["vsStages", 1, "vsStageId"]
            base["stage_2_name"] = setting["vsStages", 1, "name"]
            schedule.append(base)
        return schedule
