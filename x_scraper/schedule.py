import datetime as dt
import os
import time

import dotenv
from splatnet3_scraper.query import QueryHandler

from x_scraper.scraper import Connector, XRankScraper

dotenv.load_dotenv()


def job():
    scraper = XRankScraper.from_env()
    scraper.update_schedule_db()
    scraper.update_player_db()


if __name__ == "__main__":
    while True:
        current_time = dt.datetime.now()

        if current_time.minute in [6, 21, 36, 51]:
            job()
        time.sleep(60 - dt.datetime.now().second)
