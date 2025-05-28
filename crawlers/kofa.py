# crawlers/kofa.py

import datetime as dt
import calendar
import os
from typing import Optional, AsyncIterator

import httpx

from crawlers.base import BaseCrawler
from models import Screening, Chain


class KOFACrawler(BaseCrawler):
    chain: Chain = "KOFA"
    api_url = "https://www.kmdb.or.kr/info/api/3/api.json"
    service_key = os.getenv("KOFA_SERVICE_KEY")
    async def run(
        self,
        start_date: Optional[dt.date] = None,
        max_days: Optional[int] = None,           # now ignored
    ) -> list[Screening]:
        """
        Fetch all screenings from `start` through the end of the *next* calendar month.
        """
        start = start_date or dt.date.today()

        # compute the first day of the month *after* start.month
        year, month = start.year, start.month
        if month == 12:
            next_year, next_month = year + 1, 1
        else:
            next_year, next_month = year, month + 1

        # last day of that next month
        last_day = calendar.monthrange(next_year, next_month)[1]
        end = dt.date(next_year, next_month, last_day)

        params = {
            "serviceKey": self.service_key,
            "StartDate":  start.strftime("%Y%m%d"),
            "EndDate":    end.strftime("%Y%m%d"),
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(self.api_url, params=params)
            resp.raise_for_status()
            data = resp.json()

        programs = data.get("resultList", [])
        results = []
        for item in programs:
            play_date = dt.datetime.strptime(item["cMovieDate"], "%Y%m%d").date()
            # filter just in case
            if play_date < start or play_date > end:
                continue

            run_min    = int(item.get("cRunningTime") or 0)
            start_time = dt.datetime.strptime(item["cMovieTime"], "%H:%M").time()
            start_dt   = dt.datetime.combine(play_date, start_time)
            end_dt     = (start_dt + dt.timedelta(minutes=run_min)).time().strftime("%H:%M")

            raw = item.get("cCodeSubName3") or ""
            if "관" in raw:
                screen_name = raw.split()[-1]
            else:
                screen_name = "Main"

            results.append(
                Screening(
                    provider     = self.chain,
                    cinema_name  = "시네마테크KOFA",
                    cinema_code  = "KOFA",
                    screen_name  = screen_name,
                    movie_title  = item["cMovieName"].strip(),
                    play_date    = play_date.isoformat(),
                    start_dt     = item["cMovieTime"],
                    end_dt       = end_dt,
                    crawl_ts     = dt.datetime.utcnow().isoformat(),
                    url          = item["homePageURL"]
                )
            )

        return results
    async def iter(self, date: dt.date) -> AsyncIterator[Screening]:
        # satisfy BaseCrawler’s abstract method
        for screening in await self.run(start_date=date, max_days=1):
            yield screening

