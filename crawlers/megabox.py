from crawlers.base import BaseCrawler
from models import Screening, Chain
import httpx
import datetime as dt
from typing import Iterable

class MegaboxCrawler(BaseCrawler):
    chain: Chain = "Megabox"

    async def iter(self, date: dt.date) -> Iterable[Screening]:
        url = "https://www.megabox.co.kr/on/oh/ohc/Brch/schedulePage.do"

        headers = {
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://www.megabox.co.kr",
            "Referer": "https://www.megabox.co.kr/booking/timetable",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }

        crawl_ts = dt.datetime.utcnow()

        for theater in self.theaters:
            brch_no = theater["cinema_code"]
            body = {
                "masterType": "brch",
                "detailType": "area",
                "brchNo": brch_no,
                "brchNo1": brch_no,
                "firstAt": "N",
                "crtDe": dt.date.today().strftime("%Y%m%d"),
                "playDe": date.strftime("%Y%m%d"),
            }

            async with httpx.AsyncClient(timeout=10.0) as client:
                try:
                    resp = await client.post(url, json=body, headers=headers)
                    resp.raise_for_status()
                    data = resp.json()

                except Exception as e:
                    print(f"[{brch_no}] API request failed: {e}")
                    continue

                for item in data.get("megaMap", {}).get("movieFormList", []):
                    cinema_name = item["brchNm"]
                    screen_name = item["theabExpoNm"].strip()

                    # At Coex, only filter art screens
                    if cinema_name == "코엑스" and screen_name not in {"스크린A", "스크린B"}:
                        continue

                    yield Screening(
                        provider=self.chain,
                        cinema_name=item["brchNm"],
                        cinema_code=item["brchNo"],
                        screen_name=item["theabExpoNm"].strip(),
                        movie_title=item["rpstMovieNm"].strip(),
                        play_date=date.isoformat(),
                        start_dt=item["playStartTime"],
                        end_dt=item["playEndTime"],
                        crawl_ts=crawl_ts.isoformat(),
                    )
