from crawlers.base import BaseCrawler
from models import Screening, Chain
import httpx
import datetime as dt
from typing import Iterable

class DtryxCrawler(BaseCrawler):
    chain: Chain = "Dtryx"

    async def iter(self, date: dt.date) -> Iterable[Screening]:
        url = "https://dtryx.com/cinema/showseq_list.do"
        crawl_ts = dt.datetime.utcnow().isoformat()

        headers = {
            "X-Requested-With": "XMLHttpRequest",
        }

        for theater in self.theaters:
            params = {
                "cgid": "FE8EF4D2-F22D-4802-A39A-D58F23A29C1E",
                "ssid": "",
                "tokn": "",
                "BrandCd": "indieart",
                "CinemaCd": theater.cinema_code,
                "PlaySDT": date.isoformat(),
                "_": str(int(dt.datetime.now().timestamp() * 1000))
            }

            async with httpx.AsyncClient(timeout=10.0) as client:
                try:
                    resp = await client.get(url, params=params, headers=headers)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    print(f"[{theater.cinema_code}] API request failed: {e}")
                    continue

                for item in data.get("Showseqlist", []):
                    yield Screening(
                        provider=self.chain,
                        cinema_name=item["CinemaNm"],
                        cinema_code=item["CinemaCd"],
                        screen_name=item["ScreenNm"],
                        movie_title=item["MovieNmNat"].strip(),
                        play_date=date.isoformat(),
                        start_dt=item["StartTime"],
                        end_dt=item["EndTime"],
                        crawl_ts=crawl_ts,
                    )
