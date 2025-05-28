from crawlers.base import BaseCrawler
from models import Screening, Chain
import httpx
import datetime as dt
from typing import Iterable
import json

class LotteCinemaCrawler(BaseCrawler):
    chain: Chain = "Lotte"

    async def iter(self, date: dt.date) -> Iterable[Screening]:
        url = "https://www.lottecinema.co.kr/LCWS/Ticketing/TicketingData.aspx"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://www.lottecinema.co.kr",
            "Origin": "https://www.lottecinema.co.kr",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }

        crawl_ts = dt.datetime.utcnow()

        for theater in self.theaters:
            payload = {
                "MethodName": "GetPlaySequence",
                "channelType": "HO",
                "osType": "W",
                "osVersion": "Chrome",
                "playDate": date.strftime("%Y-%m-%d"),
                "cinemaID": theater.cinema_code,
                "representationMovieCode": ""
            }

            try:
                async with httpx.AsyncClient() as client:
                    res = await client.post(
                        url,
                        data={"ParamList": json.dumps(payload)},
                        headers=headers
                    )
                    res.raise_for_status()
                    data = res.json()

                    for item in data["PlaySeqs"]["Items"]:
                        if "아르떼" not in (item.get("ScreenDivisionNameKR") or ""):
                            continue
                        if not item.get("StartTime"):
                            continue

                        screen_id = item.get("ScreenID")
                        cinema_id = item.get("CinemaID")
                        movie_cd = item.get("RepresentationMovieCode")
                        play_date = item.get("PlayDt")  # Should already be in "YYYY-MM-DD"
                        start_time = item.get("StartTime")  # e.g., "20:30"

                        book_url = (
                            f"https://www.lottecinema.co.kr/NLCHS/ticketing"
                            f"?link_screenId={screen_id}"
                            f"&link_cinemaCode={cinema_id}"
                            f"&link_movieCd={movie_cd}"
                            f"&link_date={play_date}"
                            f"&link_time={start_time}"
                            f"&link_channelCode=naver"
                        )

                        yield Screening(
                            provider=self.chain,
                            cinema_name=item["CinemaNameKR"],
                            cinema_code=theater.cinema_code,
                            screen_name=item["ScreenNameKR"],
                            movie_title=item["MovieNameKR"].strip(),
                            play_date=play_date,
                            start_dt=start_time,
                            end_dt=item.get("EndTime"),
                            crawl_ts=crawl_ts.isoformat(),
                            url=book_url,
                            remain_seat_cnt=int(item["BookingSeatCount"]),
                            total_seat_cnt=int(item["TotalSeatCount"])
                        )

            except Exception as e:
                print(f"❌ Error processing {theater.name}: {e}")
