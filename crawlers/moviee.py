import datetime as dt
import re
from typing import Iterable

import httpx

from crawlers.base import BaseCrawler
from models import Chain, Screening


class MovieeCrawler(BaseCrawler):
    chain: Chain = "Moviee"

    _base_url = "https://moviee.co.kr"
    _play_date_url = f"{_base_url}/api/TicketApi/GetPlayDateList"
    _play_time_url = f"{_base_url}/api/TicketApi/GetPlayTimeList"
    _provider_id = "Y24"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._play_dates_cache: dict[str, set[str]] = {}

    @staticmethod
    def _to_hhmm(value: str | int | None) -> str | None:
        if value is None:
            return None
        digits = re.sub(r"\D", "", str(value))
        if len(digits) == 3:
            digits = "0" + digits
        if len(digits) != 4:
            return None
        return f"{digits[:2]}:{digits[2:]}"

    @staticmethod
    def _to_int(value) -> int | None:
        if value is None:
            return None
        text = str(value).replace(",", "").strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            return None

    async def _get_available_dates(self, client: httpx.AsyncClient, theater_code: str) -> set[str]:
        if theater_code in self._play_dates_cache:
            return self._play_dates_cache[theater_code]

        params = {
            "tIdList": theater_code,
            "mId": "",
            "groupCd": -1,
            "mode": 0,
            "gId": "",
            "pId": self._provider_id,
        }
        try:
            response = await client.get(self._play_date_url, params=params)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            print(f"[Moviee:{theater_code}] GetPlayDateList failed: {exc}")
            self._play_dates_cache[theater_code] = set()
            return set()

        if payload.get("ResCd") != "00":
            print(
                f"[Moviee:{theater_code}] GetPlayDateList returned ResCd={payload.get('ResCd')}"
            )
            self._play_dates_cache[theater_code] = set()
            return set()

        table = ((payload.get("ResData") or {}).get("Table") or [])
        dates = {
            (row.get("PLAY_DT") or "").strip()
            for row in table
            if isinstance(row, dict) and (row.get("PLAY_DT") or "").strip()
        }
        self._play_dates_cache[theater_code] = dates
        return dates

    async def iter(self, date: dt.date) -> Iterable[Screening]:
        target_date = date.isoformat()
        crawl_ts = dt.datetime.utcnow().isoformat()

        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }

        async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
            for theater in self.theaters:
                theater_code = str(theater.cinema_code)
                available_dates = await self._get_available_dates(client, theater_code)
                if available_dates and target_date not in available_dates:
                    continue

                params = {
                    "tId": theater_code,
                    "mId": "",
                    "playDt": target_date,
                    "ntId": "",
                    "gId": "",
                }
                try:
                    response = await client.get(self._play_time_url, params=params)
                    response.raise_for_status()
                    payload = response.json()
                except Exception as exc:
                    print(f"[Moviee:{theater_code}] GetPlayTimeList failed: {exc}")
                    continue

                if payload.get("ResCd") != "00":
                    print(
                        f"[Moviee:{theater_code}] GetPlayTimeList returned ResCd={payload.get('ResCd')}"
                    )
                    continue

                rows = ((payload.get("ResData") or {}).get("Table") or [])
                for item in rows:
                    movie_title = (item.get("M_NM") or "").strip()
                    if not movie_title:
                        continue

                    start_dt = self._to_hhmm(item.get("PLAY_TIME"))
                    end_dt = self._to_hhmm(item.get("END_TIME"))
                    if not start_dt or not end_dt:
                        continue

                    play_date = (item.get("PLAY_DT") or target_date).strip() or target_date
                    cinema_name = (item.get("T_NM") or theater.name).strip()
                    cinema_code = str(item.get("T_ID") or theater_code)
                    screen_name = (item.get("TS_NM") or "").strip() or "미지정"

                    movie_id = (item.get("M_ID") or "").strip()
                    ts_id = (item.get("TS_ID") or "").strip()
                    pno = item.get("PNO")
                    play_date_compact = play_date.replace("-", "")
                    booking_url = None
                    if movie_id and cinema_code and ts_id and pno not in (None, ""):
                        booking_url = (
                            f"{self._base_url}/Movie/Ticket"
                            f"?gId=&mId={movie_id}&tId={cinema_code}"
                            f"&playDate={play_date_compact}&pno={pno}&tsid={ts_id}"
                        )

                    yield Screening(
                        provider=self.chain,
                        cinema_name=cinema_name,
                        cinema_code=cinema_code,
                        screen_name=screen_name,
                        movie_title=movie_title,
                        source_movie_code=movie_id or None,
                        play_date=play_date,
                        start_dt=start_dt,
                        end_dt=end_dt,
                        crawl_ts=crawl_ts,
                        url=booking_url,
                        remain_seat_cnt=self._to_int(item.get("REMAINSEAT_CNT")),
                        total_seat_cnt=self._to_int(item.get("SEAT_CNT")),
                    )
