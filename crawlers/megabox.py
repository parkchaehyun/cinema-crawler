from crawlers.base import BaseCrawler
from models import Screening, Chain
import httpx
import datetime as dt
import html
import re
from typing import Iterable

class MegaboxCrawler(BaseCrawler):
    chain: Chain = "Megabox"

    @staticmethod
    def _normalize_screen_name(raw_name: str) -> str:
        name = html.unescape(raw_name or "").strip()
        # Remove trailing format/tech suffixes only.
        name = re.sub(r"\s*\[[^\]]*]\s*$", "", name)
        name = re.sub(r"\s*\([^)]*\)\s*$", "", name)
        return re.sub(r"\s+", " ", name).strip()

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
            brch_no = theater.cinema_code
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
                    cinema_name = html.unescape(item["brchNm"]).strip()
                    screen_name = self._normalize_screen_name(item.get("theabExpoNm"))
                    branch_code = str(item.get("brchNo") or "").strip()
                    is_core_art_screen = (
                        (cinema_name == "코엑스" and screen_name in {"스크린A", "스크린B"})
                        or branch_code == "0081"
                        or "픽쳐하우스" in cinema_name
                    )

                    play_schdl_no = item.get("playSchdlNo")
                    book_url = f"https://www.megabox.co.kr/bookingByPlaySchdlNo?playSchdlNo={play_schdl_no}" if play_schdl_no else None

                    yield Screening(
                        provider=self.chain,
                        cinema_name=cinema_name,
                        cinema_code=branch_code,
                        screen_name=screen_name,
                        movie_title=html.unescape(item["rpstMovieNm"]).strip(),
                        movie_title_en=html.unescape(item.get("movieEngNm") or "").strip() or None,
                        source_movie_code=str(
                            item.get("rpstMovieNo") or item.get("movieNo") or ""
                        ).strip() or None,
                        is_core_art_screen=is_core_art_screen,
                        play_date=date.isoformat(),
                        start_dt=item["playStartTime"],
                        end_dt=item["playEndTime"],
                        crawl_ts=crawl_ts.isoformat(),
                        url=book_url,
                        remain_seat_cnt=int(item["restSeatCnt"]),
                        total_seat_cnt=int(item["totSeatCnt"])
                    )
