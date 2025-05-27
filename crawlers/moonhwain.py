# crawlers/moonhwain.py

from crawlers.base import BaseCrawler
from models import Screening, Chain
import httpx
import datetime as dt
from typing import Iterable, List
from bs4 import BeautifulSoup
import re

class MoonhwainCrawler(BaseCrawler):
    chain: Chain = "Moonhwain"
    calendar_url = "https://picturehouse2.moonhwain.kr:447/rsvc/rsv_mv.html?b_id=picturehouse&vwCal=1"
    ajax_url     = "https://picturehouse2.moonhwain.kr:447/inc/getTimeM.html"
    detail_url   = "https://picturehouse.moonhwain.kr:447/movie/detail.html"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.available_dates: List[str] = []
        # cache of film_id (p_idx) -> runtime in minutes
        self._runtime_cache: dict[str, int] = {}

    async def _fetch_available_dates(self):
        if self.available_dates:
            return
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent":"curl/7.54.0"}) as client:
            resp = await client.get(self.calendar_url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

        act = soup.find("input", id="actDate")["value"]
        for segment in act.split(","):
            if not segment.strip():
                continue
            ymd, _ = segment.split(":", 1)
            iso = f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"
            self.available_dates.append(iso)

    async def run(
        self,
        start_date: dt.date | None = None,
        max_days: int | None = None
    ) -> List[Screening]:
        await self._fetch_available_dates()
        start = start_date or dt.date.today()
        results: List[Screening] = []
        count = 0

        for date_str in self.available_dates:
            current = dt.date.fromisoformat(date_str)
            if current < start:
                continue
            if max_days is not None and count >= max_days:
                break

            day = [s async for s in self.iter(current)]
            results.extend(day)
            count += 1

        return results

    async def iter(self, date: dt.date) -> Iterable[Screening]:
        if date.isoformat() not in self.available_dates:
            return

        form = {
            "p_idx": "",
            "b_id": "picturehouse",
            "ss_date": date.isoformat(),
            "in_ss_date": date.isoformat(),
            "in_ss_idx": "",
        }
        crawl_ts = dt.datetime.utcnow().isoformat()

        async with httpx.AsyncClient(timeout=10) as client:
            # get the day's showtimes
            resp = await client.post(
                self.ajax_url,
                data=form,
                headers={"X-Requested-With": "XMLHttpRequest"}
            )
            resp.raise_for_status()

            # unwrap XML → CDATA → HTML
            m = re.search(r"<time>\s*<!\[CDATA\[(.*?)\]\]>\s*</time>", resp.text, re.DOTALL)
            html = m.group(1) if m else ""
            soup = BeautifulSoup(html, "html.parser")

            title_areas = soup.select("div.movie_time_select > div.title_area")
            uls         = soup.select("div.movie_time_select > ul")

            for area, ul in zip(title_areas, uls):
                title       = area.select_one("p.movie_name").get_text(strip=True)
                total_str   = ul.select_one("h6 span").get_text(strip=True)
                total_seats = int(total_str.replace("총", "").replace("석", "").strip())
                screen      = ul.select_one("h6 em").get_text(strip=True)

                for dl in ul.select("dl.time_list"):
                    a        = dl.find("a")
                    time_txt = a.get_text(strip=True)
                    sold_out = bool(a.find("del"))

                    # remaining seats
                    dd = dl.find_next_sibling("dd") or dl.find("dd")
                    if sold_out:
                        remain_seats = 0
                    else:
                        txt = dd.get_text(strip=True)
                        m2  = re.search(r"(\d+)", txt)
                        remain_seats = int(m2.group(1)) if m2 else 0

                    # booking URL
                    href = a.get("href", "")
                    if "javascript:goLogin" in href:
                        inner    = href.split("goLogin('",1)[1].split("')",1)[0]
                        book_url = "https://picturehouse2.moonhwain.kr:447" + inner
                    else:
                        book_url = self.calendar_url

                    # extract p_idx (film detail id)
                    p_idx = None
                    m3    = re.search(r"getPfmDateJson_new\('\d+','(\d+)'\)", href)
                    if m3:
                        p_idx = m3.group(1)

                    # fetch & cache runtime
                    runtime_min = None
                    if p_idx:
                        if p_idx not in self._runtime_cache:
                            detail_resp = await client.get(self.detail_url, params={"p_idx": p_idx})
                            detail_resp.raise_for_status()
                            detail_soup = BeautifulSoup(detail_resp.text, "html.parser")

                            # 1) preferred: <dt>러닝타임</dt><dd>127분</dd>
                            dt_tag = detail_soup.find("dt", string=re.compile("러닝타임"))
                            if dt_tag:
                                dd_tag = dt_tag.find_next_sibling("dd")
                                if dd_tag:
                                    m4 = re.search(r"(\d+)", dd_tag.get_text())
                                    if m4:
                                        self._runtime_cache[p_idx] = int(m4.group(1))

                            # 2) fallback: <p class="sinfo"><span>…</span><span>127분</span></p>
                            if p_idx not in self._runtime_cache:
                                spans = detail_soup.select("p.sinfo span")
                                for sp in spans:
                                    m5 = re.search(r"(\d+)\s*분", sp.get_text())
                                    if m5:
                                        self._runtime_cache[p_idx] = int(m5.group(1))
                                        break

                        runtime_min = self._runtime_cache.get(p_idx)

                    # compute end time (preserve 24+ hours)
                    end_txt = time_txt
                    if runtime_min is not None:
                        hh, mm     = map(int, time_txt.split(":"))
                        start_mins = hh * 60 + mm
                        end_mins   = start_mins + runtime_min
                        end_h      = end_mins // 60
                        end_m      = end_mins % 60
                        end_txt    = f"{end_h:02d}:{end_m:02d}"

                    yield Screening(
                        provider        = self.chain,
                        cinema_name     = self.theaters[0].name,
                        cinema_code     = self.theaters[0].cinema_code,
                        screen_name     = screen,
                        movie_title     = title,
                        play_date       = date.isoformat(),
                        start_dt        = time_txt,
                        end_dt          = end_txt,
                        crawl_ts        = crawl_ts,
                        url             = book_url,
                        remain_seat_cnt = remain_seats,
                        total_seat_cnt  = total_seats,
                    )
