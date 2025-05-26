from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import re
from typing import Iterable, List

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from crawlers.base import BaseCrawler
from models import Screening, Chain
from crawlers.supabase_client import SupabaseClient

# ────────────────────────────────────────────────────────────────────────────
# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────────────
class CGVCrawler(BaseCrawler):
    chain: Chain = "CGV"

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _has_no_screenings(soup: BeautifulSoup) -> bool:
        """
        True if the iframe DOM is the ‘empty schedule’ page.
        (CGV shows a `<div class="noData">편성된 … 없습니다` block.)
        """
        return bool(
            soup.select_one("div.noData")
            or "편성된 스케줄이 없습니다" in soup.get_text(" ", strip=True)
        )

    # ----------------------------------------------------------- life-cycle
    def __init__(self, supabase: SupabaseClient, batch_size: int = 10):
        super().__init__(supabase=supabase, batch_size=batch_size)
        if not self.theaters:
            raise ValueError("No CGV theaters found")

    # ---------------------------------------------------------------- config
    @staticmethod
    def _build_driver() -> webdriver.Chrome:
        options = Options()
        options.binary_location = "/opt/chrome/chrome"

        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--single-process")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-setuid-sandbox")
        options.add_argument("--disable-extensions")
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument("--window-size=1280x1696")
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        chromedriver_path = "/opt/chromedriver"
        return webdriver.Chrome(service=Service(executable_path=chromedriver_path), options=options)

    # ---------------------------------------------------------------- crawl
    async def iter(self, date: dt.date) -> Iterable[Screening]:
        """
        Scrape a **single calendar day** for every theater in `self.theaters`.

        The base-class `.run()` keeps calling this for day+1 until it
        yields nothing, so we **return immediately** when the iframe
        tells us “no schedule”.
        """
        crawl_ts = dt.datetime.utcnow()
        date_str = date.strftime("%Y%m%d")

        driver = self._build_driver()
        try:
            # batch theatre URLs to be polite & keep memory down
            for i in range(0, len(self.theaters), self.batch_size):
                batch = self.theaters[i : i + self.batch_size]

                for theatre in batch:
                    code = theatre["cinema_code"]
                    name = theatre["name"]
                    area = theatre.get("areacode", "01")

                    url = (
                        f"http://www.cgv.co.kr/reserve/show-times/"
                        f"?areacode={area}&theaterCode={code}&date={date_str}"
                    )
                    try:
                        driver.get(url)
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.ID, "ifrm_movie_time_table"))
                        )
                        driver.switch_to.frame("ifrm_movie_time_table")
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.TAG_NAME, "body"))
                        )
                        soup = BeautifulSoup(driver.page_source, "html.parser")
                    except Exception as exc:
                        logger.warning("Failed to load %s: %s", name, exc)
                        continue

                    # ── stop-signal for outer loop ───────────────────────
                    if self._has_no_screenings(soup):
                        continue

                    for movie_block in soup.select("div.col-times"):
                        info_movie = movie_block.select_one("div.info-movie strong")
                        movie_title = info_movie.get_text(strip=True) if info_movie else "Unknown"

                        for hall in movie_block.select("div.type-hall"):
                            hall_li = hall.select_one("div.info-hall li:nth-child(2)")
                            screen_name = hall_li.get_text(strip=True) if hall_li else "Unknown"

                            if "아트하우스" not in screen_name.lower() and "art" not in screen_name.lower():
                                # skip non-arthouse screens
                                continue

                            timetable = hall.select_one("div.info-timetable")
                            for li in timetable.select("li"):
                                anchor = li.find(["a", "span"])
                                if not anchor:
                                    continue

                                start_raw = anchor.get("data-playstarttime") or ""
                                end_raw = anchor.get("data-playendtime") or ""
                                if not (start_raw.isdigit() and end_raw.isdigit() and len(start_raw) == 4):
                                    continue

                                start_time = f"{start_raw[:2]}:{start_raw[2:]}"
                                end_time = f"{end_raw[:2]}:{end_raw[2:]}"

                                status_em = anchor.find("em")
                                status = status_em.get_text(strip=True) if status_em else ""
                                if status in {"마감", "매진"}:
                                    continue

                                yield Screening(
                                    provider=self.chain,
                                    cinema_name=name,
                                    cinema_code=code,
                                    screen_name=screen_name,
                                    movie_title=movie_title,
                                    start_dt=start_time,
                                    end_dt=end_time,
                                    play_date=date.isoformat(),
                                    crawl_ts=crawl_ts.isoformat()
                                )
                                await asyncio.sleep(0)  # let event-loop breathe
        finally:
            driver.quit()