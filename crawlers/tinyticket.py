# crawlers/tinyticket_selenium.py

import re
import datetime
from typing import Generator

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from crawlers.base import BaseCrawler
from models import Screening, Chain


class TinyTicketCrawler(BaseCrawler):
    chain: Chain = "TinyTicket"
    base_url = "https://www.tinyticket.net/event-manager"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        chrome_opts = Options()
        chrome_opts.binary_location = "/opt/chrome/chrome"
        chrome_opts.add_argument("--headless=new")
        chrome_opts.add_argument("--no-sandbox")
        chrome_opts.add_argument("--disable-setuid-sandbox")
        chrome_opts.add_argument("--disable-dev-shm-usage")
        chrome_opts.add_argument("--single-process")
        chrome_opts.add_argument("--disable-gpu")
        chrome_opts.add_argument("--disable-extensions")
        chrome_opts.add_argument("--remote-debugging-port=9222")
        chrome_opts.add_argument("--window-size=1280,1024")

        service = Service("/opt/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=chrome_opts)

    async def run(
            self,
            start_date: datetime.date | None = None,
            max_days: int | None = None
    ) -> list[Screening]:
        """
        TinyTicketCrawler.iter() already grabs all dates at once,
        so override run() to call iter() a single time.
        """
        return [s async for s in self.iter(start_date)]

    async def iter(self, date: datetime.date) -> Generator[Screening, None, None]:
        for theater in self.theaters:
            url = f"{self.base_url}/{theater.cinema_code}"
            self.driver.get(url)

            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".dateLabel"))
            )

            labels = self.driver.find_elements(By.CSS_SELECTOR, ".dateLabel")

            for label in labels:
                raw = label.text.strip()
                m = re.match(r"(\d{2})/(\d{2})", raw)
                if not m:
                    continue
                mm, dd = m.groups()
                play_date = datetime.date(datetime.date.today().year, int(mm), int(dd))

                # grab the next sibling “rail” container
                rail = label.find_element(By.XPATH, "following-sibling::*[1]")
                cards = rail.find_elements(By.CSS_SELECTOR, ".cardContainer")

                for card in cards:
                    box = card.find_element(By.CSS_SELECTOR, ".sq-textbox")

                    title = box.find_element(
                        By.CSS_SELECTOR, ".nameBox span:first-child"
                    ).text.replace("radio_button_checked", "").strip()

                    times_raw = box.find_element(
                        By.CSS_SELECTOR, ".nameBox span:nth-child(2)"
                    ).text.replace("schedule", "").strip()
                    if not times_raw or "-" not in times_raw:
                        continue
                    start_str, end_str = times_raw.split("-", 1)

                    # seats
                    rem_el = box.find_element(By.CSS_SELECTOR, ".salingInfo")
                    raw_text = rem_el.get_attribute("textContent")
                    txt = raw_text.strip().strip("()")
                    seat_match = re.search(r'(?:잔여(\d+)|(매진))\s*/\s*(\d+)', txt)
                    if seat_match:
                        remaining = int(seat_match.group(1)) if seat_match.group(1) else 0
                        total = int(seat_match.group(3))
                    else:
                        remaining = total = None

                    venue = box.find_element(By.CSS_SELECTOR, ".venue").text.strip()

                    yield Screening(
                        provider=self.chain,
                        cinema_code=theater.cinema_code,
                        cinema_name=theater.name,
                        screen_name=theater.name,
                        movie_title=title,
                        play_date=play_date.isoformat(),
                        start_dt=start_str,
                        end_dt=end_str,
                        url=url,
                        remain_seat_cnt=remaining,
                        total_seat_cnt=total,
                        crawl_ts=datetime.datetime.utcnow().isoformat(),
                    )