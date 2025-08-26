from __future__ import annotations

import asyncio
import datetime as dt
from typing import Iterable

from playwright.async_api import async_playwright, Browser

from crawlers.base import BaseCrawler
from models import Screening, Chain, Cinema
from crawlers.supabase_client import SupabaseClient


class CGVCrawler(BaseCrawler):
    chain: Chain = "CGV"

    def __init__(self, supabase: SupabaseClient, batch_size: int = 10):
        super().__init__(supabase=supabase, batch_size=batch_size)
        if not self.theaters:
            raise ValueError("No CGV theaters found")

    async def run(
        self, start_date: dt.date | None = None, max_days: int | None = None
    ) -> list[Screening]:
        screenings = []
        crawl_ts = dt.datetime.utcnow()

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--no-zygote",
                    "--disable-setuid-sandbox",
                    "--disable-accelerated-2d-canvas",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-background-networking",
                    "--disable-background-timer-throttling",
                    "--disable-client-side-phishing-detection",
                    "--disable-component-update",
                    "--disable-default-apps",
                    "--disable-domain-reliability",
                    "--disable-features=AudioServiceOutOfProcess",
                    "--disable-hang-monitor",
                    "--disable-ipc-flooding-protection",
                    "--disable-popup-blocking",
                    "--disable-prompt-on-repost",
                    "--disable-renderer-backgrounding",
                    "--disable-sync",
                    "--force-color-profile=srgb",
                    "--metrics-recording-only",
                    "--mute-audio",
                    "--no-pings",
                    "--use-gl=swiftshader",
                    "--window-size=1280,1696",
                ],
            )

            for i in range(0, len(self.theaters), self.batch_size):
                batch = self.theaters[i : i + self.batch_size]
                for theater_index, theater in enumerate(batch):
                    theater_screenings = await self.crawl_theater(
                        browser, theater, theater_index, len(batch), crawl_ts
                    )
                    screenings.extend(theater_screenings)
                    await asyncio.sleep(0)

        return screenings

    async def crawl_theater(
        self,
        browser: Browser,
        theater: Cinema,
        theater_index: int,
        batch_size: int,
        crawl_ts: dt.datetime,
    ) -> list[Screening]:
        # Create a new browser context for each theater for isolation
        context = await browser.new_context()
        page = await context.new_page()
        screenings = []

        try:
            theater_name_for_click = theater.name.replace("CGV", "").strip()
            print(
                f"Processing theater {theater_index + 1}/{batch_size}: {theater.name}"
            )

            # Track API responses with loading detection
            theater_data = []
            completed_requests = set()

            async def handle_response(response):
                if (
                    "searchMovScnInfo" in response.url
                    and f"siteNo={theater.cinema_code}" in response.url
                ):
                    # Extract date from URL for tracking
                    import re

                    date_match = re.search(r'scnYmd=(\d{8})', response.url)
                    date = date_match.group(1) if date_match else "unknown"

                    data = await response.json()
                    if data and data.get("statusCode") == 0 and data.get("data"):
                        new_screenings = len(data["data"])
                        theater_data.extend(data["data"])
                        completed_requests.add(date)
                        print(
                            f"    API loaded {new_screenings} screenings for date {date}"
                        )
                    else:
                        print(f"    API returned no data for date {date}")
                        completed_requests.add(date)

            async def wait_for_data_load(max_wait_seconds=10):
                """Wait for API response"""
                start_time = asyncio.get_event_loop().time()
                initial_count = len(theater_data)

                while (
                    asyncio.get_event_loop().time() - start_time
                ) < max_wait_seconds:
                    if len(theater_data) > initial_count:
                        return True
                    await asyncio.sleep(0.1)

                return False

            page.on("response", handle_response)

            print(f"  Navigating to CGV cinema page...")
            await page.goto("https://cgv.co.kr/cnm/movieBook/cinema")
            await page.wait_for_selector(".cgv-bot-modal.active")
            print(f"  Clicking on theater: {theater_name_for_click}")
            await page.locator(".cgv-bot-modal.active").locator(
                f'text="{theater_name_for_click}"'
            ).click()

            print(f"  Waiting for initial data to load...")
            initial_load_success = await wait_for_data_load(max_wait_seconds=15)
            if not initial_load_success:
                print(f"  WARNING: Initial data load timed out!")

            print(f"  Initial data loaded: {len(theater_data)} screenings")

            # Find all available date navigation elements for this specific theater
            try:
                # Get fresh date elements each time to avoid stale references
                date_spans = await page.query_selector_all(
                    "span.dayScroll_number__o8i9s"
                )
                print(f"  Found {len(date_spans)} total date elements for {theater.name}")

                if len(date_spans) == 0:
                    print(f"  WARNING: No date navigation elements found!")
                else:
                    # Get only ENABLED/CLICKABLE date texts to avoid disabled dates
                    available_dates = []
                    disabled_dates = []

                    for span in date_spans:
                        try:
                            date_text = await span.inner_text()
                            # Check if the parent element (button) is enabled/clickable
                            parent = await span.evaluate("el => el.parentElement")
                            if parent:
                                # Check for disabled state or greyed out classes
                                is_disabled = await span.evaluate(
                                    '''el => {
                                    const parent = el.parentElement;
                                    if (!parent) return true;

                                    // Check if parent button is disabled
                                    if (parent.disabled || parent.hasAttribute('disabled')) return true;

                                    // Check for disabled/inactive classes
                                    const classes = parent.className || '';
                                    if (classes.includes('disabled') || classes.includes('inactive')) return true;

                                    // Check if element is clickable (has click events or is a button/link)
                                    const tagName = parent.tagName.toLowerCase();
                                    if (tagName === 'button' || tagName === 'a') {
                                        // Additional check for visual disabled state
                                        const style = getComputedStyle(parent);
                                        if (style.pointerEvents === 'none' || style.opacity === '0.5') return true;
                                    }

                                    return false;
                                }'''
                                )

                                if is_disabled:
                                    disabled_dates.append(date_text)
                                else:
                                    available_dates.append(date_text)
                            else:
                                available_dates.append(
                                    date_text
                                )  # Fallback if no parent
                        except:
                            continue

                    print(f"  Enabled dates: {available_dates}")
                    if disabled_dates:
                        print(f"  Disabled dates (skipped): {disabled_dates}")

                    # Skip the first (earliest) date since it's already loaded during initial page load
                    dates_to_click = available_dates[1:] if available_dates else []
                    if available_dates:
                        print(
                            f"  Skipping first date '{available_dates[0]}' (already loaded)"
                        )
                        print(
                            f"  Will click remaining {len(dates_to_click)} dates: {dates_to_click}"
                        )

                    # Click through remaining available dates using fresh queries
                    for j, target_date in enumerate(dates_to_click):
                        try:
                            print(
                                f"    [{j+1}/{len(dates_to_click)}] Clicking on date: {target_date}"
                            )

                            # Re-query to get a fresh element reference
                            fresh_date_spans = await page.query_selector_all(
                                "span.dayScroll_number__o8i9s"
                            )
                            target_span = None

                            for span in fresh_date_spans:
                                try:
                                    span_text = await span.inner_text()
                                    if span_text == target_date:
                                        target_span = span
                                        break
                                except:
                                    continue

                            if not target_span:
                                print(
                                    f"    ✗ Could not find date {target_date} on current page"
                                )
                                continue

                            initial_count = len(theater_data)
                            await target_span.click()

                            # Smart wait for data load
                            load_success = await wait_for_data_load(max_wait_seconds=8)

                            new_count = len(theater_data)
                            added_count = new_count - initial_count

                            if load_success and added_count > 0:
                                print(
                                    f"    ✓ Added {added_count} new screenings (Total: {new_count})"
                                )
                            elif load_success and added_count == 0:
                                print(
                                    f"    ⚠ Date {target_date} loaded but no new screenings (Total: {new_count})"
                                )
                            else:
                                print(
                                    f"    ✗ Timeout waiting for date {target_date} (Total: {new_count})"
                                )

                        except Exception as e:
                            print(f"    ✗ Failed to click date {target_date}: {e}")
                            continue

            except Exception as e:
                print(f"  Error finding date elements: {e}")

            # Process all collected screening data
            print(f"  Total screenings collected: {len(theater_data)}")
            for screening_data in theater_data:
                if screening_data.get("sascnsGradNm") == "아트하우스":
                    # Construct URL using the existing theater_name_for_click variable
                    movie_url = (
                        f'https://cgv.co.kr/cnm/movieBook/movie?'
                        f'movNo={screening_data["movNo"]}&'
                        f'scnYmd={screening_data["scnYmd"]}&'
                        f'siteNo={screening_data["siteNo"]}&'
                        f'siteNm={theater_name_for_click}&'
                        f'scnsNo={screening_data["scnsNo"]}&'
                        f'scnSseq={screening_data["scnSseq"]}'
                    )

                    screenings.append(
                        Screening(
                            provider=self.chain,
                            cinema_name=theater.name,
                            cinema_code=screening_data["siteNo"],
                            screen_name=screening_data["scnsNm"],
                            movie_title=screening_data["movNm"],
                            start_dt=f'{screening_data["scnsrtTm"][:2]}:{screening_data["scnsrtTm"][2:]}',
                            end_dt=f'{screening_data["scnendTm"][:2]}:{screening_data["scnendTm"][2:]}',
                            play_date=f'{screening_data["scnYmd"][:4]}-{screening_data["scnYmd"][4:6]}-{screening_data["scnYmd"][6:]}',
                            crawl_ts=crawl_ts.isoformat(),
                            url=movie_url,
                            remain_seat_cnt=int(screening_data["frSeatCnt"]),
                            total_seat_cnt=int(screening_data["stcnt"]),
                        )
                    )

            page.remove_listener("response", handle_response)
            print(f"  Completed theater {theater.name}")

        except Exception as e:
            print(f"  ERROR processing theater {theater.name}: {e}")
        finally:
            # Always close the context
            await context.close()
        return screenings

    async def iter(self, date: dt.date) -> Iterable[Screening]:
        """A-sync generator yielding Screening objects"""
        # This method is no longer used by the CGV crawler's run method
        # but is kept for compatibility with the BaseCrawler interface.
        yield
        return