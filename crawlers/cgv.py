from __future__ import annotations

import asyncio
import datetime as dt
import os
from pathlib import Path
from typing import Iterable

import httpx
import random
from playwright.async_api import async_playwright, Browser

from crawlers.base import BaseCrawler
from models import Screening, Chain, Cinema
from crawlers.supabase_client import SupabaseClient


class CGVAccessBlockedError(RuntimeError):
    """Raised when CGV serves an access-block page."""


class CGVCrawler(BaseCrawler):
    chain: Chain = "CGV"
    block_markers = (
        "비정상적으로 CGV에 접속한 것이 확인되어 이용이 제한되었어요",
        "RAY_ID",
        "CLIENT_IP",
    )
    modal_selector_candidates = (
        ".cgv-bot-modal.active",
        ".cgv-bot-modal",
        "div[class*='bot-modal']",
    )

    def __init__(self, supabase: SupabaseClient, batch_size: int = 10):
        super().__init__(supabase=supabase, batch_size=batch_size)
        if not self.theaters:
            raise ValueError("No CGV theaters found")

    async def _fetch_proxy_url(self) -> str | None:
        api_key = os.getenv("WEBSHARE_API_KEY")
        if not api_key:
            return None
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    "https://proxy.webshare.io/api/v2/proxy/list/",
                    params={"mode": "direct", "page_size": 100},
                    headers={"Authorization": f"Token {api_key}"},
                )
                resp.raise_for_status()
            proxies = [p for p in resp.json()["results"] if p.get("valid")]
            if not proxies:
                raise ValueError("No valid proxies in list")
            p = random.choice(proxies)
            return (
                f"http://{p['username']}:{p['password']}@"
                f"{p['proxy_address']}:{p['port']}"
            )
        except Exception as e:
            print(f"⚠ Could not fetch proxy list: {e}. Proceeding without proxy.")
            return None

    async def run(
        self, start_date: dt.date | None = None, max_days: int | None = None
    ) -> list[Screening]:
        screenings = []
        crawl_ts = dt.datetime.utcnow()
        headless = os.getenv("CGV_HEADLESS", "1").lower() not in {"0", "false", "no"}
        proxy_url = await self._fetch_proxy_url()
        if proxy_url:
            print("  Using Webshare proxy for CGV crawl.")
        else:
            print("  No proxy configured — proceeding without proxy.")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=headless,
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
                    try:
                        theater_screenings = await self.crawl_theater(
                            browser, theater, theater_index, len(batch), crawl_ts, proxy_url
                        )
                    except CGVAccessBlockedError as exc:
                        print(f"❌ {exc}")
                        print("❌ Stopping CGV crawl early due to access block.")
                        return screenings
                    screenings.extend(theater_screenings)
                    await asyncio.sleep(0)

        return screenings

    async def _is_access_blocked(self, page) -> bool:
        try:
            text = await page.inner_text("body")
        except Exception:
            return False
        normalized = (text or "").replace(" ", "")
        return any(marker.replace(" ", "") in normalized for marker in self.block_markers)

    async def _wait_for_theater_modal(self, page) -> str:
        for selector in self.modal_selector_candidates:
            try:
                await page.wait_for_selector(selector, timeout=7000)
                return selector
            except Exception:
                if await self._is_access_blocked(page):
                    raise CGVAccessBlockedError(
                        "CGV blocked automated access on /cnm/movieBook/cinema."
                    )

        if await self._is_access_blocked(page):
            raise CGVAccessBlockedError(
                "CGV blocked automated access on /cnm/movieBook/cinema."
            )
        raise RuntimeError(
            "CGV theater modal not found. Selector may have changed."
        )

    async def _dump_debug_artifacts(self, page, theater_code: str):
        debug_dir = Path("tmp_chain_samples_escalated")
        debug_dir.mkdir(parents=True, exist_ok=True)
        timestamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        screenshot_path = debug_dir / f"cgv_debug_{theater_code}_{timestamp}.png"
        html_path = debug_dir / f"cgv_debug_{theater_code}_{timestamp}.html"

        try:
            await page.screenshot(path=str(screenshot_path), full_page=True)
            print(f"  Saved debug screenshot: {screenshot_path}")
        except Exception:
            pass

        try:
            html_path.write_text(await page.content(), encoding="utf-8")
            print(f"  Saved debug html: {html_path}")
        except Exception:
            pass

    async def crawl_theater(
        self,
        browser: Browser,
        theater: Cinema,
        theater_index: int,
        batch_size: int,
        crawl_ts: dt.datetime,
        proxy_url: str | None = None,
    ) -> list[Screening]:
        # Create a new browser context with a realistic User-Agent and locale
        context_kwargs = dict(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="ko-KR",
            timezone_id="Asia/Seoul",
        )
        if proxy_url:
            context_kwargs["proxy"] = {"server": proxy_url}
        context = await browser.new_context(**context_kwargs)
        # Inject basic stealth to hide navigator.webdriver
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
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

            async def is_date_span_disabled(span) -> bool:
                try:
                    return await span.evaluate(
                        """el => {
                        const parent = el.parentElement;
                        if (!parent) return false;
                        if (parent.disabled || parent.hasAttribute('disabled')) return true;
                        const classes = parent.className || '';
                        if (classes.includes('disabled') || classes.includes('inactive')) return true;
                        const style = getComputedStyle(parent);
                        if (style.pointerEvents === 'none') return true;
                        return false;
                    }"""
                    )
                except Exception:
                    return True

            page.on("response", handle_response)

            print(f"  Navigating to CGV cinema page...")
            await page.goto(
                "https://cgv.co.kr/cnm/movieBook/cinema",
                wait_until="domcontentloaded",
            )
            modal_selector = await self._wait_for_theater_modal(page)
            print(f"  Clicking on theater: {theater_name_for_click}")
            await page.locator(modal_selector).first.locator(
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
                            if await is_date_span_disabled(span):
                                disabled_dates.append(date_text)
                            else:
                                available_dates.append(date_text)
                        except:
                            continue

                    # Deduplicate while preserving order.
                    def unique(seq):
                        out = []
                        seen = set()
                        for item in seq:
                            if item in seen:
                                continue
                            seen.add(item)
                            out.append(item)
                        return out

                    available_dates = unique(available_dates)
                    disabled_dates = set(disabled_dates)
                    available_dates = [d for d in available_dates if d not in disabled_dates]

                    print(f"  Enabled dates: {available_dates}")
                    if disabled_dates:
                        print(f"  Disabled dates (skipped): {sorted(disabled_dates)}")

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
                                    if (
                                        span_text == target_date
                                        and not await is_date_span_disabled(span)
                                    ):
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
                            await target_span.click(timeout=3000)

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
                            movie_title_en=(screening_data.get("movEnm") or "").strip() or None,
                            source_movie_code=str(
                                screening_data.get("movNo") or ""
                            ).strip() or None,
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

        except CGVAccessBlockedError:
            raise
        except Exception as e:
            print(f"  ERROR processing theater {theater.name}: {e}")
            await self._dump_debug_artifacts(page, theater.cinema_code)
        finally:
            try:
                page.remove_listener("response", handle_response)
            except Exception:
                pass
            # Always close the context
            await context.close()
        return screenings

    async def iter(self, date: dt.date) -> Iterable[Screening]:
        """A-sync generator yielding Screening objects"""
        # This method is no longer used by the CGV crawler's run method
        # but is kept for compatibility with the BaseCrawler interface.
        yield
        return
