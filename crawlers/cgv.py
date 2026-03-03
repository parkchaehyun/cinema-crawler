from __future__ import annotations

import asyncio
import datetime as dt
import os
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import httpx
import random
from playwright.async_api import async_playwright, Browser, TimeoutError as PlaywrightTimeoutError

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

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.lower() in {"1", "true", "yes", "on"}

    async def _fetch_proxy(self) -> dict | None:
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
            return {
                "server": f"http://{p['proxy_address']}:{p['port']}",
                "username": p["username"],
                "password": p["password"],
            }
        except Exception as e:
            print(f"⚠ Could not fetch proxy list: {e}. Proceeding without proxy.")
            return None

    async def run(
        self, start_date: dt.date | None = None, max_days: int | None = None
    ) -> list[Screening]:
        screenings = []
        crawl_ts = dt.datetime.utcnow()
        headless = os.getenv("CGV_HEADLESS", "1").lower() not in {"0", "false", "no"}
        proxy = await self._fetch_proxy()
        if proxy:
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
                            browser, theater, theater_index, len(batch), crawl_ts, proxy
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
        debug_dir = (
            Path("/tmp/cgv_debug")
            if os.getenv("AWS_LAMBDA_FUNCTION_NAME")
            else Path("tmp_chain_samples_escalated")
        )
        try:
            debug_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"  Could not create debug dir ({debug_dir}): {e}")
            return
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
        proxy: dict | None = None,
    ) -> list[Screening]:
        # Create a new browser context with a realistic User-Agent and locale
        context_kwargs = dict(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="ko-KR",
            timezone_id="Asia/Seoul",
        )
        if proxy:
            context_kwargs["proxy"] = proxy
        context = await browser.new_context(**context_kwargs)
        # Inject basic stealth to hide navigator.webdriver
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()
        bandwidth_saver = self._env_bool("CGV_BANDWIDTH_SAVER", default=False)
        blocked_counts = {"font": 0, "tracker": 0, "image": 0}
        tracker_hosts = {
            "www.googletagmanager.com",
            "analytics.google.com",
            "stats.g.doubleclick.net",
            "ad.cgv.co.kr",
            "www.google.co.kr",
        }
        route_handler = None
        if bandwidth_saver:
            async def _route_with_bandwidth_saver(route):
                req = route.request
                host = urlparse(req.url).netloc
                rtype = req.resource_type

                if host in tracker_hosts:
                    blocked_counts["tracker"] += 1
                    await route.abort()
                    return
                if rtype == "font":
                    blocked_counts["font"] += 1
                    await route.abort()
                    return
                if rtype == "image":
                    blocked_counts["image"] += 1
                    await route.abort()
                    return
                await route.continue_()
            route_handler = _route_with_bandwidth_saver
        screenings = []

        try:
            theater_name_for_click = theater.name.replace("CGV", "").strip()
            print(
                f"Processing theater {theater_index + 1}/{batch_size}: {theater.name}"
            )

            # Track API payloads deterministically (no background callbacks).
            theater_data = []
            seen_schedule_keys = set()

            def is_schedule_response(response) -> bool:
                return (
                    "searchMovScnInfo" in response.url
                    and f"siteNo={theater.cinema_code}" in response.url
                )

            async def append_from_response(response) -> int:
                try:
                    import re

                    date_match = re.search(r"scnYmd=(\d{8})", response.url)
                    date = date_match.group(1) if date_match else "unknown"

                    data = await response.json()
                    if not (data and data.get("statusCode") == 0 and data.get("data")):
                        print(f"    API returned no data for date {date}")
                        return 0

                    payload = data["data"]
                    new_unique = 0
                    for item in payload:
                        key = (
                            item.get("siteNo"),
                            item.get("movNo"),
                            item.get("scnYmd"),
                            item.get("scnsNo"),
                            item.get("scnSseq"),
                            item.get("scnsrtTm"),
                        )
                        if key in seen_schedule_keys:
                            continue
                        seen_schedule_keys.add(key)
                        theater_data.append(item)
                        new_unique += 1

                    print(
                        f"    API loaded {len(payload)} screenings for date {date} "
                        f"(new: {new_unique})"
                    )
                    return new_unique
                except Exception as e:
                    print(f"    WARN: Failed to parse schedule response: {e}")
                    return 0

            async def collect_schedule_after_action(
                action, first_timeout_ms: int, followup_timeout_ms: int = 800
            ) -> tuple[int, bool]:
                total_added = 0
                try:
                    async with page.expect_response(
                        is_schedule_response, timeout=first_timeout_ms
                    ) as first_resp_info:
                        await action()
                    first_resp = await first_resp_info.value
                    total_added += await append_from_response(first_resp)
                except PlaywrightTimeoutError:
                    return 0, False

                # Collect trailing schedule responses emitted by the same UI action.
                while True:
                    try:
                        resp = await page.wait_for_event(
                            "response",
                            predicate=is_schedule_response,
                            timeout=followup_timeout_ms,
                        )
                    except PlaywrightTimeoutError:
                        break
                    total_added += await append_from_response(resp)
                return total_added, True

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

            async def attach_page_hooks(target_page):
                if route_handler is not None:
                    await target_page.route("**/*", route_handler)

            if bandwidth_saver:
                print("  Bandwidth saver ON (fonts + trackers + images)")
            await attach_page_hooks(page)

            url = "https://cgv.co.kr/cnm/movieBook/cinema"
            goto_attempts = ((1, 12000), (2, 18000))
            for attempt, timeout_ms in goto_attempts:
                try:
                    print(f"  Navigating to CGV cinema page... (attempt {attempt}/2)")
                    await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=timeout_ms,
                    )
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    print(f"  WARN: page.goto retrying after error: {e}")
                    try:
                        await page.close()
                    except Exception:
                        pass
                    page = await context.new_page()
                    await attach_page_hooks(page)
                    await asyncio.sleep(0.5)
            modal_selector = await self._wait_for_theater_modal(page)
            print(f"  Clicking on theater: {theater_name_for_click}")
            print(f"  Waiting for initial data to load...")
            async def click_theater():
                await page.locator(modal_selector).first.locator(
                    f'text="{theater_name_for_click}"'
                ).click()

            _, initial_load_success = await collect_schedule_after_action(
                click_theater, first_timeout_ms=15000
            )
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
                            async def click_date():
                                await target_span.click(timeout=3000)

                            added_count, load_success = await collect_schedule_after_action(
                                click_date, first_timeout_ms=8000
                            )
                            new_count = len(theater_data)

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

            print(f"  Completed theater {theater.name}")
            if bandwidth_saver:
                print(
                    "  Bandwidth saver blocked: "
                    f"font={blocked_counts['font']} "
                    f"tracker={blocked_counts['tracker']} "
                    f"image={blocked_counts['image']}"
                )

        except CGVAccessBlockedError:
            raise
        except Exception as e:
            print(f"  ERROR processing theater {theater.name}: {e}")
            await self._dump_debug_artifacts(page, theater.cinema_code)
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
