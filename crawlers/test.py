"""
offline_test_cgv.py
‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
Run with:  python -m asyncio offline_test_cgv
Nothing is written to Supabase — all DB calls are echoed.
"""

import asyncio
import datetime as dt
from crawlers.cgv import CGVCrawler


class DummySupabase:
    """
    Enough of the Supabase API to keep CGVCrawler happy.
    Everything is NO-OP + a print for visibility.
    """

    def fetch_cinemas(self, chain=None):
        # Supply one theatre so __init__ doesn’t raise.
        return [
            {
                "cinema_code": "0013",
                "name": "CGV용산아이파크몰",
                "chain": "CGV",
                "latitude": 37.5299,
                "longitude": 126.9648,
                "areacode": "01",
            }
        ]

    # The crawler uses these two on save
    def delete_screenings_by_date_and_chain(self, date_str, chain):
        print(f"[DummySupabase] delete where provider='{chain}' and date='{date_str}'")

    def insert_screenings(self, data):
        print(f"[DummySupabase] insert {len(data)} rows")


async def main():
    crawler = CGVCrawler(supabase=DummySupabase(), batch_size=1)

    # start today – the crawler itself keeps moving to tomorrow
    results = await crawler.run(dt.date.today(), max_days=14)

    print(f"\n✅  Collected {len(results)} screenings\n")
    for s in results:                       # peek at the first few
        print(
            f"{s.play_date} | {s.cinema_name:<15} | {s.screen_name:<8} | "
            f"{s.start_dt} → {s.end_dt} | {s.movie_title}"
        )

    # pretend-save to the dummy DB
    await crawler.save_to_db(results)


if __name__ == "__main__":
    asyncio.run(main())
