"""
offline_test_mega.py
‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
Run with:  python offline_test_mega.py
This simulates crawling Megabox without writing to Supabase.
"""

import asyncio
import datetime as dt
from crawlers.megabox import MegaboxCrawler


class DummySupabase:
    def fetch_cinemas(self, chain=None):
        return [
            {
                "cinema_code": "1351",  # 코엑스
                "name": "메가박스 코엑스",
                "chain": "Megabox",
                "latitude": 37.5125,
                "longitude": 127.0580,
            },
            {
                "cinema_code": "1562",  # 아트나인
                "name": "아트나인",
                "chain": "Megabox",
                "latitude": 37.4946,
                "longitude": 127.0132,
            },
        ]

    def delete_screenings_by_date_and_chain(self, date_str, chain):
        print(f"[DummySupabase] delete where provider='{chain}' and date='{date_str}'")

    def insert_screenings(self, data):
        print(f"[DummySupabase] insert {len(data)} rows")


async def main():
    crawler = MegaboxCrawler(supabase=DummySupabase(), batch_size=1)
    results = await crawler.run(start_date=dt.date.today(), max_days=14)

    print(f"\n✅ Collected {len(results)} screenings\n")
    for s in results:
        print(
            f"{s.play_date} | {s.cinema_name:<15} | {s.screen_name:<20} | "
            f"{s.start_dt} → {s.end_dt} | {s.movie_title}"
        )

    await crawler.save_to_db(results)


if __name__ == "__main__":
    asyncio.run(main())
