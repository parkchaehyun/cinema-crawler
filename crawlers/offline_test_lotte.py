import asyncio
from crawlers.lotte import LotteCinemaCrawler  # adjust if module path differs
import datetime as dt
import json

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
    crawler = LotteCinemaCrawler(supabase=DummySupabase(), batch_size=1)
    results = await crawler.run(start_date=dt.date.today(), max_days=14)

    print(f"{len(results)} results")
    for s in results:
        print(s.model_dump())

    with open("lotte_screenings.json", "w", encoding="utf-8") as f:
        json.dump([s.model_dump() for s in results], f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    asyncio.run(main())
