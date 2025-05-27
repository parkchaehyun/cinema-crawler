import asyncio
import datetime as dt
import json
from pathlib import Path
import importlib
from crawlers.crawler_registry import CrawlerRegistry

CHAIN = "Megabox"  # ⬅️ change this to "Lotte", "Megabox", "CGV", etc.

# --- dummy supabase for local testing ---
class DummySupabase:
    def fetch_cinemas(self, chain=None):
        json_path = Path(__file__).parent.parent / "cinemas.json"
        with open(json_path, encoding="utf-8") as f:
            cinemas = json.load(f)
        return [c for c in cinemas if c["chain"] == chain]

    def delete_screenings_by_date_and_chain(self, date_str, chain):
        print(f"[DummySupabase] delete where provider='{chain}' and date='{date_str}'")

    def insert_screenings(self, data):
        print(f"[DummySupabase] insert {len(data)} rows")


async def main():
    crawler = CrawlerRegistry.get_crawler(CHAIN, supabase=DummySupabase)
    start_date = dt.date.today()

    results = await crawler.run(start_date=start_date, max_days=1)
    print(f"{len(results)} results")

    for s in results:
        print(s.model_dump())

    with open(f"{CHAIN.lower()}_screenings.json", "w", encoding="utf-8") as f:
        json.dump([s.model_dump() for s in results], f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    asyncio.run(main())
