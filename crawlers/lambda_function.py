import asyncio
import datetime as dt
from crawlers.crawler_registry import CrawlerRegistry
from crawlers.supabase_client import SupabaseClient

def lambda_handler(event, context):
    chains = event.get("chains", ["CGV", "Megabox", "Lotte"])
    max_days = event.get("max_days", 1)
    supabase = SupabaseClient()

    async def run_all():
        for chain in chains:
            try:
                crawler = CrawlerRegistry.get_crawler(chain, supabase)
                print(f"▶ Running crawler for {chain}...")
                screenings = await crawler.run(
                    start_date=dt.date.today(),
                    max_days=max_days
                )
                print(f"✔ {chain}: Crawled {len(screenings)} screenings")
                await crawler.save_to_db(screenings)
            except Exception as e:
                print(f"❌ Error with {chain}: {e}")

    asyncio.run(run_all())

    return {
        "statusCode": 200,
        "body": f"Crawlers run for: {chains}"
    }
