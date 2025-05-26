import asyncio
import json
import logging
import datetime as dt
from crawlers.crawler_registry import CrawlerRegistry
from crawlers.supabase_client import SupabaseClient
from models import Chain

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_crawlers():
    supabase = SupabaseClient()
    chains: list[Chain] = ["CGV"]  # Add "Megabox", "Lotte", etc. when implemented
    total_screenings = 0
    for chain in chains:
        try:
            crawler = CrawlerRegistry.get_crawler(chain, supabase)
            screenings = await crawler.run(dt.date.today())
            await crawler.save_to_db(screenings)
            total_screenings += len(screenings)
            logger.info(f"Processed {len(screenings)} screenings for {chain}")
        except Exception as e:
            logger.error(f"Error processing {chain}: {e}")
    return total_screenings

def lambda_handler(event, context):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        count = loop.run_until_complete(run_crawlers())
        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Processed {count} screenings"})
        }
    except Exception as e:
        logger.error(f"Lambda error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }