from typing import Dict, Type
from models import Chain
from crawlers.base import BaseCrawler
from crawlers.cgv import CGVCrawler
from crawlers.megabox import MegaboxCrawler
from crawlers.supabase_client import SupabaseClient

class CrawlerRegistry:
    _crawlers: Dict[Chain, Type[BaseCrawler]] = {
        "CGV": CGVCrawler,
        "Megabox": MegaboxCrawler,
        # Add others: "Megabox": MegaboxCrawler, "Lotte": LotteCrawler, etc.
    }

    @classmethod
    def get_crawler(cls, chain: Chain, supabase: SupabaseClient, batch_size: int = 10) -> BaseCrawler:
        """Get crawler instance for a chain."""
        crawler_class = cls._crawlers.get(chain)
        if not crawler_class:
            raise ValueError(f"No crawler registered for chain: {chain}")
        return crawler_class(supabase=supabase, batch_size=batch_size)

    @classmethod
    def register_crawler(cls, chain: Chain, crawler_class: Type[BaseCrawler]) -> None:
        """Register a new crawler for a chain."""
        cls._crawlers[chain] = crawler_class