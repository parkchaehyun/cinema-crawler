import abc
import json
import logging
import os
from pathlib import Path
from typing import Iterable, List, get_args
import datetime as dt
from models import Screening, Chain

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseCrawler(abc.ABC):
    chain: Chain

    def __init__(self, supabase=None, batch_size: int = 10):
        if not hasattr(self, "chain") or self.chain not in get_args(Chain):
            raise ValueError(f"Invalid chain: {getattr(self, 'chain', None)}")

        self.supabase = supabase
        self.batch_size = batch_size
        self.theaters: List[dict] = self.load_theaters()
    def load_theaters(self) -> List[dict]:
        """
        Load theaters from local JSON or fallback to Supabase.
        Filters by `self.chain`.
        """
        try:
            root_dir = Path(__file__).parent.parent
            json_path = root_dir / "cinemas.json"
            if json_path.exists():
                with open(json_path, encoding="utf-8") as fp:
                    data = [c for c in json.load(fp) if c["chain"] == self.chain]
                logger.info("Loaded %d %s theaters from %s", len(data), self.chain, json_path)
                return data
            elif self.supabase:
                data = self.supabase.fetch_cinemas(chain=self.chain)
                logger.info("Loaded %d %s theaters from Supabase", len(data), self.chain)
                return data
        except Exception as exc:
            logger.error("Error loading theaters: %s", exc)
        return []

    async def save_to_db(self, screenings: List) -> None:
        if not screenings:
            return
        try:
            data = [s.model_dump() for s in screenings]
            self.supabase.delete_screenings_by_date_and_chain(
                screenings[0].play_date, self.chain
            )
            self.supabase.insert_screenings(data)
            logger.info("Saved %d %s screenings to Supabase", len(data), self.chain)
        except Exception as exc:
            logger.error("Supabase save error: %s", exc)
            raise

    async def run(
            self,
            start_date: dt.date | None = None,
            max_days: int | None = None  # ⬅️ safety valve; None == unlimited
    ) -> list[Screening]:
        """
        Crawl day-by-day until `iter()` yields nothing.
        Optional `max_days` stops the loop after N days even
        if data keeps coming (guards against site bugs).
        """
        start = start_date or dt.date.today()
        collected: list[Screening] = []

        day_offset = 0
        while True:
            if max_days is not None and day_offset >= max_days:
                break

            target_date = start + dt.timedelta(days=day_offset)
            day_screenings = [s async for s in self.iter(target_date)]

            # nothing for this date → we’re done
            if not day_screenings:
                break

            collected.extend(day_screenings)
            day_offset += 1

        return collected

    @abc.abstractmethod
    async def iter(self, date: dt.date) -> Iterable[Screening]:
        """A-sync generator yielding Screening objects"""