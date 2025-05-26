import abc
from typing import Iterable
import datetime as dt
from models import Screening, Chain

class BaseCrawler(abc.ABC):
    chain: Chain

    def __init__(self, batch_size: int = 10):
        self.batch_size = batch_size

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

    async def save_to_db(self, screenings: list[Screening]) -> None:
        """Save screenings to database (implemented in subclasses)"""
        pass