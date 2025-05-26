from datetime import datetime, date
from typing import Literal
from pydantic import BaseModel, Field
import uuid

Chain = Literal["CGV", "Megabox", "Lotte", "TinyTicket", "Moonhwain", "Dtryx"]


class Screening(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique identifier")
    provider: Chain
    cinema_name: str
    cinema_code: str
    screen_name: str
    movie_title: str  # '2025-05-26'
    play_date: str  # e.g. 2025-05-26
    start_dt: str = Field(..., pattern=r'^\d{2}:\d{2}$',
                          description="Start time in HH:MM format, allowing 00:00 to 26:59")
    end_dt: str = Field(..., pattern=r'^\d{2}:\d{2}$', description="End time in HH:MM format, allowing 00:00 to 26:59")
    crawl_ts: str  # '2025-05-26T12:34:56'


class Cinema(BaseModel):
    cinema_code: str = Field(..., description="Unique cinema code, e.g., '0013'")
    name: str = Field(..., description="Cinema name, e.g., 'CGV용산아이파크몰'")
    chain: Chain
    latitude: float = Field(..., description="GPS latitude, e.g., 37.5299")
    longitude: float = Field(..., description="GPS longitude, e.g., 126.9648")
