from supabase import create_client, Client
import os
from datetime import datetime
from typing import List, Dict, Any
from models import Chain, Screening


class SupabaseClient:
    def __init__(self):
        """Initialize Supabase client using environment variables."""
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        self.client: Client = create_client(url, key)

    def insert_screenings(self, data: list[Screening]):
        """Insert screenings into Supabase."""
        payload = [s.model_dump(exclude_none=True) for s in data]
        (
            self.client.table("screenings").upsert(
                payload,
                on_conflict="provider,cinema_code,play_date,start_dt,screen_name",
            ).execute()
        )

    def fetch_cinemas(self, chain: Chain | None = None) -> List[Dict[str, Any]]:
        """Fetch cinemas from Supabase, optionally filtered by chain."""
        query = self.client.table("cinemas").select("*")
        if chain:
            query = query.eq("chain", chain)
        response = query.execute()
        return response.data

    def insert_cinemas(self, cinemas: List[Dict[str, Any]]) -> None:
        """Insert cinemas into Supabase."""
        self.client.table("cinemas").insert(cinemas).execute()