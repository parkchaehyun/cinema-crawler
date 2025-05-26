from supabase import create_client, Client
import os
from datetime import datetime
from typing import List, Dict, Any
from models import Chain

class SupabaseClient:
    def __init__(self):
        """Initialize Supabase client using environment variables."""
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")
        self.client: Client = create_client(url, key)

    def insert_screenings(self, screenings: List[Dict[str, Any]]) -> None:
        """Insert screenings into Supabase."""
        self.client.table("screenings").insert(screenings).execute()

    def delete_screenings_by_date_and_chain(self, date_str: str, chain: Chain) -> None:
        """Delete screenings for a specific date and chain."""
        self.client.table("screenings").delete().eq("play_date", date_str).eq("provider", chain).execute()

    def fetch_screenings(self, min_time: str, chain: Chain) -> List[Dict[str, Any]]:
        """Fetch all screenings after min_time (HH:MM) for a chain."""
        response = self.client.table("screenings")\
            .select("*")\
            .eq("provider", chain)\
            .gte("start_dt::time", min_time)\
            .execute()
        return response.data

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