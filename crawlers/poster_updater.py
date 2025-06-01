# crawlers/poster_updater.py

import os
import logging
import httpx
from supabase_client import SupabaseClient  # your existing client

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

supabase_wrapper = SupabaseClient()
supabase = supabase_wrapper.client

TMDB_SEARCH_URL  = "https://api.themoviedb.org/3/search/movie"
TMDB_IMAGE_BASE  = "https://image.tmdb.org/t/p/w500"


def fetch_movies_needing_posters() -> list[dict]:
    """
    Select id and title from 'movies' where poster_url IS NULL.
    Returns a list of dicts, or [] if no matches.
    """
    resp = supabase.table("movies") \
                   .select("id, title") \
                   .is_("poster_url", None) \
                   .execute()

    return resp.data or []


def lookup_poster_for(title: str) -> str:
    """
    Call TMDB /3/search/movie using Bearer‐token auth exactly as in your Deno function.
    Returns the full poster URL string (w500), or "" if none is found.
    """
    TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/movie"
    TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_API_KEY}",
    }
    params = {"query": title}

    try:
        r = httpx.get(TMDB_SEARCH_URL, headers=headers, params=params, timeout=10.0)
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        # If you see status_code=401 here, it means the Bearer token is invalid/expired.
        print(f"TMDB 401/other error for '{title}': {e.response.status_code}")
        return ""
    except Exception as e:
        print(f"TMDB request failed for '{title}': {e}")
        return ""

    data = r.json()
    results = data.get("results", [])
    if not results:
        return ""

    first = results[0]
    poster_path = first.get("poster_path")
    if not poster_path:
        return ""

    return TMDB_IMAGE_BASE + poster_path


def update_movie_poster(movie_id: int, poster_url: str) -> bool:
    """
    Update the poster_url column for a given ID.
    """
    resp = supabase.table("movies") \
                   .update({"poster_url": poster_url}) \
                   .eq("id", movie_id) \
                   .execute()
    # We assume success if no exception. You could check resp.status_code if you want.
    return True


def lambda_handler(event, context):
    """
    1) Fetch all movies without a poster_url
    2) For each, call TMDB to get a poster URL
    3) Write it back to Supabase
    """
    logger.info("=== TMDB Poster Updater: Starting run")
    try:
        movies = fetch_movies_needing_posters()
    except Exception as e:
        logger.error(f"Aborting run: {e}")
        return {"status": "error", "message": str(e)}

    logger.info(f"Found {len(movies)} movie(s) needing poster URLs")
    processed = 0

    for movie in movies:
        movie_id = movie.get("id")
        title    = movie.get("title", "").strip()
        if not title:
            logger.warning(f"Skipping id={movie_id} because title is empty")
            continue

        poster_url = lookup_poster_for(title)
        if not poster_url:
            logger.info(f"No poster found for '{title}' (id={movie_id})")
            continue

        update_movie_poster(movie_id, poster_url)

        logger.info(f"Would update movie id={movie_id} → poster_url={poster_url}")
        processed += 1

    logger.info(f"=== Completed run; processed {processed}/{len(movies)}")
    return {"status": "success", "processed": processed}