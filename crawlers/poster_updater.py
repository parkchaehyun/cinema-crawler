import os
import re
import logging
from datetime import datetime, timezone
import httpx
try:
    # TMDB Lambda image flattens this module into /var/task/supabase_client.py
    from supabase_client import SupabaseClient
except ModuleNotFoundError:
    # Local repo layout
    from crawlers.supabase_client import SupabaseClient

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

supabase_wrapper = SupabaseClient()
supabase = supabase_wrapper.client

TMDB_SEARCH_URL  = "https://api.themoviedb.org/3/search/movie"
TMDB_IMAGE_BASE  = "https://image.tmdb.org/t/p/w500"
SEARCH_LANGUAGES = ("ko-KR", "en-US")
EVENT_KEYWORDS = (
    "gv",
    "시네토크",
    "영화소개",
    "강연",
    "무대인사",
    "인디토크",
    "토크",
    "온라인",
)
MOVIE_FETCH_CHUNK_SIZE = 500


def fetch_movies_needing_posters() -> list[dict]:
    """
    Select upcoming movie rows that have not been linked to TMDB yet.
    Returns a list of dicts, or [] if no matches.
    """
    upcoming_resp = (
        supabase.table("upcoming_movie_ids")
        .select("movie_id")
        .execute()
    )

    upcoming_ids = sorted(
        {
            row.get("movie_id")
            for row in (upcoming_resp.data or [])
            if row.get("movie_id") is not None
        }
    )
    if not upcoming_ids:
        return []

    movies: list[dict] = []
    for idx in range(0, len(upcoming_ids), MOVIE_FETCH_CHUNK_SIZE):
        chunk = upcoming_ids[idx: idx + MOVIE_FETCH_CHUNK_SIZE]
        movie_resp = (
            supabase.table("movies")
            .select("id, title, canonical_title")
            .in_("id", chunk)
            .is_("tmdb_id", None)
            .execute()
        )
        movies.extend(movie_resp.data or [])

    return movies


def _normalize_for_match(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"\s*\[[^\]]*\]\s*", " ", value)
    value = re.sub(r"\s*\([^)]*\)\s*", " ", value)
    value = value.replace("+", " ")
    value = re.sub(r"[^0-9a-zA-Z가-힣\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _trim_trailing_parenthetical(title: str) -> str:
    prev = title
    while True:
        curr = re.sub(r"\s*\([^)]*\)\s*$", "", prev).strip()
        if curr == prev:
            return curr
        prev = curr


def _contains_event_keyword(value: str) -> bool:
    return bool(
        re.search(
            r"(?:%s)" % "|".join(EVENT_KEYWORDS),
            value,
            flags=re.IGNORECASE,
        )
    )


def _build_title_candidates(title: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add(value: str):
        v = re.sub(r"\s+", " ", value).strip()
        if len(v) < 2 or v in seen:
            return
        seen.add(v)
        candidates.append(v)

    raw = title.strip()
    add(raw)

    without_bracket = re.sub(r"^\[[^\]]+\]\s*", "", raw).strip()
    add(without_bracket)

    without_tail_paren = _trim_trailing_parenthetical(without_bracket)
    add(without_tail_paren)

    no_event_suffix = without_tail_paren
    plus_parts = re.split(r"\s*\+\s*", without_tail_paren, maxsplit=1)
    if len(plus_parts) == 2:
        left_part, right_part = plus_parts[0].strip(), plus_parts[1].strip()
        # For composite titles, prefer the left/main segment.
        no_event_suffix = left_part
        add(left_part)
        # If suffix clearly looks like event text, keep only the left side.
        if _contains_event_keyword(right_part):
            no_event_suffix = left_part

    no_event_keyword = re.sub(
        r"\s+(?:%s)\b.*$" % "|".join(EVENT_KEYWORDS),
        "",
        no_event_suffix,
        flags=re.IGNORECASE,
    ).strip()
    add(no_event_keyword)

    # Fallback for composite programs: only first segment to reduce false positives.
    first_segment = re.split(r"\s*\+\s*|/", raw, maxsplit=1)[0].strip()
    first_segment = re.sub(r"^\[[^\]]+\]\s*", "", first_segment).strip()
    first_segment = _trim_trailing_parenthetical(first_segment)
    if not _contains_event_keyword(first_segment):
        add(first_segment)

    return candidates


def _score_result(result: dict, query: str, original_title: str) -> int:
    poster_path = result.get("poster_path")
    if not poster_path:
        return -10_000

    normalized_query = _normalize_for_match(query)
    normalized_original = _normalize_for_match(original_title)
    normalized_result_titles = {
        _normalize_for_match(result.get("title") or ""),
        _normalize_for_match(result.get("original_title") or ""),
    }
    normalized_result_titles.discard("")

    if not normalized_result_titles:
        return -10_000

    score = 0

    if normalized_query in normalized_result_titles:
        score += 100
    elif any(
        normalized_query and (normalized_query in rt or rt in normalized_query)
        for rt in normalized_result_titles
    ):
        score += 55

    if normalized_original in normalized_result_titles:
        score += 45
    elif any(
        normalized_original and (normalized_original in rt or rt in normalized_original)
        for rt in normalized_result_titles
    ):
        score += 20

    if result.get("release_date"):
        score += 5

    popularity = result.get("popularity")
    if isinstance(popularity, (int, float)):
        score += min(int(popularity), 20)

    return score


def _find_best_result(results: list[dict], query: str, original_title: str) -> dict | None:
    best: dict | None = None
    best_score = -10_000
    normalized_query = _normalize_for_match(query)

    for result in results:
        score = _score_result(result, query, original_title)
        if score > best_score:
            best = result
            best_score = score

    # Guard against false positives on very short queries ("금", "암호", etc.)
    if normalized_query and len(normalized_query) <= 2 and best_score < 100:
        return None
    if best_score < 55:
        return None
    return best


def _search_tmdb(client: httpx.Client, query: str, language: str) -> list[dict]:
    params = {
        "query": query,
        "language": language,
        "include_adult": "false",
        "region": "KR",
    }
    response = client.get(TMDB_SEARCH_URL, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("results", [])


def lookup_poster_for(title: str, client: httpx.Client) -> dict | None:
    """
    Try multiple normalized query variants and languages.
    Returns TMDB match payload when matched.
    """
    for query in _build_title_candidates(title):
        for language in SEARCH_LANGUAGES:
            try:
                results = _search_tmdb(client, query=query, language=language)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "TMDB HTTP error for title='%s' query='%s' lang=%s status=%s",
                    title,
                    query,
                    language,
                    exc.response.status_code if exc.response else "unknown",
                )
                continue
            except Exception as exc:
                logger.error(
                    "TMDB request failed for title='%s' query='%s' lang=%s error=%s",
                    title,
                    query,
                    language,
                    exc,
                )
                continue

            best = _find_best_result(results, query=query, original_title=title)
            if not best:
                continue

            poster_path = best.get("poster_path")
            if not poster_path:
                continue

            tmdb_id = best.get("id")
            if not tmdb_id:
                continue

            score = _score_result(best, query=query, original_title=title)
            return {
                "tmdb_id": tmdb_id,
                "poster_url": TMDB_IMAGE_BASE + poster_path,
                "matched_query": query,
                "matched_tmdb_title": best.get("title") or best.get("original_title") or "",
                "original_title": best.get("original_title") or None,
                "release_date": best.get("release_date") or None,
                "tmdb_language": best.get("original_language") or language,
                "tmdb_match_score": float(score),
            }

    return None


def update_movie_poster(movie_id: int, match: dict) -> bool:
    """
    Update TMDB enrichment fields for a given movie ID.
    """
    payload = {
        "tmdb_id": match.get("tmdb_id"),
        "poster_url": match.get("poster_url"),
        "original_title": match.get("original_title"),
        "release_date": match.get("release_date"),
        "tmdb_language": match.get("tmdb_language"),
        "tmdb_match_score": match.get("tmdb_match_score"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    (
        supabase.table("movies")
        .update(payload)
        .eq("id", movie_id)
        .is_("tmdb_id", None)
        .execute()
    )
    # We assume success if no exception. You could check resp.status_code if you want.
    return True


def lambda_handler(event, context):
    """
    1) Fetch movies missing TMDB identity
    2) Resolve TMDB match for each movie title
    3) Write TMDB metadata + poster URL back to Supabase
    """
    logger.info("=== TMDB Poster Updater: Starting run")
    if not TMDB_API_KEY:
        message = "TMDB_API_KEY is not set"
        logger.error(message)
        return {"status": "error", "message": message}

    try:
        movies = fetch_movies_needing_posters()
    except Exception as e:
        logger.error(f"Aborting run: {e}")
        return {"status": "error", "message": str(e)}

    logger.info(f"Found {len(movies)} movie(s) needing TMDB enrichment")
    processed = 0

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_API_KEY}",
    }
    with httpx.Client(timeout=10.0, headers=headers) as client:
        for movie in movies:
            movie_id = movie.get("id")
            canonical_title = (movie.get("canonical_title") or "").strip()
            fallback_title = (movie.get("title") or "").strip()
            title = canonical_title or fallback_title
            if not title:
                logger.warning("Skipping id=%s because title is empty", movie_id)
                continue

            lookup_result = lookup_poster_for(title, client)
            if not lookup_result:
                logger.info("No poster found for '%s' (id=%s)", title, movie_id)
                continue

            update_movie_poster(movie_id, lookup_result)

            logger.info(
                "Updated movie id=%s title='%s' tmdb_id=%s matched_query='%s' tmdb_title='%s'",
                movie_id,
                title,
                lookup_result.get("tmdb_id"),
                lookup_result.get("matched_query"),
                lookup_result.get("matched_tmdb_title"),
            )
            processed += 1

    logger.info(f"=== Completed run; processed {processed}/{len(movies)}")
    return {"status": "success", "processed": processed}
