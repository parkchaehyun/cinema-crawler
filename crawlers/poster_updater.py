import os
import re
import logging
import html
from datetime import datetime, timezone
import httpx
from postgrest.exceptions import APIError
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
GENERIC_EN_CLEAR_MARGIN = 12
EDITION_SUFFIX_PATTERN = r"(?:특별판|무삭제판|극장판|감독판|디렉터스\s*컷|director['’]s\s*cut)"
FORMAT_SUFFIX_PATTERN = r"(?:2d|3d|4k|8k|35mm|70mm|16mm|imax|dolby|atmos|자막|더빙|리마스터링|디지털복원|배리어프리(?:\s*버전)?|영문자막|한글자막)"
EVENT_REGEX_FRAGMENTS = (
    r"g\s*[.]?\s*v\s*[.]?",
    r"시네토크",
    r"씨네토크",
    r"인디토크",
    r"무대인사",
    r"영화소개",
    r"관객과의\s*대화",
    r"q\s*&\s*a",
    r"q\s*n\s*a",
    r"qna",
    r"강연",
    r"강의",
    r"대담",
    r"좌담",
    r"스페셜\s*토크",
    r"토크",
    r"포럼",
    r"라이브\s*스크리닝",
    r"시사회",
    r"상영\s*후",
    r"섹션\s*\d+",
)
EVENT_MARKER_PATTERN = r"(?:%s)" % "|".join(EVENT_REGEX_FRAGMENTS)
PLUS_EVENT_SUFFIX_RE = re.compile(
    r"\s*\+\s*%s.*$" % EVENT_MARKER_PATTERN,
    flags=re.IGNORECASE,
)
TRAILING_EVENT_PAREN_RE = re.compile(
    r"\s*\((?=[^)]*%s)[^)]*\)\s*$" % EVENT_MARKER_PATTERN,
    flags=re.IGNORECASE,
)
EDITION_SUFFIX_RE = re.compile(
    r"\s*%s\s*$" % EDITION_SUFFIX_PATTERN,
    flags=re.IGNORECASE,
)
FORMAT_SUFFIX_RE = re.compile(
    r"\s*(?:%s\s*)+$" % FORMAT_SUFFIX_PATTERN,
    flags=re.IGNORECASE,
)
YEAR_PAREN_RE = re.compile(r"\(\s*((?:19|20)\d{2})\s*\)")
ANY_PAREN_RE = re.compile(r"\([^)]*\)")
ANY_BRACKET_RE = re.compile(r"\[[^]]*\]")
MOVIE_FETCH_CHUNK_SIZE = 500
EN_STOPWORDS = {"the", "a", "an", "of", "and", "in", "on", "to", "for", "with", "without", "at", "from"}


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
        try:
            movie_resp = (
                supabase.table("movies")
                .select("id, title, canonical_title, canonical_title_en")
                .in_("id", chunk)
                .is_("tmdb_id", None)
                .execute()
            )
        except Exception as exc:
            # Backward compatibility if migration adding canonical_title_en is not applied yet.
            if "canonical_title_en" not in str(exc):
                raise
            movie_resp = (
                supabase.table("movies")
                .select("id, title, canonical_title")
                .in_("id", chunk)
                .is_("tmdb_id", None)
                .execute()
            )
        movies.extend(movie_resp.data or [])

    return movies


def reconcile_movies_with_tmdb_anchor() -> int:
    """
    Run DB-side reconciliation for duplicate normalized titles with TMDB anchor.
    """
    try:
        resp = supabase.rpc("reconcile_movies_with_tmdb_anchor").execute()
    except Exception as exc:
        logger.warning("Skipping reconciliation RPC (missing or failed): %s", exc)
        return 0
    rows = resp.data or []
    return len(rows)


def merge_movie_rows(keep_movie_id: int, drop_movie_id: int) -> bool:
    """
    Merge duplicate movie row into canonical row via DB RPC.
    """
    if keep_movie_id == drop_movie_id:
        return False
    try:
        supabase.rpc(
            "merge_movie_rows",
            {"keep_movie_id": keep_movie_id, "drop_movie_id": drop_movie_id},
        ).execute()
        return True
    except Exception as exc:
        logger.error(
            "Failed to merge duplicate movie rows keep=%s drop=%s err=%s",
            keep_movie_id,
            drop_movie_id,
            exc,
        )
        return False


def _normalize_for_match(value: str) -> str:
    value = _clean_title_core(value)
    value = re.sub(r"[^0-9a-zA-Z가-힣\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _contains_event_keyword(value: str) -> bool:
    return bool(re.search(EVENT_MARKER_PATTERN, value, flags=re.IGNORECASE))


def _strip_plus_event_suffix(value: str) -> str:
    return PLUS_EVENT_SUFFIX_RE.sub("", value).strip()


def _trim_edition_suffix(value: str) -> str:
    return EDITION_SUFFIX_RE.sub("", value).strip()


def _trim_format_suffix(value: str) -> str:
    return FORMAT_SUFFIX_RE.sub("", value).strip()


def _strip_parentheses_and_brackets(value: str) -> str:
    # Keep year-only tags like "(1980)" as plain "1980", then drop all other (...) and [...].
    cleaned = YEAR_PAREN_RE.sub(r" \1 ", value)
    cleaned = ANY_PAREN_RE.sub(" ", cleaned)
    cleaned = ANY_BRACKET_RE.sub(" ", cleaned)
    return cleaned.strip()


def _clean_title_core(value: str, *, lower: bool = True) -> str:
    cleaned = (value or "").strip()
    if lower:
        cleaned = cleaned.lower()
    cleaned = _strip_plus_event_suffix(cleaned)
    cleaned = TRAILING_EVENT_PAREN_RE.sub("", cleaned).strip()
    cleaned = _strip_parentheses_and_brackets(cleaned)
    cleaned = _trim_edition_suffix(cleaned)
    cleaned = _trim_format_suffix(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


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

    cleaned = _clean_title_core(raw, lower=False)
    add(cleaned)

    # Keep left side only when '+' suffix is event metadata.
    no_event_suffix = raw
    plus_parts = re.split(r"\s*\+\s*", raw, maxsplit=1)
    if len(plus_parts) == 2:
        left_part, right_part = plus_parts[0].strip(), plus_parts[1].strip()
        if _contains_event_keyword(right_part):
            no_event_suffix = left_part
            add(left_part)

    no_event_suffix = TRAILING_EVENT_PAREN_RE.sub("", no_event_suffix).strip()
    no_event_suffix = _strip_parentheses_and_brackets(no_event_suffix)
    no_event_suffix = _trim_edition_suffix(no_event_suffix)
    no_event_suffix = _trim_format_suffix(no_event_suffix)
    add(no_event_suffix)

    return candidates


def _build_seed_titles(movie: dict) -> list[tuple[str, str]]:
    seeds: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(value: str | None, seed_type: str):
        raw = html.unescape((value or "").strip())
        if len(raw) < 2:
            return
        key = raw.casefold()
        if key in seen:
            return
        seen.add(key)
        seeds.append((raw, seed_type))

    add(movie.get("canonical_title_en"), "en")
    add(movie.get("canonical_title"), "ko")
    add(movie.get("title"), "raw")
    return seeds


def _is_generic_english_title(seed: str) -> bool:
    # Very short single-token EN titles are highly collision-prone on TMDB.
    letters_only = re.sub(r"[^A-Za-z ]", "", seed or "").strip().lower()
    tokens = [token for token in re.split(r"\s+", letters_only) if token]
    content_tokens = [token for token in tokens if token not in EN_STOPWORDS]
    content_len = len("".join(content_tokens))

    if not tokens:
        return False
    if len(content_tokens) <= 1 and content_len <= 10:
        return True
    if len(tokens) <= 2 and content_len <= 7:
        return True
    return False


def _preferred_languages_for(seed: str) -> tuple[str, str]:
    has_korean = bool(re.search(r"[가-힣]", seed))
    has_latin = bool(re.search(r"[A-Za-z]", seed))
    if has_latin and not has_korean:
        return ("en-US", "ko-KR")
    if has_korean:
        return ("ko-KR", "en-US")
    return SEARCH_LANGUAGES


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


def _pick_final_candidate(candidates: list[dict]) -> dict | None:
    if not candidates:
        return None

    def score_of(candidate: dict) -> int:
        return int(candidate.get("tmdb_match_score", -10_000))

    best_en = None
    best_ko = None
    best_overall = None
    for candidate in candidates:
        if best_overall is None or score_of(candidate) > score_of(best_overall):
            best_overall = candidate
        seed_type = candidate.get("seed_type")
        if seed_type == "en" and (best_en is None or score_of(candidate) > score_of(best_en)):
            best_en = candidate
        if seed_type == "ko" and (best_ko is None or score_of(candidate) > score_of(best_ko)):
            best_ko = candidate

    # If both EN and KO candidates exist, resolve ambiguity deliberately.
    if best_en and best_ko:
        en_score = score_of(best_en)
        ko_score = score_of(best_ko)
        if _is_generic_english_title(best_en.get("matched_seed_title", "")):
            if en_score >= ko_score + GENERIC_EN_CLEAR_MARGIN:
                best_en["selection_reason"] = "generic_en_clear_margin"
                return best_en
            best_ko["selection_reason"] = "ko_preferred_over_generic_en"
            return best_ko
        if en_score > ko_score:
            best_en["selection_reason"] = "en_higher_score"
            return best_en
        best_ko["selection_reason"] = "ko_tiebreak_or_higher"
        return best_ko

    if best_overall:
        best_overall["selection_reason"] = "best_overall"
    return best_overall


def lookup_poster_for(seed_titles: list[tuple[str, str]], client: httpx.Client) -> dict | None:
    """
    Try multiple normalized query variants and languages.
    Returns TMDB match payload when matched.
    """
    attempted: set[tuple[str, str]] = set()
    candidates: list[dict] = []
    for seed, seed_type in seed_titles:
        for query in _build_title_candidates(seed):
            for language in _preferred_languages_for(seed):
                attempt_key = (query.casefold(), language)
                if attempt_key in attempted:
                    continue
                attempted.add(attempt_key)

                try:
                    results = _search_tmdb(client, query=query, language=language)
                except httpx.HTTPStatusError as exc:
                    logger.error(
                        "TMDB HTTP error for seed='%s' query='%s' lang=%s status=%s",
                        seed,
                        query,
                        language,
                        exc.response.status_code if exc.response else "unknown",
                    )
                    continue
                except Exception as exc:
                    logger.error(
                        "TMDB request failed for seed='%s' query='%s' lang=%s error=%s",
                        seed,
                        query,
                        language,
                        exc,
                    )
                    continue

                best = _find_best_result(results, query=query, original_title=seed)
                if not best:
                    continue

                poster_path = best.get("poster_path")
                if not poster_path:
                    continue

                tmdb_id = best.get("id")
                if not tmdb_id:
                    continue

                score = _score_result(best, query=query, original_title=seed)
                candidates.append(
                    {
                    "tmdb_id": tmdb_id,
                    "poster_url": TMDB_IMAGE_BASE + poster_path,
                    "matched_seed_title": seed,
                    "seed_type": seed_type,
                    "matched_query": query,
                    "matched_tmdb_title": best.get("title") or best.get("original_title") or "",
                    "original_title": best.get("original_title") or None,
                    "release_date": best.get("release_date") or None,
                    "tmdb_language": best.get("original_language") or language,
                    "tmdb_match_score": float(score),
                    }
                )

    return _pick_final_candidate(candidates)


def update_movie_poster(movie_id: int, match: dict) -> bool:
    """
    Update TMDB enrichment fields for a given movie ID.
    """
    tmdb_id = match.get("tmdb_id")
    if tmdb_id is None:
        return False

    # Guard: tmdb_id is unique on movies; skip instead of crashing if another row already owns it.
    existing = (
        supabase.table("movies")
        .select("id")
        .eq("tmdb_id", tmdb_id)
        .limit(1)
        .execute()
    )
    existing_rows = existing.data or []
    if existing_rows and existing_rows[0].get("id") != movie_id:
        existing_movie_id = existing_rows[0].get("id")
        merged = merge_movie_rows(existing_movie_id, movie_id)
        if merged:
            logger.info(
                "Merged duplicate movie id=%s into id=%s via tmdb_id=%s",
                movie_id,
                existing_movie_id,
                tmdb_id,
            )
            return False
        logger.warning(
            "Skipping movie id=%s: tmdb_id=%s already linked to movie id=%s",
            movie_id,
            tmdb_id,
            existing_movie_id,
        )
        return False

    payload = {
        "tmdb_id": tmdb_id,
        "poster_url": match.get("poster_url"),
        "original_title": match.get("original_title"),
        "release_date": match.get("release_date"),
        "tmdb_language": match.get("tmdb_language"),
        "tmdb_match_score": match.get("tmdb_match_score"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    try:
        (
            supabase.table("movies")
            .update(payload)
            .eq("id", movie_id)
            .is_("tmdb_id", None)
            .execute()
        )
        return True
    except APIError as exc:
        if str(exc).find("movies_tmdb_id_uidx") >= 0 or str(exc).find("duplicate key value") >= 0:
            logger.warning(
                "Skipping movie id=%s due tmdb_id conflict (tmdb_id=%s): %s",
                movie_id,
                tmdb_id,
                exc,
            )
            return False
        raise


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

    reconciled = reconcile_movies_with_tmdb_anchor()
    if reconciled:
        logger.info("Reconciliation merged %s duplicate movie row(s) before TMDB run", reconciled)

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
            try:
                seed_titles = _build_seed_titles(movie)
                if not seed_titles:
                    logger.warning("Skipping id=%s because all candidate titles are empty", movie_id)
                    continue

                lookup_result = lookup_poster_for(seed_titles, client)
                if not lookup_result:
                    logger.info(
                        "No poster found for id=%s seeds=%s",
                        movie_id,
                        [seed for seed, _ in seed_titles[:3]],
                    )
                    continue

                update_movie_poster(movie_id, lookup_result)

                logger.info(
                    "Updated movie id=%s seed='%s'(%s) tmdb_id=%s matched_query='%s' tmdb_title='%s' reason=%s score=%.1f",
                    movie_id,
                    lookup_result.get("matched_seed_title"),
                    lookup_result.get("seed_type"),
                    lookup_result.get("tmdb_id"),
                    lookup_result.get("matched_query"),
                    lookup_result.get("matched_tmdb_title"),
                    lookup_result.get("selection_reason"),
                    lookup_result.get("tmdb_match_score", 0.0),
                )
                processed += 1
            except Exception as e:
                logger.error("Unexpected error processing movie id=%s: %s", movie_id, e)

    logger.info(f"=== Completed run; processed {processed}/{len(movies)}")
    return {"status": "success", "processed": processed}
