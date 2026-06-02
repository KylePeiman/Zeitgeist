"""
historical_backfill.py — Zeitgeist Historical Data Backfill

Fetches historical sentiment data from multiple free sources and writes
aggregated scores directly to SQLite with accurate historical timestamps.

Sources:
  - GDELT Project   (2013+, no auth) — news with built-in tone scores
  - Google Trends   (2004+, no auth) — weekly search interest as engagement proxy
  - Reddit          (paginated search, weeks–months back)
  - YouTube         (year-by-year from 2005+, requires YOUTUBE_API_KEY)
  - NewsAPI         (30-day window, requires NEWS_API_KEY)

Usage:
  python producers/historical_backfill.py
  python producers/historical_backfill.py --sources gdelt,trends
  python producers/historical_backfill.py --sources reddit,youtube --start-year 2010
"""

import argparse
import json
import math
import os
import sys
import time
import urllib.parse
from datetime import datetime, timezone, timedelta

import psycopg2
import requests
from dotenv import load_dotenv
from loguru import logger
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from entities import ENTITIES
from db import DATABASE_URL, get_connection, init_db

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

HEADERS = {
    "User-Agent": "zeitgeist/1.0 (personal sentiment pipeline; not for commercial use)",
    "Accept": "application/json",
}

vader = SentimentIntensityAnalyzer()


# ── DATABASE ──────────────────────────────────────────────────

# Connection + schema (including the unique index used for idempotent re-runs)
# live in db.py — init_db / get_connection are imported above.


def upsert_entity(conn, entity: dict) -> int:
    now = datetime.now(timezone.utc).isoformat()
    name = entity["name"]
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM entities WHERE name = %s", (name,))
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE entities SET last_seen = %s, category = %s, entity_type = %s WHERE id = %s",
                (now, entity["category"], entity["entity_type"], row[0]),
            )
            conn.commit()
            return row[0]
        cur.execute(
            "INSERT INTO entities (name, category, entity_type, first_seen, last_seen) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (name, entity["category"], entity["entity_type"], now, now),
        )
        entity_id = cur.fetchone()[0]
    conn.commit()
    return entity_id


def write_score(
    conn,
    entity_id: int,
    timestamp: str,
    sentiment_score: float,
    source: str,
    reasoning: str,
    mention_count: int = 1,
    engagement_score: float = 0.0,
    confidence: float = 0.5,
    sample_size: int = 0,
):
    score = max(-1.0, min(1.0, float(sentiment_score)))
    if score > 0.2:
        sentiment = "positive"
    elif score < -0.2:
        sentiment = "negative"
    else:
        sentiment = "neutral"
    intensity = "high" if abs(score) > 0.5 else "medium" if abs(score) > 0.2 else "low"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO sentiment_scores
                (entity_id, timestamp, sentiment, confidence, sentiment_score,
                 reasoning, intensity, mention_count, engagement_score, source, sample_size)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity_id, timestamp, source) DO NOTHING
            """,
            (
                entity_id, timestamp, sentiment, round(confidence, 3), round(score, 4),
                reasoning, intensity, mention_count, float(engagement_score), source, sample_size,
            ),
        )
    conn.commit()


# ── GDELT PROJECT ─────────────────────────────────────────────

def fetch_gdelt(entity_name: str, start_year: int) -> list[dict]:
    """
    Fetch GDELT V2 tone timeline for an entity (2013+).
    Returns [{timestamp, score, count}].
    GDELT tone is approximately -30 to +10; we normalize to -1..+1.
    """
    gdelt_start = max(start_year, 2013)
    start_dt = f"{gdelt_start}0101000000"
    end_dt = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    query = urllib.parse.quote(f'"{entity_name}"')

    url = (
        "https://api.gdeltproject.org/api/v2/doc/doc"
        f"?query={query}&mode=tonechart&format=json"
        f"&startdatetime={start_dt}&enddatetime={end_dt}"
    )

    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 429:
                wait = 30 * (2 ** attempt)  # 30, 60, 120, 240s
                logger.warning(f"GDELT rate-limited — waiting {wait}s before retry {attempt + 1}/4")
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            break
        except Exception as e:
            if attempt < 3:
                time.sleep(15)
                continue
            logger.warning(f"GDELT failed for '{entity_name}' after 4 attempts: {e}")
            return []
    else:
        return []

    results = []
    for series in data.get("timeline", []):
        for pt in series.get("data", []):
            date_str = pt.get("date", "")
            tone = pt.get("value", None)
            if not date_str or tone is None:
                continue
            try:
                dt = datetime.strptime(date_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                score = max(-1.0, min(1.0, float(tone) / 10.0))
                count = int(pt.get("count", 1)) if "count" in pt else 1
                results.append({"timestamp": dt.isoformat(), "score": score, "count": count})
            except (ValueError, TypeError):
                continue

    return results


# ── GOOGLE TRENDS ─────────────────────────────────────────────

def fetch_google_trends(entity_names: list[str], start_year: int) -> dict[str, list[dict]]:
    """
    Fetch Google Trends weekly interest (0–100) for up to 5 entities.
    Requires: pip install pytrends
    Returns {entity_name: [{timestamp, interest}]}
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        logger.warning("pytrends not installed — skipping Google Trends. Run: pip install pytrends")
        return {}

    timeframe = f"{start_year}-01-01 {datetime.now().strftime('%Y-%m-%d')}"
    results: dict[str, list[dict]] = {}

    for attempt in range(3):
        try:
            pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 30))
            pytrends.build_payload(entity_names[:5], timeframe=timeframe, geo="")
            df = pytrends.interest_over_time()
            if df.empty:
                return {}

            for name in entity_names[:5]:
                if name not in df.columns:
                    continue
                series = []
                for ts, row in df.iterrows():
                    interest = row[name]
                    if not isinstance(interest, (int, float)) or math.isnan(float(interest)):
                        continue
                    if int(interest) == 0:
                        continue
                    # Normalize timestamp timezone
                    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                        ts_iso = ts.tz_localize("UTC").isoformat()
                    else:
                        ts_iso = ts.isoformat()
                    series.append({"timestamp": ts_iso, "interest": int(interest)})
                results[name] = series
            return results

        except Exception as e:
            if attempt < 2:
                logger.warning(f"Google Trends attempt {attempt + 1} failed, retrying in 30s: {e}")
                time.sleep(30)
            else:
                logger.warning(f"Google Trends failed after 3 attempts for {entity_names}: {e}")
                return {}

    return results


# ── REDDIT ────────────────────────────────────────────────────

def fetch_reddit_history(entity_name: str, max_pages: int = 10) -> list[dict]:
    """Paginate Reddit search (sort=new, t=all) for historical posts."""
    posts = []
    after = None

    for page in range(max_pages):
        params: dict = {
            "q": entity_name,
            "sort": "new",
            "t": "all",
            "limit": 100,
            "type": "link",
        }
        if after:
            params["after"] = after

        url = "https://www.reddit.com/search.json?" + urllib.parse.urlencode(params)
        fetched = False
        for attempt in range(4):
            try:
                r = requests.get(url, headers=HEADERS, timeout=15)
                if r.status_code == 429:
                    wait = 20 * (2 ** attempt)  # 20, 40, 80, 160s
                    logger.warning(f"Reddit rate-limited — waiting {wait}s (attempt {attempt+1}/4)")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                data = r.json()
                fetched = True
                break
            except Exception as e:
                logger.warning(f"Reddit page {page} attempt {attempt+1} failed for '{entity_name}': {e}")
                if attempt < 3:
                    time.sleep(10)
        if not fetched:
            break

        children = data.get("data", {}).get("children", [])
        if not children:
            break

        for child in children:
            post = child.get("data", {})
            title = post.get("title", "")
            body = post.get("selftext", "")
            full_text = f"{title} {body}".strip()
            created_utc = post.get("created_utc", 0)
            if full_text and created_utc:
                posts.append({"text": full_text[:500], "created_utc": int(created_utc)})

        after = data.get("data", {}).get("after")
        if not after:
            break
        time.sleep(1.5)

    return posts


# ── YOUTUBE ───────────────────────────────────────────────────

def fetch_youtube_history(entity_name: str, start_year: int) -> list[dict]:
    """Fetch YouTube videos year by year using publishedAfter/publishedBefore."""
    if not YOUTUBE_API_KEY:
        return []

    posts = []
    current_year = datetime.now().year

    for year in range(max(start_year, 2005), current_year + 1):
        params = {
            "part": "snippet",
            "q": entity_name,
            "type": "video",
            "maxResults": 50,
            "publishedAfter": f"{year}-01-01T00:00:00Z",
            "publishedBefore": f"{year + 1}-01-01T00:00:00Z",
            "order": "relevance",
            "key": YOUTUBE_API_KEY,
        }
        url = "https://www.googleapis.com/youtube/v3/search?" + urllib.parse.urlencode(params)

        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 403:
                quota_err = r.json().get("error", {}).get("errors", [{}])[0].get("reason", "")
                if "quotaExceeded" in quota_err or "dailyLimitExceeded" in quota_err:
                    logger.warning("YouTube quota exceeded — stopping YouTube history fetch")
                    return posts
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.warning(f"YouTube {year} failed for '{entity_name}': {e}")
            time.sleep(2)
            continue

        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            title = snippet.get("title", "")
            description = snippet.get("description", "")
            published_at = snippet.get("publishedAt", "")
            if not published_at:
                continue
            try:
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                posts.append({
                    "text": f"{title} {description[:200]}".strip(),
                    "created_utc": int(dt.timestamp()),
                })
            except (ValueError, TypeError):
                continue

        time.sleep(1.2)

    return posts


# ── NEWSAPI ───────────────────────────────────────────────────

def fetch_newsapi_history(entity_name: str) -> list[dict]:
    """Fetch NewsAPI articles with pagination (free tier: 30 days back)."""
    if not NEWS_API_KEY or NEWS_API_KEY.startswith("your_"):
        return []

    posts = []
    from_date = (datetime.now(timezone.utc) - timedelta(days=29)).strftime("%Y-%m-%d")

    for page in range(1, 6):
        params = {
            "q": entity_name,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": 100,
            "page": page,
            "from": from_date,
            "apiKey": NEWS_API_KEY,
        }
        url = "https://newsapi.org/v2/everything?" + urllib.parse.urlencode(params)
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.warning(f"NewsAPI page {page} failed for '{entity_name}': {e}")
            break

        if data.get("status") != "ok":
            break
        articles = data.get("articles", [])
        if not articles:
            break

        for article in articles:
            title = article.get("title", "") or ""
            description = article.get("description", "") or ""
            published_at = article.get("publishedAt", "")
            if not published_at:
                continue
            try:
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                posts.append({
                    "text": f"{title} {description}".strip(),
                    "created_utc": int(dt.timestamp()),
                })
            except (ValueError, TypeError):
                continue

        if len(articles) < 100:
            break
        time.sleep(1)

    return posts


# ── AGGREGATION ───────────────────────────────────────────────

def aggregate_to_hourly(posts: list[dict], source_label: str) -> list[dict]:
    """Group posts by UTC hour and compute VADER sentiment per bucket."""
    buckets: dict[str, list[str]] = {}

    for post in posts:
        ts = post.get("created_utc", 0)
        text = post.get("text", "")
        if not ts or not text:
            continue
        dt = datetime.fromtimestamp(ts, tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
        key = dt.isoformat()
        buckets.setdefault(key, []).append(text)

    results = []
    for timestamp, texts in sorted(buckets.items()):
        scores = [vader.polarity_scores(t)["compound"] for t in texts]
        avg_score = sum(scores) / len(scores) if scores else 0.0
        mention_count = len(texts)
        confidence = min(0.85, 0.3 + mention_count / 20.0)
        results.append({
            "timestamp": timestamp,
            "score": round(avg_score, 4),
            "mention_count": mention_count,
            "confidence": round(confidence, 3),
            "sample_size": mention_count,
            "reasoning": f"{source_label} historical: {mention_count} mentions, avg VADER={avg_score:+.3f}",
        })

    return results


# ── MAIN ──────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Zeitgeist Historical Backfill")
    parser.add_argument(
        "--sources",
        default="gdelt,trends,reddit,youtube,news",
        help="Comma-separated list of sources: gdelt, trends, reddit, youtube, news (default: all)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2004,
        help="Earliest year to fetch data from (default: 2004). GDELT starts 2013, YouTube 2005.",
    )
    return parser.parse_args()


def run():
    args = parse_args()
    sources = {s.strip().lower() for s in args.sources.split(",")}
    start_year = args.start_year

    logger.info(f"Backfill starting | sources={sorted(sources)} | start_year={start_year}")
    logger.info("DB: Postgres")

    conn = init_db(get_connection())

    entity_ids: dict[str, int] = {}
    for entity in ENTITIES:
        entity_ids[entity["name"]] = upsert_entity(conn, entity)
    logger.info(f"Upserted {len(entity_ids)} entities into DB")

    # ── GDELT ──────────────────────────────────────────────────
    if "gdelt" in sources:
        logger.info("=== GDELT Historical News (2013 → present) ===")
        for entity in ENTITIES:
            name = entity["name"]
            logger.info(f"  GDELT: {name}")
            points = fetch_gdelt(name, start_year)
            written = 0
            for pt in points:
                count = pt.get("count", 1)
                confidence = min(0.9, 0.4 + count / 100.0)
                write_score(
                    conn, entity_ids[name],
                    timestamp=pt["timestamp"],
                    sentiment_score=pt["score"],
                    source="gdelt_historical",
                    reasoning=f"GDELT news tone: {pt['score']:+.3f} from {count} articles",
                    mention_count=count,
                    confidence=confidence,
                )
                written += 1
            logger.info(f"    → {written} monthly data points written")
            time.sleep(7)  # GDELT enforces 1 req/5s; stay safely under

    # ── GOOGLE TRENDS ──────────────────────────────────────────
    if "trends" in sources:
        logger.info("=== Google Trends (2004 → present) ===")
        entity_names = [e["name"] for e in ENTITIES]
        batches = [entity_names[i : i + 5] for i in range(0, len(entity_names), 5)]

        for batch in batches:
            logger.info(f"  Trends batch: {batch}")
            trends_data = fetch_google_trends(batch, start_year)

            for name, series in trends_data.items():
                if name not in entity_ids:
                    continue
                written = 0
                for pt in series:
                    write_score(
                        conn, entity_ids[name],
                        timestamp=pt["timestamp"],
                        sentiment_score=0.0,
                        source="google_trends",
                        reasoning=f"Google Trends: search interest = {pt['interest']}/100",
                        mention_count=1,
                        engagement_score=float(pt["interest"]),
                        confidence=0.5,
                    )
                    written += 1
                logger.info(f"    {name}: {written} weekly points")

            time.sleep(8)  # Google Trends rate limits aggressively

    # ── REDDIT ─────────────────────────────────────────────────
    if "reddit" in sources:
        logger.info("=== Reddit Search History ===")
        for entity in ENTITIES:
            name = entity["name"]
            logger.info(f"  Reddit: {name}")
            posts = fetch_reddit_history(name, max_pages=10)
            if not posts:
                logger.info("    → no posts found")
                continue
            buckets = aggregate_to_hourly(posts, "reddit")
            for b in buckets:
                write_score(
                    conn, entity_ids[name],
                    timestamp=b["timestamp"],
                    sentiment_score=b["score"],
                    source="reddit_historical",
                    reasoning=b["reasoning"],
                    mention_count=b["mention_count"],
                    confidence=b["confidence"],
                    sample_size=b["sample_size"],
                )
            logger.info(f"    → {len(posts)} posts → {len(buckets)} hourly buckets")

    # ── YOUTUBE ────────────────────────────────────────────────
    if "youtube" in sources:
        if not YOUTUBE_API_KEY:
            logger.warning("YOUTUBE_API_KEY not set — skipping YouTube history")
        else:
            logger.info(f"=== YouTube History ({max(start_year, 2005)} → present) ===")
            for entity in ENTITIES:
                name = entity["name"]
                logger.info(f"  YouTube: {name}")
                posts = fetch_youtube_history(name, start_year)
                if not posts:
                    continue
                buckets = aggregate_to_hourly(posts, "youtube")
                for b in buckets:
                    write_score(
                        conn, entity_ids[name],
                        timestamp=b["timestamp"],
                        sentiment_score=b["score"],
                        source="youtube_historical",
                        reasoning=b["reasoning"],
                        mention_count=b["mention_count"],
                        confidence=b["confidence"],
                        sample_size=b["sample_size"],
                    )
                logger.info(f"    → {len(posts)} videos → {len(buckets)} hourly buckets")

    # ── NEWSAPI ────────────────────────────────────────────────
    if "news" in sources:
        if not NEWS_API_KEY or NEWS_API_KEY.startswith("your_"):
            logger.warning("NEWS_API_KEY not configured — skipping NewsAPI history")
        else:
            logger.info("=== NewsAPI History (30-day window) ===")
            for entity in ENTITIES:
                name = entity["name"]
                logger.info(f"  NewsAPI: {name}")
                posts = fetch_newsapi_history(name)
                if not posts:
                    continue
                buckets = aggregate_to_hourly(posts, "news")
                for b in buckets:
                    write_score(
                        conn, entity_ids[name],
                        timestamp=b["timestamp"],
                        sentiment_score=b["score"],
                        source="newsapi_historical",
                        reasoning=b["reasoning"],
                        mention_count=b["mention_count"],
                        confidence=b["confidence"],
                        sample_size=b["sample_size"],
                    )
                logger.info(f"    → {len(posts)} articles → {len(buckets)} hourly buckets")
                time.sleep(1)

    conn.close()
    logger.info("=== Backfill complete ===")
    logger.info(
        "Run: SELECT source, MIN(timestamp), MAX(timestamp), COUNT(*) "
        "FROM sentiment_scores GROUP BY source;"
    )


if __name__ == "__main__":
    run()
