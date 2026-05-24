"""
news_producer.py — Zeitgeist News Producer

Two sources:
  1. NewsAPI (newsapi-python) — searches headlines and articles per entity.
     Requires NEWS_API_KEY in .env. Skipped gracefully if missing/invalid.
  2. Google News RSS — no API key required, always runs as supplement/fallback.

Publishes article messages to the raw.news Kafka topic.
Matches the patterns and structure of reddit_producer.py.
"""

import json
import re
import time
import os
import sys
import urllib.parse
import requests
import feedparser
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer
from loguru import logger

# Add parent dir to path for entities import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from entities import ALIAS_MAP, ENTITIES

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "raw.news"
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 60))
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

NEWSAPI_BASE = "https://newsapi.org/v2/everything"
GNEWS_RSS_BASE = "https://news.google.com/rss/search"

HEADERS = {
    "User-Agent": "zeitgeist/1.0 (personal sentiment pipeline; not for commercial use)",
    "Accept": "application/json",
}


# ── KAFKA PRODUCER ────────────────────────────────────────────
def make_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "zeitgeist-news-producer",
        "acks": "all",
        "retries": 3,
    })


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


# ── ENTITY MATCHING ───────────────────────────────────────────
def extract_entities(text: str) -> list[str]:
    """Match text against the entity alias map. Returns canonical entity names."""
    if not text:
        return []

    text_lower = text.lower()
    found = set()

    for alias, canonical in ALIAS_MAP.items():
        # Word boundary check — avoid partial matches
        if f" {alias} " in f" {text_lower} ":
            found.add(canonical)

    return list(found)


def get_entity_metadata(entity_name: str) -> dict:
    """Get category and type for a canonical entity name."""
    for e in ENTITIES:
        if e["name"] == entity_name:
            return {"category": e["category"], "entity_type": e["entity_type"]}
    return {"category": "unknown", "entity_type": "unknown"}


# ── NEWSAPI ───────────────────────────────────────────────────
def is_newsapi_key_valid(api_key: str) -> bool:
    """Quick check that the API key isn't the placeholder."""
    return bool(api_key) and api_key not in ("your_news_api_key", "YOUR_NEWS_API_KEY", "")


def fetch_newsapi_articles(entity_name: str, api_key: str) -> list[dict]:
    """Fetch recent articles mentioning an entity via NewsAPI."""
    params = {
        "q": entity_name,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 10,
        "apiKey": api_key,
    }
    try:
        response = requests.get(NEWSAPI_BASE, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get("status") != "ok":
            logger.warning(f"NewsAPI error for '{entity_name}': {data.get('message', 'unknown')}")
            return []
        return data.get("articles", [])
    except requests.exceptions.RequestException as e:
        logger.warning(f"NewsAPI request failed for '{entity_name}': {e}")
        return []


# ── GOOGLE NEWS RSS ───────────────────────────────────────────
def fetch_google_news_rss(entity_name: str) -> list[dict]:
    """Fetch articles from Google News RSS feed (no API key required)."""
    query = urllib.parse.quote(entity_name)
    url = f"{GNEWS_RSS_BASE}?q={query}&hl=en-US&gl=US&ceid=US:en"
    try:
        # feedparser doesn't support timeout natively — fetch raw then parse
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        return feed.entries if feed.entries else []
    except Exception as e:
        logger.warning(f"Google News RSS failed for '{entity_name}': {e}")
        return []


# ── MESSAGE BUILDERS ──────────────────────────────────────────
def build_newsapi_message(article: dict, target_entity: str) -> dict | None:
    """Build a Kafka message from a NewsAPI article."""
    title = article.get("title", "") or ""
    description = article.get("description", "") or ""
    content = article.get("content", "") or ""
    full_text = f"{title} {description} {content}".strip()

    entities = extract_entities(full_text)
    if target_entity not in entities:
        entities.append(target_entity)

    if not entities:
        return None

    source = article.get("source", {})
    url = article.get("url", "")
    published_at = article.get("publishedAt", "")

    created_utc = 0
    if published_at:
        try:
            created_utc = int(
                datetime.fromisoformat(published_at.replace("Z", "+00:00")).timestamp()
            )
        except ValueError:
            pass

    return {
        "source": "news",
        "content_type": "article",
        "article_source": "newsapi",
        "source_name": source.get("name", "unknown"),
        "article_url": url,
        "title": title,
        "text": description[:500] if description else "",
        "author": article.get("author", "") or "",
        "published_at": published_at,
        "entities": entities,
        "entity_metadata": {e: get_entity_metadata(e) for e in entities},
        "created_utc": created_utc,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def build_rss_message(entry, target_entity: str) -> dict | None:
    """Build a Kafka message from a Google News RSS feed entry."""
    title = getattr(entry, "title", "") or ""
    summary = getattr(entry, "summary", "") or ""
    # Strip HTML tags from RSS summary
    summary_clean = re.sub(r"<[^>]+>", " ", summary)
    summary_clean = re.sub(r"\s+", " ", summary_clean).strip()
    full_text = f"{title} {summary_clean}".strip()

    entities = extract_entities(full_text)
    if target_entity not in entities:
        entities.append(target_entity)

    if not entities:
        return None

    url = getattr(entry, "link", "") or ""
    # Google News RSS entry IDs are unique per article
    article_id = getattr(entry, "id", url) or url

    published_parsed = getattr(entry, "published_parsed", None)
    created_utc = 0
    published_at = ""
    if published_parsed:
        try:
            dt = datetime(*published_parsed[:6], tzinfo=timezone.utc)
            created_utc = int(dt.timestamp())
            published_at = dt.isoformat()
        except Exception:
            pass

    # Source name is embedded in the title: "Article title - Source Name"
    source_name = "google_news"
    if " - " in title:
        parts = title.rsplit(" - ", 1)
        source_name = parts[-1].strip()
        title = parts[0].strip()

    return {
        "source": "news",
        "content_type": "article",
        "article_source": "google_news_rss",
        "source_name": source_name,
        "article_url": url,
        "article_id": article_id,
        "title": title,
        "text": summary_clean[:500],
        "author": "",
        "published_at": published_at,
        "entities": entities,
        "entity_metadata": {e: get_entity_metadata(e) for e in entities},
        "created_utc": created_utc,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ── MAIN LOOP ─────────────────────────────────────────────────
def run():
    producer = make_producer()
    messages_sent = 0
    cycle = 0

    use_newsapi = is_newsapi_key_valid(NEWS_API_KEY)
    if use_newsapi:
        logger.info("NewsAPI key found — will use NewsAPI + Google News RSS")
    else:
        logger.warning("NEWS_API_KEY not set or is placeholder — falling back to Google News RSS only")

    logger.info(f"Starting News producer → topic: {KAFKA_TOPIC}")
    logger.info(f"Tracking {len(ENTITIES)} entities | Poll interval: {POLL_INTERVAL_SECONDS}s")

    # Track seen URLs to avoid duplicates within a cycle
    seen_urls: set[str] = set()

    while True:
        cycle += 1
        cycle_messages = 0
        seen_urls.clear()
        logger.info(f"── Cycle {cycle} starting ──")

        # ── PHASE 1: NewsAPI ──────────────────────────────────
        if use_newsapi:
            logger.info("Phase 1: Fetching from NewsAPI...")
            for entity in ENTITIES:
                entity_name = entity["name"]
                logger.debug(f"NewsAPI search: {entity_name}")

                articles = fetch_newsapi_articles(entity_name, NEWS_API_KEY)
                for article in articles:
                    url = article.get("url", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    msg = build_newsapi_message(article, entity_name)
                    if msg:
                        producer.produce(
                            KAFKA_TOPIC,
                            key=url or entity_name,
                            value=json.dumps(msg),
                            callback=delivery_report,
                        )
                        cycle_messages += 1

                producer.flush()
                time.sleep(1)  # Rate limit: 1 req/sec

        # ── PHASE 2: Google News RSS (always runs) ────────────
        logger.info("Phase 2: Fetching from Google News RSS...")
        for entity in ENTITIES:
            entity_name = entity["name"]
            logger.debug(f"Google News RSS: {entity_name}")

            entries = fetch_google_news_rss(entity_name)
            for entry in entries:
                url = getattr(entry, "link", "") or getattr(entry, "id", "")
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                msg = build_rss_message(entry, entity_name)
                if msg:
                    producer.produce(
                        KAFKA_TOPIC,
                        key=url or entity_name,
                        value=json.dumps(msg),
                        callback=delivery_report,
                    )
                    cycle_messages += 1

            producer.flush()
            time.sleep(1)  # Rate limit: 1 req/sec

        messages_sent += cycle_messages
        logger.info(f"Cycle {cycle} complete — {cycle_messages} messages sent (total: {messages_sent})")
        logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s until next cycle...")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
