"""
reddit_producer.py — Zeitgeist Reddit Producer

Uses Reddit's public JSON API (no credentials required).
Scrapes hot posts and comments from high-signal subreddits,
matches content against the entity seed list, and publishes
raw messages to the raw.reddit Kafka topic.
"""

import json
import time
import os
import sys
import requests
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
KAFKA_TOPIC = "raw.reddit"
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 60))

# Reddit public API headers — required to avoid 429s
HEADERS = {
    "User-Agent": "zeitgeist/1.0 (personal sentiment pipeline; not for commercial use)",
    "Accept": "application/json",
}

# Subreddits to monitor — ordered by signal richness
SUBREDDITS = [
    # News & general
    "news", "worldnews", "todayilearned", "nottheonion",
    # Entertainment
    "entertainment", "popculturechat", "celebrity", "movies", "television",
    # Music
    "music", "popheads", "hiphopheads",
    # Sports
    "nba", "soccer", "nfl", "tennis", "mma",
    # Tech
    "technology", "artificial", "MachineLearning", "programming",
    # Gaming
    "gaming", "pcgaming", "Games",
    # Business & finance
    "wallstreetbets", "investing", "stocks",
]

# ── KAFKA PRODUCER ────────────────────────────────────────────
def make_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "zeitgeist-reddit-producer",
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
    """
    Match text against the entity alias map.
    Returns a list of canonical entity names found in the text.
    """
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


# ── REDDIT API ────────────────────────────────────────────────
def fetch_subreddit_posts(subreddit: str, limit: int = 25) -> list[dict]:
    """Fetch hot posts from a subreddit via public JSON API."""
    url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit={limit}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("children", [])
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch r/{subreddit}: {e}")
        return []


def fetch_post_comments(subreddit: str, post_id: str, limit: int = 20) -> list[dict]:
    """Fetch top comments for a post via public JSON API."""
    url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json?limit={limit}&sort=top"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        if len(data) < 2:
            return []
        return data[1].get("data", {}).get("children", [])
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch comments for {post_id}: {e}")
        return []


def fetch_entity_search(entity_name: str, limit: int = 10) -> list[dict]:
    """Search Reddit for a specific entity across all subreddits."""
    url = f"https://www.reddit.com/search.json?q={requests.utils.quote(entity_name)}&sort=new&limit={limit}&type=link"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("data", {}).get("children", [])
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to search for {entity_name}: {e}")
        return []


# ── MESSAGE BUILDERS ──────────────────────────────────────────
def build_post_message(post_data: dict, subreddit: str) -> dict | None:
    """Build a Kafka message from a Reddit post."""
    title = post_data.get("title", "")
    selftext = post_data.get("selftext", "")
    full_text = f"{title} {selftext}".strip()

    entities = extract_entities(full_text)
    # Publish even with no seed-entity match — Flink NER handles discovery

    return {
        "source": "reddit",
        "content_type": "post",
        "subreddit": subreddit,
        "post_id": post_data.get("id"),
        "title": title,
        "text": selftext[:500] if selftext else "",
        "url": post_data.get("url", ""),
        "score": post_data.get("score", 0),
        "upvote_ratio": post_data.get("upvote_ratio", 0.0),
        "num_comments": post_data.get("num_comments", 0),
        "entities": entities,
        "entity_metadata": {e: get_entity_metadata(e) for e in entities},
        "author": post_data.get("author", "[deleted]"),
        "created_utc": post_data.get("created_utc", 0),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def build_comment_message(comment_data: dict, subreddit: str, post_id: str) -> dict | None:
    """Build a Kafka message from a Reddit comment."""
    body = comment_data.get("body", "")
    if not body or body in ("[deleted]", "[removed]"):
        return None

    entities = extract_entities(body)
    # Publish even with no seed-entity match — Flink NER handles discovery

    return {
        "source": "reddit",
        "content_type": "comment",
        "subreddit": subreddit,
        "post_id": post_id,
        "comment_id": comment_data.get("id"),
        "text": body[:500],
        "score": comment_data.get("score", 0),
        "entities": entities,
        "entity_metadata": {e: get_entity_metadata(e) for e in entities},
        "author": comment_data.get("author", "[deleted]"),
        "created_utc": comment_data.get("created_utc", 0),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ── MAIN LOOP ─────────────────────────────────────────────────
def run():
    producer = make_producer()
    messages_sent = 0
    cycle = 0

    logger.info(f"Starting Reddit producer → topic: {KAFKA_TOPIC}")
    logger.info(f"Monitoring {len(SUBREDDITS)} subreddits | Poll interval: {POLL_INTERVAL_SECONDS}s")

    while True:
        cycle += 1
        cycle_messages = 0
        logger.info(f"── Cycle {cycle} starting ──")

        # ── PHASE 1: Scan subreddits ──────────────────────────
        for subreddit in SUBREDDITS:
            posts = fetch_subreddit_posts(subreddit, limit=25)
            logger.debug(f"r/{subreddit}: {len(posts)} posts fetched")

            for post in posts:
                post_data = post.get("data", {})

                # Publish post
                msg = build_post_message(post_data, subreddit)
                if msg:
                    producer.produce(
                        KAFKA_TOPIC,
                        key=post_data.get("id", ""),
                        value=json.dumps(msg),
                        callback=delivery_report,
                    )
                    cycle_messages += 1

                # Fetch and publish comments for high-engagement posts
                if post_data.get("num_comments", 0) > 10:
                    post_id = post_data.get("id", "")
                    comments = fetch_post_comments(subreddit, post_id, limit=15)

                    for comment in comments:
                        comment_data = comment.get("data", {})
                        cmsg = build_comment_message(comment_data, subreddit, post_id)
                        if cmsg:
                            producer.produce(
                                KAFKA_TOPIC,
                                key=comment_data.get("id", ""),
                                value=json.dumps(cmsg),
                                callback=delivery_report,
                            )
                            cycle_messages += 1

                # Respect rate limits — 1 req/sec max
                time.sleep(1)

            producer.flush()

        # ── PHASE 2: Targeted entity searches (every 5 cycles) ─
        if cycle % 5 == 0:
            logger.info("Running targeted entity searches...")
            for entity in ENTITIES:
                results = fetch_entity_search(entity["name"], limit=5)
                for post in results:
                    post_data = post.get("data", {})
                    msg = build_post_message(post_data, post_data.get("subreddit", "unknown"))
                    if msg:
                        # Ensure the target entity is always included
                        if entity["name"] not in msg["entities"]:
                            msg["entities"].append(entity["name"])
                            msg["entity_metadata"][entity["name"]] = get_entity_metadata(entity["name"])
                        producer.produce(
                            KAFKA_TOPIC,
                            key=post_data.get("id", ""),
                            value=json.dumps(msg),
                            callback=delivery_report,
                        )
                        cycle_messages += 1
                time.sleep(1)

            producer.flush()

        messages_sent += cycle_messages
        logger.info(f"Cycle {cycle} complete — {cycle_messages} messages sent (total: {messages_sent})")
        logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s until next cycle...")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
