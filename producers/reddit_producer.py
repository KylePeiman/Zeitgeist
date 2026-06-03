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
from collections import deque
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

# Reddit OAuth (script app) credentials — preferred when set, since the
# public JSON API 403-blocks most datacenter/cloud IPs. Falls back to the
# unauthenticated public JSON endpoints when these are absent.
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT = os.getenv(
    "REDDIT_USER_AGENT", "zeitgeist/1.0 (personal sentiment pipeline)"
)

# Reddit public API headers — required to avoid 429s
HEADERS = {
    "User-Agent": "zeitgeist/1.0 (personal sentiment pipeline; not for commercial use)",
    "Accept": "application/json",
}

# Bounded cross-cycle de-duplication cache: a post/comment is published at
# most once even though hot listings re-surface the same items every poll
# cycle. The cap lets very long-lived items eventually re-enter rather than
# leaking memory forever.
SEEN_CACHE_MAX = int(os.getenv("SEEN_CACHE_MAX", 20000))
_seen_ids: set[str] = set()
_seen_order: deque[str] = deque()


def mark_seen(item_id: str) -> bool:
    """Record an item id. Returns True if newly seen, False if a duplicate."""
    if not item_id:
        return True  # no id to dedupe on — let it through
    if item_id in _seen_ids:
        return False
    _seen_ids.add(item_id)
    _seen_order.append(item_id)
    if len(_seen_order) > SEEN_CACHE_MAX:
        evicted = _seen_order.popleft()
        _seen_ids.discard(evicted)
    return True


# ── REDDIT OAUTH CLIENT (optional) ────────────────────────────
def make_reddit_client():
    """Build a read-only praw client if OAuth credentials are configured.

    Returns None when credentials are missing or praw cannot initialise,
    in which case the producer uses the public JSON fallback paths.
    """
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET):
        return None
    try:
        import praw

        client = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
            check_for_async=False,
        )
        client.read_only = True
        return client
    except Exception as e:
        logger.warning(f"Could not init authenticated Reddit client: {e} — using public JSON")
        return None


# Module-level client, populated in run().
_reddit = None


def _submission_to_child(submission) -> dict:
    """Convert a praw submission into the public-JSON `{'data': {...}}` shape."""
    return {
        "data": {
            "id": submission.id,
            "title": submission.title or "",
            "selftext": getattr(submission, "selftext", "") or "",
            "url": getattr(submission, "url", "") or "",
            "score": int(getattr(submission, "score", 0) or 0),
            "upvote_ratio": float(getattr(submission, "upvote_ratio", 0.0) or 0.0),
            "num_comments": int(getattr(submission, "num_comments", 0) or 0),
            "author": str(submission.author) if submission.author else "[deleted]",
            "created_utc": int(getattr(submission, "created_utc", 0) or 0),
            "subreddit": str(submission.subreddit),
        }
    }


def _comment_to_child(comment) -> dict:
    """Convert a praw comment into the public-JSON `{'data': {...}}` shape."""
    return {
        "data": {
            "id": comment.id,
            "body": getattr(comment, "body", "") or "",
            "score": int(getattr(comment, "score", 0) or 0),
            "author": str(comment.author) if comment.author else "[deleted]",
            "created_utc": int(getattr(comment, "created_utc", 0) or 0),
        }
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
    """Fetch hot posts from a subreddit (authenticated API, else public JSON)."""
    if _reddit is not None:
        try:
            return [_submission_to_child(s) for s in _reddit.subreddit(subreddit).hot(limit=limit)]
        except Exception as e:
            logger.warning(f"Authenticated fetch of r/{subreddit} failed: {e}")
            return []
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
    """Fetch top comments for a post (authenticated API, else public JSON)."""
    if _reddit is not None:
        try:
            submission = _reddit.submission(id=post_id)
            submission.comment_sort = "top"
            submission.comments.replace_more(limit=0)
            return [_comment_to_child(c) for c in submission.comments[:limit]]
        except Exception as e:
            logger.warning(f"Authenticated comment fetch for {post_id} failed: {e}")
            return []
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
    """Search Reddit for a specific entity (authenticated API, else public JSON)."""
    if _reddit is not None:
        try:
            results = _reddit.subreddit("all").search(entity_name, sort="new", limit=limit)
            return [_submission_to_child(s) for s in results]
        except Exception as e:
            logger.warning(f"Authenticated search for '{entity_name}' failed: {e}")
            return []
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
    global _reddit
    producer = make_producer()
    messages_sent = 0
    cycle = 0

    _reddit = make_reddit_client()
    mode = "authenticated API (praw)" if _reddit is not None else "public JSON"
    logger.info(f"Starting Reddit producer → topic: {KAFKA_TOPIC}")
    logger.info(f"Source mode: {mode}")
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
                post_id = post_data.get("id", "")

                # Publish post — skip if already seen in a previous cycle
                if mark_seen(f"post:{post_id}"):
                    msg = build_post_message(post_data, subreddit)
                    if msg:
                        producer.produce(
                            KAFKA_TOPIC,
                            key=post_id,
                            value=json.dumps(msg),
                            callback=delivery_report,
                        )
                        cycle_messages += 1

                # Fetch and publish comments for high-engagement posts
                if post_data.get("num_comments", 0) > 10:
                    comments = fetch_post_comments(subreddit, post_id, limit=15)

                    for comment in comments:
                        comment_data = comment.get("data", {})
                        if not mark_seen(f"comment:{comment_data.get('id', '')}"):
                            continue
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
                    if not mark_seen(f"post:{post_data.get('id', '')}"):
                        continue
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
