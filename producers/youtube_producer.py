"""
youtube_producer.py — Zeitgeist YouTube Producer

Uses YouTube Data API v3 to search for entities, fetch video metadata
and comments, then publishes to the raw.youtube Kafka topic.
Matches the patterns and structure of reddit_producer.py.
"""

import json
import time
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer
from loguru import logger
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Add parent dir to path for entities import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from entities import ALIAS_MAP, ENTITIES

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "raw.youtube"
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 60))
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")


# ── KAFKA PRODUCER ────────────────────────────────────────────
def make_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "zeitgeist-youtube-producer",
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


# ── YOUTUBE API ───────────────────────────────────────────────
def make_youtube_client(api_key: str):
    """Build a YouTube Data API v3 client."""
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


def fetch_entity_videos(entity_name: str, youtube) -> list[dict]:
    """Search YouTube for videos matching an entity name."""
    try:
        search_response = youtube.search().list(
            q=entity_name,
            type="video",
            part="id,snippet",
            maxResults=10,
            order="relevance",
            relevanceLanguage="en",
        ).execute()
        return search_response.get("items", [])
    except HttpError as e:
        logger.warning(f"YouTube search failed for '{entity_name}': {e}")
        return []


def fetch_video_stats(video_ids: list[str], youtube) -> dict[str, dict]:
    """Fetch statistics for a list of video IDs. Returns {video_id: stats}."""
    if not video_ids:
        return {}
    try:
        stats_response = youtube.videos().list(
            part="statistics",
            id=",".join(video_ids),
        ).execute()
        result = {}
        for item in stats_response.get("items", []):
            result[item["id"]] = item.get("statistics", {})
        return result
    except HttpError as e:
        logger.warning(f"Failed to fetch video stats: {e}")
        return {}


def fetch_video_comments(video_id: str, youtube) -> list[dict]:
    """Fetch top-level comments for a video."""
    try:
        comments_response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=20,
            textFormat="plainText",
            order="relevance",
        ).execute()
        return comments_response.get("items", [])
    except HttpError as e:
        # Comments disabled is common — log as debug not warning
        logger.debug(f"Cannot fetch comments for {video_id}: {e}")
        return []


# ── MESSAGE BUILDERS ──────────────────────────────────────────
def build_video_message(search_item: dict, stats: dict, target_entity: str) -> dict | None:
    """Build a Kafka message from a YouTube video search result."""
    snippet = search_item.get("snippet", {})
    video_id = search_item.get("id", {}).get("videoId", "")

    if not video_id:
        return None

    title = snippet.get("title", "")
    description = snippet.get("description", "")
    full_text = f"{title} {description}".strip()

    entities = extract_entities(full_text)
    # Always include the entity we searched for
    if target_entity not in entities:
        entities.append(target_entity)

    if not entities:
        return None

    return {
        "source": "youtube",
        "content_type": "video",
        "video_id": video_id,
        "channel_id": snippet.get("channelId", ""),
        "channel_title": snippet.get("channelTitle", ""),
        "title": title,
        "text": description[:500] if description else "",
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "view_count": int(stats.get("viewCount", 0)),
        "like_count": int(stats.get("likeCount", 0)),
        "comment_count": int(stats.get("commentCount", 0)),
        "published_at": snippet.get("publishedAt", ""),
        "entities": entities,
        "entity_metadata": {e: get_entity_metadata(e) for e in entities},
        "author": snippet.get("channelTitle", ""),
        "created_utc": int(
            datetime.fromisoformat(
                snippet.get("publishedAt", "1970-01-01T00:00:00Z").replace("Z", "+00:00")
            ).timestamp()
        ) if snippet.get("publishedAt") else 0,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def build_comment_message(comment_item: dict, video_id: str, video_entities: list[str]) -> dict | None:
    """Build a Kafka message from a YouTube comment."""
    top_comment = comment_item.get("snippet", {}).get("topLevelComment", {})
    snippet = top_comment.get("snippet", {})

    text = snippet.get("textDisplay", "")
    if not text:
        return None

    comment_id = top_comment.get("id", "")
    entities = extract_entities(text)

    # Inherit entities from parent video if comment has none
    if not entities:
        entities = list(video_entities)

    if not entities:
        return None

    published_at = snippet.get("publishedAt", "")
    created_utc = 0
    if published_at:
        try:
            created_utc = int(
                datetime.fromisoformat(published_at.replace("Z", "+00:00")).timestamp()
            )
        except ValueError:
            pass

    return {
        "source": "youtube",
        "content_type": "video_comment",
        "video_id": video_id,
        "comment_id": comment_id,
        "text": text[:500],
        "like_count": int(snippet.get("likeCount", 0)),
        "entities": entities,
        "entity_metadata": {e: get_entity_metadata(e) for e in entities},
        "author": snippet.get("authorDisplayName", ""),
        "created_utc": created_utc,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ── MAIN LOOP ─────────────────────────────────────────────────
def run():
    if not YOUTUBE_API_KEY:
        logger.error("YOUTUBE_API_KEY is not set — cannot start YouTube producer")
        logger.warning("Set YOUTUBE_API_KEY in your .env file and restart")
        return

    producer = make_producer()
    youtube = make_youtube_client(YOUTUBE_API_KEY)
    messages_sent = 0
    cycle = 0

    logger.info(f"Starting YouTube producer → topic: {KAFKA_TOPIC}")
    logger.info(f"Tracking {len(ENTITIES)} entities | Poll interval: {POLL_INTERVAL_SECONDS}s")

    while True:
        cycle += 1
        cycle_messages = 0
        logger.info(f"── Cycle {cycle} starting ──")

        for entity in ENTITIES:
            entity_name = entity["name"]
            logger.debug(f"Searching YouTube for: {entity_name}")

            # Fetch videos for this entity
            search_items = fetch_entity_videos(entity_name, youtube)
            time.sleep(1)  # Rate limit between searches

            if not search_items:
                continue

            # Batch-fetch stats for all video IDs
            video_ids = [
                item.get("id", {}).get("videoId", "")
                for item in search_items
                if item.get("id", {}).get("videoId")
            ]
            stats_map = fetch_video_stats(video_ids, youtube)
            time.sleep(1)

            for item in search_items:
                video_id = item.get("id", {}).get("videoId", "")
                if not video_id:
                    continue

                stats = stats_map.get(video_id, {})
                msg = build_video_message(item, stats, entity_name)
                if msg:
                    producer.produce(
                        KAFKA_TOPIC,
                        key=video_id,
                        value=json.dumps(msg),
                        callback=delivery_report,
                    )
                    cycle_messages += 1

                # Fetch comments for videos with comments enabled
                if int(stats.get("commentCount", 0)) > 0:
                    comments = fetch_video_comments(video_id, youtube)
                    time.sleep(1)

                    video_entities = msg["entities"] if msg else [entity_name]
                    for comment_item in comments:
                        cmsg = build_comment_message(comment_item, video_id, video_entities)
                        if cmsg:
                            producer.produce(
                                KAFKA_TOPIC,
                                key=cmsg["comment_id"],
                                value=json.dumps(cmsg),
                                callback=delivery_report,
                            )
                            cycle_messages += 1

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
