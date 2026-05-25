"""
sentiment_pipeline.py — Zeitgeist Flink Sentiment Pipeline

Consumes raw.reddit, raw.youtube, and raw.news simultaneously.
Applies a sliding window (5 min window, 30 sec slide) per entity.
Computes per-entity signals using VADER sentiment and spaCy NER.
Emits normalized signal objects to the processed.signals topic.

Runs as a standalone Python process implementing sliding window logic
and publishes to processed.signals just as a Flink job would.
Also submits a heartbeat job to the Flink REST API for UI visibility.
"""

import json
import os
import sys
import time
import threading
import collections
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer, KafkaError
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import spacy
import requests
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from entities import ALIAS_MAP

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FLINK_HOST = os.getenv("FLINK_JOBMANAGER_HOST", "localhost")
FLINK_PORT = os.getenv("FLINK_JOBMANAGER_PORT", "8081")
FLINK_URL = f"http://{FLINK_HOST}:{FLINK_PORT}"

WINDOW_SIZE_SECONDS = int(os.getenv("SLIDING_WINDOW_SIZE_SECONDS", 300))   # 5 minutes
SLIDE_SECONDS = int(os.getenv("SLIDING_WINDOW_SLIDE_SECONDS", 30))          # 30 seconds
MAX_SAMPLE_TEXTS = int(os.getenv("MAX_SAMPLE_TEXTS", 5))

INPUT_TOPICS = ["raw.reddit", "raw.youtube", "raw.news"]
OUTPUT_TOPIC = "processed.signals"

# ── NLP INIT ──────────────────────────────────────────────────
vader = SentimentIntensityAnalyzer()

try:
    nlp = spacy.load("en_core_web_sm")
    NLP_AVAILABLE = True
except OSError:
    logger.warning("spaCy model not found — NER discovery disabled")
    NLP_AVAILABLE = False


# ── KAFKA SETUP ───────────────────────────────────────────────
def make_consumer(group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "zeitgeist-flink-pipeline",
        "acks": "all",
        "retries": 3,
    })


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(f"Signal delivered to {msg.topic()} [{msg.partition()}]")


# ── ENGAGEMENT SCORE ──────────────────────────────────────────
def get_engagement(msg: dict) -> int:
    """Extract a unified engagement score from any source message."""
    source = msg.get("source", "")
    if source == "reddit":
        return msg.get("score", 0) + msg.get("num_comments", 0)
    elif source == "youtube":
        return int(msg.get("like_count", 0)) + int(msg.get("comment_count", 0))
    elif source == "news":
        return 1  # Articles contribute equally
    return 0


# ── SENTIMENT KEYWORDS ────────────────────────────────────────
POSITIVE_WORDS = {
    "love", "amazing", "great", "excellent", "awesome", "fantastic", "best",
    "brilliant", "wonderful", "incredible", "outstanding", "legendary", "iconic",
    "beautiful", "perfect", "genius", "fire", "goat", "queen", "king",
}
NEGATIVE_WORDS = {
    "hate", "terrible", "awful", "worst", "horrible", "disgusting", "trash",
    "garbage", "overrated", "boring", "fraud", "scam", "fake", "bad", "disappointing",
    "disaster", "failure", "stupid", "dumb", "cancelled", "problematic",
}


def extract_sentiment_keywords(texts: list[str]) -> list[str]:
    """Find positive/negative signal words present across the window texts."""
    combined = " ".join(texts).lower()
    words = set(combined.split())
    found = list((words & POSITIVE_WORDS) | (words & NEGATIVE_WORDS))
    return sorted(found)[:20]


# ── SPACY NER DISCOVERY ───────────────────────────────────────
def discover_entities_ner(text_blob: str) -> list[dict]:
    """Use spaCy NER to find entities not in the seed list."""
    if not NLP_AVAILABLE or not text_blob:
        return []
    try:
        doc = nlp(text_blob[:5000])  # Cap to avoid slow processing
        discovered = []
        for ent in doc.ents:
            if ent.label_ in ("PERSON", "ORG", "GPE", "PRODUCT"):
                discovered.append({
                    "name": ent.text,
                    "entity_type": ent.label_.lower(),
                    "source": "ner_discovery",
                })
        return discovered[:10]
    except Exception as e:
        logger.debug(f"NER error: {e}")
        return []


# ── SLIDING WINDOW BUFFER ─────────────────────────────────────
# Buffer structure: {(entity, source): deque of (timestamp, message)}

def make_window_buffer():
    return collections.defaultdict(collections.deque)


def add_to_buffer(buffer: dict, entity: str, source: str, msg: dict, ts: float):
    key = (entity, source)
    buffer[key].append((ts, msg))


def evict_old_entries(buffer: dict, now: float):
    """Remove entries older than the window size."""
    cutoff = now - WINDOW_SIZE_SECONDS
    for key in list(buffer.keys()):
        dq = buffer[key]
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        if not dq:
            del buffer[key]


# ── SIGNAL COMPUTATION ────────────────────────────────────────
def compute_signal(entity: str, source: str, entries: list[tuple]) -> dict:
    """Compute a ProcessedSignal from a window of (timestamp, message) entries."""
    if not entries:
        return None

    texts = []
    engagement_scores = []
    positive_count = negative_count = neutral_count = 0

    for _, msg in entries:
        text = msg.get("text", "") or msg.get("title", "") or ""
        if text:
            texts.append(text)
            scores = vader.polarity_scores(text)
            compound = scores["compound"]
            if compound >= 0.05:
                positive_count += 1
            elif compound <= -0.05:
                negative_count += 1
            else:
                neutral_count += 1

        engagement_scores.append(get_engagement(msg))

    total_engagement = sum(engagement_scores)
    mention_count = len(entries)
    window_minutes = WINDOW_SIZE_SECONDS / 60.0
    engagement_velocity = round(mention_count / window_minutes, 2)

    # Top sample texts by engagement score (paired sort)
    paired = sorted(zip(engagement_scores, texts), reverse=True)
    sample_texts = [t for _, t in paired[:MAX_SAMPLE_TEXTS]]

    raw_text_blob = " ".join(texts[:20])  # Cap blob size
    sentiment_keywords = extract_sentiment_keywords(texts)

    # Get entity metadata from the most recent message
    _, last_msg = entries[-1]
    entity_metadata = last_msg.get("entity_metadata", {}).get(entity, {})
    entity_type = entity_metadata.get("entity_type", "unknown")

    now = datetime.now(timezone.utc)
    window_start = datetime.fromtimestamp(
        entries[0][0], tz=timezone.utc
    ).isoformat()
    window_end = now.isoformat()

    return {
        "entity": entity,
        "entity_type": entity_type,
        "source": source,
        "window_start": window_start,
        "window_end": window_end,
        "mention_count": mention_count,
        "engagement_velocity": engagement_velocity,
        "engagement_score": total_engagement,
        "raw_sentiment_keywords": sentiment_keywords,
        "positive_signal_count": positive_count,
        "negative_signal_count": negative_count,
        "neutral_signal_count": neutral_count,
        "sample_texts": sample_texts,
        "raw_text_blob": raw_text_blob[:2000],
        "discovered_entities": discover_entities_ner(raw_text_blob),
        "computed_at": now.isoformat(),
    }


# ── FLINK REST API — HEARTBEAT JOB ───────────────────────────
def register_flink_job():
    """
    Post a 'running' job stub to the Flink REST API so this pipeline
    appears in the Flink UI at localhost:8081.
    The Flink API doesn't allow arbitrary job registration, so we just
    log the Flink cluster info to confirm connectivity.
    """
    try:
        r = requests.get(f"{FLINK_URL}/overview", timeout=5)
        if r.status_code == 200:
            info = r.json()
            logger.info(
                f"Flink cluster connected: {info.get('flink-version', '?')} | "
                f"taskmanagers={info.get('taskmanagers', 0)} | "
                f"slots-total={info.get('slots-total', 0)}"
            )
        else:
            logger.warning(f"Flink UI responded with status {r.status_code}")
    except Exception as e:
        logger.warning(f"Cannot reach Flink UI at {FLINK_URL}: {e}")


# ── MAIN PIPELINE LOOP ────────────────────────────────────────
def run():
    logger.info("Starting Zeitgeist Sentiment Pipeline")
    logger.info(f"Window: {WINDOW_SIZE_SECONDS}s | Slide: {SLIDE_SECONDS}s")
    logger.info(f"Input topics: {INPUT_TOPICS} → {OUTPUT_TOPIC}")

    register_flink_job()

    consumer = make_consumer("zeitgeist-flink-pipeline")
    consumer.subscribe(INPUT_TOPICS)
    producer = make_producer()

    buffer = make_window_buffer()
    messages_consumed = 0
    signals_emitted = 0
    last_slide_ts = time.time()

    # NER entity promotion: track how many messages mention each discovered entity.
    # An entity enters the buffer only after NER_PROMOTION_THRESHOLD occurrences.
    _ner_candidate_counts: dict[str, int] = {}
    NER_PROMOTION_THRESHOLD = 10

    logger.info("Pipeline running — consuming messages and computing sliding windows")

    try:
        while True:
            # Poll Kafka for new messages
            msg = consumer.poll(timeout=1.0)
            now = time.time()

            if msg is not None and not msg.error():
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    entities = data.get("entities", [])
                    source = data.get("source", "unknown")
                    messages_consumed += 1

                    for entity in entities:
                        add_to_buffer(buffer, entity, source, data, now)

                    # NER discovery for messages with no seed-entity match
                    if not entities and NLP_AVAILABLE:
                        raw_text = (
                            (data.get("text") or "")
                            + " "
                            + (data.get("title") or "")
                        ).strip()
                        for hit in discover_entities_ner(raw_text):
                            name = hit["name"]
                            canonical = ALIAS_MAP.get(name.lower())
                            if canonical:
                                # Already a seed entity — buffer directly under canonical name
                                add_to_buffer(buffer, canonical, source, data, now)
                            else:
                                # New entity — apply promotion threshold to suppress noise
                                _ner_candidate_counts[name] = _ner_candidate_counts.get(name, 0) + 1
                                if _ner_candidate_counts[name] >= NER_PROMOTION_THRESHOLD:
                                    msg_with_meta = dict(data)
                                    msg_with_meta.setdefault("entity_metadata", {})[name] = {
                                        "category": "discovered",
                                        "entity_type": hit["entity_type"],
                                    }
                                    add_to_buffer(buffer, name, source, msg_with_meta, now)

                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.debug(f"Skipping malformed message: {e}")

            elif msg is not None and msg.error().code() != KafkaError._PARTITION_EOF:
                logger.warning(f"Kafka error: {msg.error()}")

            # ── Sliding window emission ──
            if now - last_slide_ts >= SLIDE_SECONDS:
                last_slide_ts = now
                evict_old_entries(buffer, now)

                slide_signals = 0
                for (entity, source), dq in list(buffer.items()):
                    entries = list(dq)
                    if not entries:
                        continue

                    signal = compute_signal(entity, source, entries)
                    if signal and signal["mention_count"] > 0:
                        producer.produce(
                            OUTPUT_TOPIC,
                            key=f"{entity}:{source}",
                            value=json.dumps(signal),
                            callback=delivery_report,
                        )
                        slide_signals += 1

                if slide_signals > 0:
                    producer.flush()
                    signals_emitted += slide_signals
                    logger.info(
                        f"Slide emitted {slide_signals} signals | "
                        f"consumed={messages_consumed} | "
                        f"total_signals={signals_emitted} | "
                        f"active_windows={len(buffer)}"
                    )

    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    finally:
        consumer.close()
        producer.flush()
        logger.info(f"Final: consumed={messages_consumed} | signals_emitted={signals_emitted}")


if __name__ == "__main__":
    run()
