"""
sentiment_scorer.py — Zeitgeist LLM Sentiment Scorer

Polls the processed.signals Kafka topic continuously.
For each signal, builds a prompt and calls the llama.cpp server
at LLAMA_SERVER_URL (default: http://localhost:8080).
Writes LLM sentiment results to SQLite at SQLITE_DB_PATH.
"""

import json
import os
import re
import sys
import sqlite3
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from loguru import logger
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
LLAMA_SERVER_URL = os.getenv("LLAMA_SERVER_URL", "http://localhost:8080")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./data/zeitgeist.db")
INPUT_TOPIC = "processed.signals"

LLAMA_MAX_TOKENS = 500
LLAMA_TEMPERATURE = 0.3


# ── DATABASE ──────────────────────────────────────────────────
def init_db(db_path: str) -> sqlite3.Connection:
    """Create SQLite DB and tables if they don't exist."""
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entities (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    UNIQUE NOT NULL,
            category    TEXT,
            entity_type TEXT,
            first_seen  TEXT    NOT NULL,
            last_seen   TEXT    NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_scores (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id       INTEGER NOT NULL REFERENCES entities(id),
            timestamp       TEXT    NOT NULL,
            sentiment       TEXT    NOT NULL,
            confidence      REAL    NOT NULL,
            sentiment_score REAL    NOT NULL,
            reasoning       TEXT,
            intensity       TEXT,
            mention_count   INTEGER,
            engagement_score REAL,
            source          TEXT,
            sample_size     INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ss_entity_ts ON sentiment_scores(entity_id, timestamp)")
    conn.commit()
    return conn


def upsert_entity(conn: sqlite3.Connection, signal: dict) -> int:
    """Insert or update entity record. Returns entity id."""
    name = signal["entity"]
    entity_metadata = {}

    # Try to get metadata from a sample message's entity_metadata field
    # The signal has entity_type directly at top level
    entity_type = signal.get("entity_type", "unknown")

    now = datetime.now(timezone.utc).isoformat()

    cursor = conn.execute("SELECT id FROM entities WHERE name = ?", (name,))
    row = cursor.fetchone()

    if row:
        conn.execute(
            "UPDATE entities SET last_seen = ?, entity_type = ? WHERE id = ?",
            (now, entity_type, row[0])
        )
        conn.commit()
        return row[0]
    else:
        cursor = conn.execute(
            "INSERT INTO entities (name, category, entity_type, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)",
            (name, signal.get("category", "unknown"), entity_type, now, now)
        )
        conn.commit()
        return cursor.lastrowid


def write_sentiment_score(conn: sqlite3.Connection, entity_id: int, signal: dict, llm_result: dict):
    """Insert a sentiment score row."""
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO sentiment_scores
            (entity_id, timestamp, sentiment, confidence, sentiment_score,
             reasoning, intensity, mention_count, engagement_score, source, sample_size)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        entity_id,
        now,
        llm_result.get("sentiment", "neutral"),
        float(llm_result.get("confidence", 0.5)),
        float(llm_result.get("sentiment_score", 0.0)),
        llm_result.get("reasoning", ""),
        llm_result.get("intensity", "medium"),
        signal.get("mention_count", 0),
        float(signal.get("engagement_score", 0)),
        signal.get("source", "unknown"),
        len(signal.get("sample_texts", [])),
    ))
    conn.commit()


# ── PROMPT BUILDER ────────────────────────────────────────────
def build_llm_prompt(signal: dict) -> str:
    """Build a structured prompt for the llama.cpp model."""
    entity = signal["entity"]
    mention_count = signal.get("mention_count", 0)
    engagement_score = signal.get("engagement_score", 0)
    keywords = signal.get("raw_sentiment_keywords", [])
    positive = signal.get("positive_signal_count", 0)
    negative = signal.get("negative_signal_count", 0)
    neutral = signal.get("neutral_signal_count", 0)
    samples = signal.get("sample_texts", [])[:3]
    source = signal.get("source", "unknown")

    sample_block = ""
    for i, text in enumerate(samples, 1):
        sample_block += f"\n  {i}. {text[:200]}"

    keywords_str = ", ".join(keywords[:10]) if keywords else "none detected"

    prompt = f"""You are a sentiment analysis expert. Analyze the public sentiment toward "{entity}" based on social media and news data.

Data summary:
- Source: {source}
- Mentions in last 5 minutes: {mention_count}
- Engagement score: {engagement_score}
- Signal breakdown: {positive} positive, {negative} negative, {neutral} neutral
- Key sentiment words: {keywords_str}
- Sample texts:{sample_block}

Respond ONLY with a JSON object (no markdown, no extra text):
{{
  "sentiment": "positive" | "negative" | "neutral",
  "confidence": <float 0.0-1.0>,
  "sentiment_score": <float -1.0 to 1.0, negative=very negative, positive=very positive>,
  "reasoning": "<one sentence explanation>",
  "intensity": "low" | "medium" | "high"
}}"""

    return prompt


# ── LLM CALL ─────────────────────────────────────────────────
def call_llm(signal: dict, llama_url: str) -> dict | None:
    """POST to llama.cpp /completion endpoint and parse JSON response."""
    prompt = build_llm_prompt(signal)
    payload = {
        "prompt": prompt,
        "max_tokens": LLAMA_MAX_TOKENS,
        "temperature": LLAMA_TEMPERATURE,
        "stop": ["\n\n", "```"],
    }
    try:
        response = requests.post(
            f"{llama_url}/completion",
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        content = data.get("content", "").strip()

        # Extract JSON from content (handle cases where model adds surrounding text)
        json_match = re.search(r'\{[^{}]+\}', content, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            # Validate required fields
            if "sentiment" in result and "sentiment_score" in result:
                return result

        logger.warning(f"LLM returned unparseable content for '{signal['entity']}': {content[:200]}")
        return None

    except requests.exceptions.ConnectionError:
        logger.warning(f"Cannot connect to llama.cpp at {llama_url} — skipping LLM call")
        return None
    except requests.exceptions.Timeout:
        logger.warning(f"LLM request timed out for '{signal['entity']}'")
        return None
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Failed to parse LLM response for '{signal['entity']}': {e}")
        return None
    except Exception as e:
        logger.warning(f"LLM call failed for '{signal['entity']}': {e}")
        return None


def make_fallback_score(signal: dict) -> dict:
    """
    VADER-based fallback when LLM is unavailable.
    Uses the positive/negative/neutral signal counts from the Flink pipeline.
    """
    pos = signal.get("positive_signal_count", 0)
    neg = signal.get("negative_signal_count", 0)
    total = pos + neg + signal.get("neutral_signal_count", 1)

    if total == 0:
        score = 0.0
    else:
        score = round((pos - neg) / total, 3)

    if score > 0.2:
        sentiment = "positive"
    elif score < -0.2:
        sentiment = "negative"
    else:
        sentiment = "neutral"

    confidence = min(0.8, (pos + neg) / max(total, 1))

    return {
        "sentiment": sentiment,
        "confidence": round(confidence, 3),
        "sentiment_score": score,
        "reasoning": f"VADER fallback: {pos} positive, {neg} negative signals from {total} mentions",
        "intensity": "high" if abs(score) > 0.5 else "medium" if abs(score) > 0.2 else "low",
    }


# ── KAFKA CONSUMER ────────────────────────────────────────────
def make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "zeitgeist-llm-scorer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })


# ── MAIN LOOP ─────────────────────────────────────────────────
def run():
    logger.info(f"Starting LLM Sentiment Scorer")
    logger.info(f"Topic: {INPUT_TOPIC} | LLM: {LLAMA_SERVER_URL} | DB: {SQLITE_DB_PATH}")

    conn = init_db(SQLITE_DB_PATH)
    consumer = make_consumer()
    consumer.subscribe([INPUT_TOPIC])

    # Check LLM availability
    try:
        r = requests.get(f"{LLAMA_SERVER_URL}/health", timeout=5)
        llm_available = r.status_code == 200
        logger.info("llama.cpp server is reachable" if llm_available else "llama.cpp server returned non-200")
    except Exception:
        llm_available = False
        logger.warning(f"llama.cpp server not reachable at {LLAMA_SERVER_URL} — using VADER fallback scoring")

    messages_processed = 0
    scores_written = 0

    logger.info("Consuming from processed.signals...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.warning(f"Kafka error: {msg.error()}")
                continue

            try:
                signal = json.loads(msg.value().decode("utf-8"))
                entity = signal.get("entity", "")
                if not entity:
                    continue

                messages_processed += 1

                # Get LLM score or fall back to VADER
                if llm_available:
                    llm_result = call_llm(signal, LLAMA_SERVER_URL)
                    if llm_result is None:
                        llm_result = make_fallback_score(signal)
                        llm_available = False  # Back off from LLM after failure
                else:
                    llm_result = make_fallback_score(signal)

                # Write to DB
                entity_id = upsert_entity(conn, signal)
                write_sentiment_score(conn, entity_id, signal, llm_result)
                scores_written += 1

                logger.info(
                    f"[{entity}] {llm_result['sentiment']} "
                    f"(score={llm_result['sentiment_score']:+.2f}, "
                    f"conf={llm_result['confidence']:.2f}) | "
                    f"mentions={signal.get('mention_count', 0)} | "
                    f"total_written={scores_written}"
                )

            except (json.JSONDecodeError, KeyError) as e:
                logger.debug(f"Skipping malformed signal: {e}")

    except KeyboardInterrupt:
        logger.info("Scorer stopped by user")
    finally:
        consumer.close()
        conn.close()
        logger.info(f"Final: processed={messages_processed} | scores_written={scores_written}")


if __name__ == "__main__":
    run()
