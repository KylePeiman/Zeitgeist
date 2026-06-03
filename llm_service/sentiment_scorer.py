"""
sentiment_scorer.py — Zeitgeist LLM Sentiment Scorer

Polls the processed.signals Kafka topic continuously.
For each signal, builds a prompt and calls the llama.cpp server
at LLAMA_SERVER_URL (default: http://localhost:8080).
Writes LLM sentiment results to Postgres (Neon) — see db.py / DATABASE_URL.
"""

import json
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from loguru import logger
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import DATABASE_URL, get_connection, init_db  # noqa: E402

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
LLAMA_SERVER_URL = os.getenv("LLAMA_SERVER_URL", "http://localhost:8080")
INPUT_TOPIC = "processed.signals"

LLAMA_MAX_TOKENS = 500
LLAMA_TEMPERATURE = 0.3
SCORER_WORKERS = int(os.getenv("SCORER_WORKERS", 4))
LLM_CONCURRENCY = int(os.getenv("LLM_CONCURRENCY", 4))  # match llama-server n_parallel

_llm_semaphore = threading.Semaphore(LLM_CONCURRENCY)


# ── DATABASE ──────────────────────────────────────────────────
# Connection + schema live in db.py (init_db / get_connection, imported above).
def upsert_entity(conn, signal: dict) -> int:
    """Insert or update entity record. Returns entity id."""
    name = signal["entity"]
    entity_type = signal.get("entity_type", "unknown")
    category = signal.get("category", "unknown")

    now = datetime.now(timezone.utc).isoformat()

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM entities WHERE name = %s", (name,))
        row = cur.fetchone()

        if row:
            cur.execute(
                "UPDATE entities SET last_seen = %s, entity_type = %s, "
                "category = CASE WHEN category = 'unknown' THEN %s ELSE category END "
                "WHERE id = %s",
                (now, entity_type, category, row[0]),
            )
            conn.commit()
            return row[0]

        cur.execute(
            "INSERT INTO entities (name, category, entity_type, first_seen, last_seen) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (name, category, entity_type, now, now),
        )
        entity_id = cur.fetchone()[0]
    conn.commit()
    return entity_id


def write_sentiment_score(conn, entity_id: int, signal: dict, llm_result: dict):
    """Insert a sentiment score row."""
    # Key the row on the window's event time, not the write time. Using
    # datetime.now() here made the (entity_id, timestamp, source) unique index
    # — and the ON CONFLICT DO NOTHING below — a no-op, because every write got
    # a fresh microsecond timestamp. Taking the time from the signal means a
    # reprocessed signal (consumer redelivery, scorer restart, replayed offset)
    # carries the same timestamp and collides instead of duplicating the point.
    ts = (
        signal.get("window_end")
        or signal.get("computed_at")
        or datetime.now(timezone.utc).isoformat()
    )
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
                entity_id,
                ts,
                llm_result.get("sentiment", "neutral"),
                float(llm_result.get("confidence", 0.5)),
                float(llm_result.get("sentiment_score", 0.0)),
                llm_result.get("reasoning", ""),
                llm_result.get("intensity", "medium"),
                signal.get("mention_count", 0),
                float(signal.get("engagement_score", 0)),
                signal.get("source", "unknown"),
                len(signal.get("sample_texts", [])),
            ),
        )
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
        with _llm_semaphore:
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
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })


# ── WORKER FUNCTION ───────────────────────────────────────────
def process_signal(raw_value: bytes, conn, db_lock: threading.Lock) -> str:
    """Parse, score, and persist one signal. Runs in a worker thread."""
    signal = json.loads(raw_value.decode("utf-8"))
    entity = signal.get("entity", "")
    if not entity:
        return "skipped: no entity"

    llm_result = call_llm(signal, LLAMA_SERVER_URL) or make_fallback_score(signal)
    method = "VADER" if llm_result.get("reasoning", "").startswith("VADER fallback") else "LLM"

    with db_lock:
        entity_id = upsert_entity(conn, signal)
        write_sentiment_score(conn, entity_id, signal, llm_result)

    return (
        f"[{entity}] {llm_result['sentiment']} "
        f"(score={llm_result['sentiment_score']:+.2f}, conf={llm_result['confidence']:.2f}) "
        f"via {method} | mentions={signal.get('mention_count', 0)}"
    )


# ── MAIN LOOP ─────────────────────────────────────────────────
def run():
    logger.info(f"Starting LLM Sentiment Scorer | workers={SCORER_WORKERS}")
    logger.info(f"Topic: {INPUT_TOPIC} | LLM: {LLAMA_SERVER_URL} | DB: Postgres")

    conn = init_db(get_connection())
    db_lock = threading.Lock()
    consumer = make_consumer()
    consumer.subscribe([INPUT_TOPIC])

    # Check LLM availability at startup (informational only — call_llm falls back to VADER per-call)
    try:
        r = requests.get(f"{LLAMA_SERVER_URL}/health", timeout=5)
        if r.status_code == 200:
            logger.info("llama.cpp server is reachable — LLM scoring enabled")
        else:
            logger.warning("llama.cpp server returned non-200 — will use VADER fallback")
    except Exception:
        logger.warning(f"llama.cpp not reachable at {LLAMA_SERVER_URL} — using VADER fallback scoring")

    submitted = 0
    completed = 0
    futures: set = set()

    logger.info(f"Consuming from {INPUT_TOPIC} with {SCORER_WORKERS} worker threads...")

    try:
        with ThreadPoolExecutor(max_workers=SCORER_WORKERS) as executor:
            while True:
                # Reap completed futures and surface any errors
                done = {f for f in futures if f.done()}
                for f in done:
                    try:
                        logger.info(f.result())
                    except Exception as e:
                        logger.error(f"Signal processing failed: {e}")
                    completed += 1
                futures -= done

                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.warning(f"Kafka error: {msg.error()}")
                    continue

                futures.add(executor.submit(process_signal, msg.value(), conn, db_lock))
                submitted += 1

                if submitted % 100 == 0:
                    logger.info(f"Progress: submitted={submitted} | completed={completed} | in-flight={len(futures)}")

    except KeyboardInterrupt:
        logger.info("Scorer stopped by user")
    finally:
        consumer.close()
        conn.close()
        logger.info(f"Final: submitted={submitted} | completed={completed}")


if __name__ == "__main__":
    run()
