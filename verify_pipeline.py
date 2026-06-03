"""
verify_pipeline.py — Zeitgeist End-to-End Pipeline Verification

Checks all pipeline stages and prints a summary:
  1. Kafka topics — message counts via Kafdrop REST API
  2. Flink cluster — connectivity and job slots
  3. SQLite DB — entity and score counts

Run this to confirm the full pipeline is working.
"""

import os
import sys
import requests
from dotenv import load_dotenv
from loguru import logger
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db import get_connection

load_dotenv()

KAFDROP_URL = "http://localhost:9000"
FLINK_URL = "http://localhost:8081"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPICS = ["raw.reddit", "raw.youtube", "raw.news", "processed.signals"]

OK = "[OK]  "
FAIL = "[FAIL]"
WARN = "[WARN]"


def check_kafka_topic(topic: str) -> dict:
    """Get high-water-mark offset totals for a topic via confluent-kafka."""
    try:
        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "zeitgeist-verifier",
        })
        metadata = consumer.list_topics(topic=topic, timeout=5)
        if topic not in metadata.topics:
            consumer.close()
            return {"ok": False, "messages": 0, "error": "topic not found"}

        partitions = [
            TopicPartition(topic, p)
            for p in metadata.topics[topic].partitions
        ]
        total = 0
        for tp in partitions:
            lo, hi = consumer.get_watermark_offsets(tp, timeout=5)
            total += max(0, hi - lo)

        consumer.close()
        return {"ok": True, "messages": total, "partitions": len(partitions)}
    except Exception as e:
        return {"ok": False, "messages": 0, "error": str(e)}


def check_flink() -> dict:
    """Check Flink cluster health."""
    try:
        r = requests.get(f"{FLINK_URL}/overview", timeout=5)
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}"}
        info = r.json()

        # Also check running jobs
        jobs_r = requests.get(f"{FLINK_URL}/jobs", timeout=5)
        running_jobs = 0
        if jobs_r.status_code == 200:
            jobs_data = jobs_r.json()
            running_jobs = len([j for j in jobs_data.get("jobs", []) if j.get("status") == "RUNNING"])

        return {
            "ok": True,
            "version": info.get("flink-version", "?"),
            "taskmanagers": info.get("taskmanagers", 0),
            "slots_total": info.get("slots-total", 0),
            "slots_available": info.get("slots-available", 0),
            "running_jobs": running_jobs,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_db() -> dict:
    """Check the Postgres (Neon) database for data."""
    try:
        conn = get_connection()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM entities")
            entity_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM sentiment_scores")
            score_count = cur.fetchone()[0]
            cur.execute("SELECT timestamp FROM sentiment_scores ORDER BY id DESC LIMIT 1")
            latest = cur.fetchone()
        return {
            "ok": score_count > 0,
            "entities": entity_count,
            "scores": score_count,
            "latest_ts": latest[0] if latest else "none",
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


def check_ner() -> dict:
    """Check whether the spaCy NER model is available.

    Without it, NLP_AVAILABLE is False in the pipeline and NO new entities are
    ever discovered — the entity store stays capped at the seed list.
    """
    try:
        import spacy
    except ImportError:
        return {"ok": False, "error": "spacy not installed"}
    try:
        spacy.load("en_core_web_sm")
        return {"ok": True}
    except OSError:
        return {"ok": False, "error": "model 'en_core_web_sm' not installed"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def check_latency(limit: int = 2000) -> dict:
    """Summarize pipeline latency over the most recent scored rows.

    Two measurements:
      - end_to_end:  newest source ingest in the window → DB write (latency_seconds)
      - scoring:     window emit (timestamp) → DB write (scored_at − timestamp)
    """
    try:
        conn = get_connection()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(latency_seconds),
                    AVG(latency_seconds),
                    PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY latency_seconds),
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_seconds),
                    MAX(latency_seconds),
                    AVG(EXTRACT(EPOCH FROM (scored_at::timestamptz - timestamp::timestamptz))),
                    PERCENTILE_CONT(0.95) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (scored_at::timestamptz - timestamp::timestamptz))
                    )
                FROM (
                    SELECT latency_seconds, scored_at, timestamp
                    FROM sentiment_scores
                    WHERE scored_at IS NOT NULL
                    ORDER BY id DESC
                    LIMIT %s
                ) recent
                """,
                (limit,),
            )
            r = cur.fetchone()
        if not r or r[0] in (None, 0):
            return {"ok": False, "instrumented": False}
        return {
            "ok": True,
            "instrumented": True,
            "samples": r[0],
            "e2e_avg": r[1],
            "e2e_p50": r[2],
            "e2e_p95": r[3],
            "e2e_max": r[4],
            "score_avg": r[5],
            "score_p95": r[6],
        }
    except Exception as e:
        # Most likely the latency columns don't exist yet (scorer hasn't run
        # with instrumentation). Treat as "not instrumented" rather than fatal.
        return {"ok": False, "instrumented": False, "error": str(e)}
    finally:
        conn.close()


def check_dashboard() -> dict:
    """Check if Streamlit dashboard is running."""
    try:
        r = requests.get("http://localhost:8501", timeout=5)
        return {"ok": r.status_code == 200, "status": r.status_code}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    print("\n" + "=" * 60)
    print("  ZEITGEIST PIPELINE VERIFICATION")
    print("=" * 60)

    all_ok = True

    # ── Kafka Topics ──────────────────────────────────────────
    print("\n[KAFKA] KAFKA TOPICS (via Kafdrop at localhost:9000)")
    print(f"  {'Topic':<25} {'Messages':>10}  {'Status'}")
    print(f"  {'-'*55}")

    for topic in TOPICS:
        result = check_kafka_topic(topic)
        if result["ok"]:
            msgs = result["messages"]
            icon = OK if msgs > 0 else WARN
            status = f"{msgs:,} messages" if msgs > 0 else "topic exists, no messages yet"
            if msgs == 0:
                all_ok = False
        else:
            icon = FAIL
            status = f"ERROR: {result.get('error', '?')}"
            all_ok = False
        print(f"  {icon} {topic:<25} {status}")

    # ── Flink ─────────────────────────────────────────────────
    print("\n[FLINK] FLINK CLUSTER (localhost:8081)")
    flink = check_flink()
    if flink["ok"]:
        print(f"  {OK} Flink {flink['version']} | "
              f"{flink['taskmanagers']} taskmanager(s) | "
              f"{flink['slots_available']}/{flink['slots_total']} slots free")
        if flink["running_jobs"] > 0:
            print(f"  {OK} {flink['running_jobs']} job(s) currently RUNNING in Flink UI")
        else:
            print(f"  {WARN} No Flink jobs currently RUNNING")
            print("       (sentiment_pipeline.py runs as standalone process — see instructions)")
    else:
        print(f"  {FAIL} Cannot reach Flink: {flink.get('error', '?')}")
        all_ok = False

    # ── NER model (entity discovery) ──────────────────────────
    print("\n[NER] ENTITY DISCOVERY (spaCy model)")
    ner = check_ner()
    if ner["ok"]:
        print(f"  {OK} en_core_web_sm loaded — NER discovery ENABLED (new entities can be found)")
    else:
        print(f"  {FAIL} {ner.get('error', '?')} — NER discovery DISABLED")
        print("       Entity count is capped at the seed list until this is fixed.")
        print("       Fix: pip install -r requirements.txt  (or: python -m spacy download en_core_web_sm)")
        all_ok = False

    # ── Postgres ──────────────────────────────────────────────
    print("\n[DB]  POSTGRES (NEON) DATABASE")
    db = check_db()
    if db["ok"]:
        print(f"  {OK} {db['entities']} entities | {db['scores']} sentiment scores")
        print(f"       Latest: {db['latest_ts']}")
    else:
        print(f"  {FAIL} DB error: {db.get('error', '?')}")
        if db.get("scores", 0) == 0:
            print("       Run sentiment_scorer.py to populate the DB")
        all_ok = False

    # ── Latency ───────────────────────────────────────────────
    print("\n[LAT] PIPELINE LATENCY (recent scored rows)")
    lat = check_latency()
    if lat["ok"]:
        def _fmt(s):
            return f"{s:.1f}s" if s is not None else "n/a"
        print(f"  {OK} samples={lat['samples']:,}")
        print(f"       end-to-end (ingest→score): "
              f"p50 {_fmt(lat['e2e_p50'])} | p95 {_fmt(lat['e2e_p95'])} | "
              f"avg {_fmt(lat['e2e_avg'])} | max {_fmt(lat['e2e_max'])}")
        print(f"       scoring stage (emit→write): "
              f"p95 {_fmt(lat['score_p95'])} | avg {_fmt(lat['score_avg'])}")
    elif not lat.get("instrumented", True):
        print(f"  {WARN} No latency data yet "
              "(run the scorer with instrumentation to populate scored_at/latency_seconds)")
    else:
        print(f"  {FAIL} Latency query error: {lat.get('error', '?')}")

    # ── Dashboard ─────────────────────────────────────────────
    print("\n[DASH] STREAMLIT DASHBOARD (localhost:8501)")
    dash = check_dashboard()
    if dash["ok"]:
        print(f"  {OK} Dashboard is UP at http://localhost:8501")
    else:
        print(f"  {WARN} Dashboard not running: {dash.get('error', '?')}")
        print("       Start with: streamlit run dashboard/app.py")

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    if all_ok and db.get("scores", 0) > 0:
        print("  [OK] PIPELINE IS FULLY OPERATIONAL")
    else:
        print("  [WARN]  SOME COMPONENTS NEED ATTENTION (see above)")

    print("\n  How to run all components:")
    print("    python producers/reddit_producer.py")
    print("    python producers/youtube_producer.py")
    print("    python producers/news_producer.py")
    print("    python flink/sentiment_pipeline.py")
    print("    python llm_service/sentiment_scorer.py")
    print("    streamlit run dashboard/app.py")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
