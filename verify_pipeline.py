"""
verify_pipeline.py — Zeitgeist End-to-End Pipeline Verification

Checks all pipeline stages and prints a summary:
  1. Kafka topics — message counts via Kafdrop REST API
  2. Flink cluster — connectivity and job slots
  3. SQLite DB — entity and score counts

Run this to confirm the full pipeline is working.
"""

import os
import sqlite3
import sys
import requests
from dotenv import load_dotenv
from loguru import logger
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

load_dotenv()

KAFDROP_URL = "http://localhost:9000"
FLINK_URL = "http://localhost:8081"
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./data/zeitgeist.db")
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


def check_sqlite(db_path: str) -> dict:
    """Check SQLite database for data."""
    if not os.path.exists(db_path):
        return {"ok": False, "error": "DB file does not exist"}
    try:
        conn = sqlite3.connect(db_path)
        entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        score_count = conn.execute("SELECT COUNT(*) FROM sentiment_scores").fetchone()[0]
        latest = conn.execute(
            "SELECT timestamp FROM sentiment_scores ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        return {
            "ok": score_count > 0,
            "entities": entity_count,
            "scores": score_count,
            "latest_ts": latest[0] if latest else "none",
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


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

    # ── SQLite ────────────────────────────────────────────────
    print("\n[DB]  SQLITE DATABASE")
    db = check_sqlite(SQLITE_DB_PATH)
    if db["ok"]:
        print(f"  {OK} {db['entities']} entities | {db['scores']} sentiment scores")
        print(f"       Latest: {db['latest_ts']}")
    else:
        print(f"  {FAIL} DB error: {db.get('error', '?')}")
        if db.get("scores", 0) == 0:
            print("       Run sentiment_scorer.py to populate the DB")
        all_ok = False

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
