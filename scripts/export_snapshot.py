"""
export_snapshot.py — Zeitgeist Static Snapshot Exporter

Reads from Postgres (Neon) and writes docs/data.json for GitHub Pages.

Usage:
  python scripts/export_snapshot.py
  python scripts/export_snapshot.py --days 14
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta

import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_connection

load_dotenv()

OUT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs", "data.json")


def export(days: int = 30) -> None:
    try:
        conn = get_connection()
    except Exception as e:
        print(f"ERROR: cannot connect to database: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        df = pd.read_sql_query(
            """
            SELECT e.name, e.category, e.entity_type,
                   s.sentiment, s.sentiment_score, s.confidence,
                   s.mention_count, s.engagement_score, s.source,
                   s.timestamp, s.reasoning
            FROM sentiment_scores s
            JOIN entities e ON s.entity_id = e.id
            ORDER BY s.timestamp
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        print("No data in DB — nothing to export.")
        sys.exit(0)

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")

    # Latest snapshot per entity (all time)
    latest = df.sort_values("timestamp").groupby("name").last().reset_index()
    _MENTION_PRIOR = 10
    latest["adjusted_score"] = (
        latest["sentiment_score"] * latest["mention_count"]
        / (latest["mention_count"] + _MENTION_PRIOR)
    ).round(4)
    latest["sentiment_score"] = latest["sentiment_score"].round(4)
    latest["confidence"] = latest["confidence"].round(3)
    latest["timestamp"] = latest["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Daily-aggregated history for last N days
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
    hist = df[df["timestamp"] >= cutoff].copy()
    hist["day"] = hist["timestamp"].dt.floor("D").dt.strftime("%Y-%m-%d")
    daily = (
        hist.groupby(["name", "day"])
        .agg(
            sentiment_score=("sentiment_score", "mean"),
            mention_count=("mention_count", "sum"),
            confidence=("confidence", "mean"),
        )
        .reset_index()
    )
    daily["sentiment_score"] = daily["sentiment_score"].round(4)
    daily["confidence"] = daily["confidence"].round(3)

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "history_days": days,
        "entity_count": int(len(latest)),
        "latest": latest[
            ["name", "category", "entity_type", "sentiment", "sentiment_score",
             "adjusted_score", "confidence", "mention_count", "source",
             "timestamp", "reasoning"]
        ].to_dict(orient="records"),
        "history": daily[["name", "day", "sentiment_score", "mention_count", "confidence"]].to_dict(
            orient="records"
        ),
    }

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))

    size_kb = os.path.getsize(OUT_PATH) / 1024
    print(f"Exported {len(payload['latest'])} entities, {len(payload['history'])} daily history rows")
    print(f"Written to {OUT_PATH} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30, help="Days of history to include (default: 30)")
    args = parser.parse_args()
    export(args.days)
