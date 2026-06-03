#!/usr/bin/env python3
"""
clean_discovered_entities.py — Reset discovered entities and all sentiment scores.

What it does:
  1. Deletes ALL rows from sentiment_scores (full signal reset)
  2. Deletes entities with category = 'Discovered' (NER-promoted entities)
  3. Wipes data/ner_candidates.json (resets promotion-count cache)

Seed entities (Entertainment, Politics, Sports, etc.) are preserved.
Run once after deploying the NER quality filters so junk entities are gone
and the pipeline rediscovers entities cleanly from scratch.

Usage:
    python scripts/clean_discovered_entities.py [--dry-run]
"""
import argparse
import json
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from db import get_connection
from dotenv import load_dotenv

load_dotenv()

NER_COUNTS_PATH = Path(__file__).parent.parent / "data" / "ner_candidates.json"


def main():
    parser = argparse.ArgumentParser(description="Reset discovered entities and all sentiment scores.")
    parser.add_argument("--dry-run", action="store_true", help="Print counts without deleting anything")
    args = parser.parse_args()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sentiment_scores")
            score_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM entities WHERE category = 'Discovered'")
            discovered_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM entities WHERE category != 'Discovered'")
            seed_count = cur.fetchone()[0]

        print(f"  sentiment_scores:       {score_count:,} rows  → will DELETE ALL")
        print(f"  entities (Discovered):  {discovered_count:,} rows  → will DELETE")
        print(f"  entities (seed):        {seed_count:,} rows  → PRESERVED")
        ner_size = len(json.loads(NER_COUNTS_PATH.read_text())) if NER_COUNTS_PATH.exists() else 0
        print(f"  ner_candidates.json:    {ner_size:,} candidates  → will WIPE")

        if args.dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        confirm = input("\nProceed with deletion? [y/N] ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return

        with conn.cursor() as cur:
            cur.execute("DELETE FROM sentiment_scores")
            deleted_scores = cur.rowcount
            cur.execute("DELETE FROM entities WHERE category = 'Discovered'")
            deleted_entities = cur.rowcount
        conn.commit()

        print(f"\nDeleted {deleted_scores:,} sentiment_scores")
        print(f"Deleted {deleted_entities:,} discovered entities")

        if NER_COUNTS_PATH.exists():
            NER_COUNTS_PATH.write_text("{}")
            print(f"Wiped {NER_COUNTS_PATH}")

        print("\nDone. Restart the pipeline to begin fresh scoring.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
