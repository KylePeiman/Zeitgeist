"""
db.py — Zeitgeist Postgres (Neon) access layer.

Centralizes the connection and schema for every component that persists
sentiment data (the LLM scorer, the historical backfill, the dashboard,
the snapshot exporter and the pipeline verifier).

Connection details come from the DATABASE_URL environment variable, which
should hold a standard Postgres/Neon connection string, e.g.:

    postgresql://user:password@host/dbname?sslmode=require&channel_binding=require

Timestamps are stored as ISO-8601 TEXT (UTC) to match the original schema and
the dashboard's parsing — no behavioural change versus the old SQLite layout.
"""

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Set DATABASE_URL in the environment (or a local .env) to your Neon
# connection string — it is never hardcoded here so no credential is committed.
DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_connection():
    """Open a new psycopg2 connection to the configured Postgres database."""
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL is not set. Point it at your Neon Postgres connection "
            "string (see .env.example)."
        )
    return psycopg2.connect(DATABASE_URL)


def init_db(conn=None):
    """Create the entities / sentiment_scores tables and indexes if missing.

    Pass an existing connection to reuse it (the caller keeps ownership);
    otherwise a fresh connection is opened and returned.
    """
    own = conn is None
    if own:
        conn = get_connection()

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS entities (
                id          SERIAL PRIMARY KEY,
                name        TEXT    UNIQUE NOT NULL,
                category    TEXT,
                entity_type TEXT,
                first_seen  TEXT    NOT NULL,
                last_seen   TEXT    NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sentiment_scores (
                id               SERIAL PRIMARY KEY,
                entity_id        INTEGER NOT NULL REFERENCES entities(id),
                timestamp        TEXT    NOT NULL,
                sentiment        TEXT    NOT NULL,
                confidence       DOUBLE PRECISION NOT NULL,
                sentiment_score  DOUBLE PRECISION NOT NULL,
                reasoning        TEXT,
                intensity        TEXT,
                mention_count    INTEGER,
                engagement_score DOUBLE PRECISION,
                source           TEXT,
                sample_size      INTEGER
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ss_entity_ts "
            "ON sentiment_scores(entity_id, timestamp)"
        )
        # Unique index lets writers use INSERT ... ON CONFLICT DO NOTHING for
        # idempotent (re-runnable) inserts of the same point.
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_ss_unique "
            "ON sentiment_scores(entity_id, timestamp, source)"
        )
    conn.commit()
    return conn
