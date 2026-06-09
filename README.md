# zeitgeist

> Real-time sentiment pipeline that determines the most loved and hated things on the internet.

## Architecture

```
[Reddit Producer] ──→ raw.reddit ──┐
[YouTube Producer] ─→ raw.youtube ─┤→ [Flink] → processed.signals → [LLM Service] → Postgres (Neon) → Streamlit
[News Producer] ────→ raw.news ────┘                                       └→ scored.sentiment (Kafka, for Oracle)
```

**Stack:**
- **Kafka** — message broker for raw and processed data streams
- **Apache Flink** — sliding window aggregation and signal normalization
- **VADER / llama.cpp** — sentiment scoring (VADER by default; llama.cpp if running locally)
- **Postgres (Neon)** — persistence layer for sentiment scores (set via `DATABASE_URL`)
- **Streamlit** — real-time leaderboard dashboard

## Project Structure

```
zeitgeist/
├── start.ps1 / start.sh     # Start the full pipeline (Windows / macOS-Linux)
├── stop.ps1 / stop.sh       # Stop everything and tear down Docker
├── verify_pipeline.py       # Health check for all components
├── docker-compose.yml       # Kafka, Zookeeper, Flink, Kafdrop
├── .env.example             # Environment variables template
├── entities.py              # Seed entity list
├── producers/
│   ├── reddit_producer.py   # Reddit public JSON API (no credentials needed)
│   ├── youtube_producer.py  # YouTube Data API v3
│   └── news_producer.py     # Google News RSS + NewsAPI
├── flink/
│   └── sentiment_pipeline.py
├── llm_service/
│   └── sentiment_scorer.py
├── dashboard/
│   └── app.py
└── db.py                    # Postgres (Neon) connection + schema
```

Sentiment data is stored in **Neon Postgres**. Tables are created automatically
on first run; point `DATABASE_URL` at your Neon connection string in `.env`.

## Setup

### 1. Prerequisites
- Docker Desktop (running)
- Python 3.10+

### 2. Clone and configure
```powershell
git clone https://github.com/KylePeiman/Zeitgeist.git
cd Zeitgeist
cp .env.example .env
# Add your YouTube API key to .env (optional — pipeline works without it)
```

### 3. Install Python dependencies
```powershell
pip install -r requirements.txt
```

> **Required for entity discovery:** `requirements.txt` includes the spaCy
> `en_core_web_sm` model. Without it, NER discovery is disabled and the pipeline
> can only ever track the seed entities (the "stuck at 94" symptom). If the
> bundled wheel URL can't be fetched in your environment, install it directly:
> ```powershell
> python -m spacy download en_core_web_sm
> ```
> Confirm it's enabled any time with `python verify_pipeline.py` (the `[NER]`
> line must say "ENABLED").

> **After pulling new code:** re-run `pip install -r requirements.txt` and
> restart the pipeline (`.\stop.ps1` then `.\start.ps1` on Windows, or
> `./stop.sh` then `./start.sh` on macOS/Linux) — long-running
> producer/scorer processes do not pick up code or dependency changes until
> restarted.

### 4. Start everything

**Windows (PowerShell):**
```powershell
.\start.ps1
```

**macOS / Linux (bash):**
```bash
./start.sh
```

This single command:
- Tears down any previous Docker state (prevents Zookeeper stale-node crashes)
- Starts Kafka, Zookeeper, Flink, and Kafdrop via Docker
- Waits for Kafka to be healthy
- Launches all 6 pipeline services silently in the background
- Logs each service to `logs/<name>.log`

### 5. Verify
```powershell
python verify_pipeline.py
```

### 6. Stop everything

**Windows (PowerShell):**
```powershell
.\stop.ps1
```

**macOS / Linux (bash):**
```bash
./stop.sh
```

Kills all pipeline processes and tears down Docker (including volumes).

## API Keys

| Source | Key required? | Where to get one |
|--------|--------------|-----------------|
| Reddit | Recommended | Public JSON works locally but **403-blocks most cloud/datacenter IPs**. For reliable ingestion create a free "script" app at [reddit.com/prefs/apps](https://www.reddit.com/prefs/apps) and set `REDDIT_CLIENT_ID`/`REDDIT_CLIENT_SECRET` in `.env`; the producer uses the authenticated API when present and falls back to public JSON otherwise. |
| YouTube | Optional | [Google Cloud Console](https://console.cloud.google.com) — enable YouTube Data API v3 |
| News | No | Google News RSS needs no key (runs by default). Optionally set `NEWS_API_KEY` from [newsapi.org](https://newsapi.org) to also pull NewsAPI. |

If `raw.reddit` or `raw.news` show **0 messages** in `python verify_pipeline.py`,
that producer isn't ingesting — check `logs/reddit.log` / `logs/news.log` for
403s (Reddit → set OAuth creds) or a missing `feedparser` (News → re-run
`pip install -r requirements.txt`).

## UIs

| Service | URL |
|---------|-----|
| Streamlit Dashboard | http://localhost:8501 |
| Kafdrop (Kafka UI) | http://localhost:9000 |
| Flink UI | http://localhost:8081 |

## Kafka Topics

| Topic | Description |
|-------|-------------|
| `raw.reddit` | Raw Reddit posts and comments |
| `raw.youtube` | Raw YouTube comments and metadata |
| `raw.news` | Raw news headlines and articles |
| `processed.signals` | Normalized signals from Flink, ready for scoring |
| `scored.sentiment` | Scored sentiment results from the LLM Service (consumed by downstream services like Oracle) |

## LLM Scoring

The scorer defaults to VADER for fast local scoring. If you have [llama.cpp](https://github.com/ggerganov/llama.cpp) running locally at `http://localhost:8080`, it will use that instead for richer sentiment analysis.

Start a server with `./scripts/run_llama.sh`. It loads a local GGUF from `../models` and never downloads — place a `qwen2.5-1.5b-instruct-*.gguf` there first. For better quality on an 8 GB machine, add a `qwen2.5-3b-instruct-*.gguf` and run `LLAMA_MODEL=3b ./scripts/run_llama.sh`. Grab files with e.g. `hf download Qwen/Qwen2.5-3B-Instruct-GGUF qwen2.5-3b-instruct-q5_k_m.gguf --local-dir ../models`.
