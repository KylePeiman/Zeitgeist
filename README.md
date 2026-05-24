# zeitgeist

> Real-time sentiment pipeline that determines the most loved and hated things on the internet.

## Architecture

```
[Reddit Producer] ──→ raw.reddit ──┐
[YouTube Producer] ─→ raw.youtube ─┤→ [Flink] → processed.signals → [LLM Service] → SQLite → Streamlit
[News Producer] ────→ raw.news ────┘
```

**Stack:**
- **Kafka** — message broker for raw and processed data streams
- **Apache Flink** — sliding window aggregation and signal normalization
- **VADER / llama.cpp** — sentiment scoring (VADER by default; llama.cpp if running locally)
- **SQLite** — persistence layer for sentiment scores
- **Streamlit** — real-time leaderboard dashboard

## Project Structure

```
zeitgeist/
├── start.ps1                # Start the full pipeline (one command)
├── stop.ps1                 # Stop everything and tear down Docker
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
└── data/
    └── zeitgeist.db         # SQLite database (auto-created, gitignored)
```

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

### 4. Start everything
```powershell
.\start.ps1
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
```powershell
.\stop.ps1
```

Kills all pipeline processes and tears down Docker (including volumes).

## API Keys

| Source | Key required? | Where to get one |
|--------|--------------|-----------------|
| Reddit | No | Uses public JSON API — no credentials needed |
| YouTube | Optional | [Google Cloud Console](https://console.cloud.google.com) — enable YouTube Data API v3 |
| News | Optional | [newsapi.org](https://newsapi.org) free tier (100 req/day); falls back to Google News RSS |

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

## LLM Scoring

The scorer defaults to VADER for fast local scoring. If you have [llama.cpp](https://github.com/ggerganov/llama.cpp) running locally at `http://localhost:8080` with a compatible model, it will use that instead for richer sentiment analysis.
