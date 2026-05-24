# zeitgeist 🌐

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
- **llama.cpp (phi-4-mini)** — local LLM for sentiment scoring
- **SQLite** — persistence layer for sentiment scores
- **Streamlit** — real-time leaderboard dashboard

## Project Structure

```
zeitgeist/
├── docker-compose.yml       # Kafka, Flink, Kafdrop
├── .env.example             # Environment variables template
├── .env                     # Your local config (never commit this)
├── producers/
│   ├── reddit_producer.py
│   ├── youtube_producer.py
│   └── news_producer.py
├── flink/
│   └── sentiment_pipeline.py
├── llm_service/
│   └── sentiment_scorer.py
├── dashboard/
│   └── app.py
├── data/
│   └── zeitgeist.db         # SQLite database (auto-created)
└── entities.py              # Seed entity list
```

## Setup

### 1. Prerequisites
- Docker Desktop (running)
- Python 3.10+
- llama.cpp server running locally with phi-4-mini model

### 2. Clone and configure
```bash
git clone https://github.com/KylePeiman/zeitgeist.git
cd zeitgeist
cp .env.example .env
# Edit .env with your API keys
```

### 3. Start the infrastructure
```bash
docker compose up -d
```

### 4. Verify everything is healthy
```bash
docker compose ps
```

All services should show `healthy` or `running`.

### 5. Access the UIs
| Service | URL |
|---------|-----|
| Kafdrop (Kafka UI) | http://localhost:9000 |
| Flink UI | http://localhost:8081 |
| Streamlit Dashboard | http://localhost:8501 |

### 6. Get API keys
- **Reddit**: https://www.reddit.com/prefs/apps → create a "script" app
- **YouTube**: https://console.cloud.google.com → enable YouTube Data API v3
- **News API**: https://newsapi.org → free tier (100 req/day)

### 7. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 8. Run the producers
```bash
python producers/reddit_producer.py
python producers/youtube_producer.py
python producers/news_producer.py
```

### 9. Submit the Flink job
```bash
python flink/sentiment_pipeline.py
```

### 10. Start the LLM service
```bash
python llm_service/sentiment_scorer.py
```

### 11. Launch the dashboard
```bash
streamlit run dashboard/app.py
```

## Kafka Topics

| Topic | Description |
|-------|-------------|
| `raw.reddit` | Raw Reddit posts and comments |
| `raw.youtube` | Raw YouTube comments and metadata |
| `raw.news` | Raw news headlines and articles |
| `processed.signals` | Normalized signals from Flink, ready for LLM |

## Stopping the stack
```bash
docker compose down
```

To also remove volumes (wipes all Kafka data):
```bash
docker compose down -v
```
