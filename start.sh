#!/usr/bin/env bash
# start.sh — Launch the full Zeitgeist pipeline (macOS / Linux)
#
# Tolerate non-zero exits from poll/wait loops: do NOT use bare `set -e`.
set -uo pipefail

# Resolve the script's own directory and cd into it so relative service
# paths (producers/…, dashboard/…) work no matter where this is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || { echo "ERROR: cannot cd to $SCRIPT_DIR"; exit 1; }

# Prefer the project venv (.venv) when present so its installed requirements are
# used without needing to activate it first. Both are overridable via env.
if [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON="${PYTHON:-$SCRIPT_DIR/.venv/bin/python}"
    STREAMLIT="${STREAMLIT:-$SCRIPT_DIR/.venv/bin/streamlit}"
    echo "Using project venv: $SCRIPT_DIR/.venv"
else
    PYTHON="${PYTHON:-python3}"
    STREAMLIT="${STREAMLIT:-streamlit}"
    echo "No .venv found — using '$PYTHON' on PATH (ensure requirements are installed)."
fi

LOGS_DIR="$SCRIPT_DIR/logs"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
PIDS_FILE="$SCRIPT_DIR/.pids"

mkdir -p "$LOGS_DIR" || { echo "ERROR: cannot create $LOGS_DIR"; exit 1; }
# Start a fresh PID file for this run.
: > "$PIDS_FILE"

# ── DOCKER ────────────────────────────────────────────────────
echo "Starting Docker infrastructure..."
# Tear down first to clear any stale Zookeeper ephemeral nodes from a previous run.
docker compose -f "$COMPOSE_FILE" down -v  >  "$LOGS_DIR/docker.log" 2>&1
docker compose -f "$COMPOSE_FILE" up -d     >> "$LOGS_DIR/docker.log" 2>&1 \
    || { echo "ERROR: 'docker compose up' failed. See $LOGS_DIR/docker.log"; exit 1; }

printf "Waiting for Kafka to be ready"
kafka_ready=false
deadline=$(( $(date +%s) + 120 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    # Wait for container health, then verify Kafka actually responds to a topic list.
    health="$(docker inspect --format '{{.State.Health.Status}}' zeitgeist-kafka 2>/dev/null)"
    if [ "$health" = "healthy" ]; then
        if docker exec zeitgeist-kafka \
            kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            kafka_ready=true
            break
        fi
    fi
    printf "."
    sleep 2
done
printf "\n"

if [ "$kafka_ready" != true ]; then
    echo "ERROR: Kafka did not become ready within 120s. Check: docker compose ps"
    exit 1
fi
echo "Kafka is ready."

echo "Creating Kafka topics..."
# Includes scored.sentiment — the scorer dual-writes scored results here for Oracle.
TOPICS="raw.reddit raw.youtube raw.news processed.signals scored.sentiment"
for topic in $TOPICS; do
    docker exec zeitgeist-kafka \
        kafka-topics --bootstrap-server localhost:9092 \
        --create --if-not-exists --topic "$topic" \
        --partitions 3 --replication-factor 1 >/dev/null 2>&1
done
echo "Topics ready."
echo ""

# ── LLAMA.CPP ─────────────────────────────────────────────────
# Apple Silicon → Metal. Use the repo's own launcher, which finds a local GGUF
# in ../models and applies Metal/8GB-friendly flags before exec'ing llama-server.
# A missing model is NOT fatal — the scorer falls back to VADER.
echo "Starting llama.cpp server..."
nohup ./scripts/run_llama.sh >> "$LOGS_DIR/llama.log" 2>&1 &
llama_pid=$!
echo "llama=$llama_pid" >> "$PIDS_FILE"

printf "Waiting for model to load"
llama_ready=false
llama_deadline=$(( $(date +%s) + 300 ))
while [ "$(date +%s)" -lt "$llama_deadline" ]; do
    # If run_llama.sh exited early (e.g. no model file found), stop waiting.
    if ! kill -0 "$llama_pid" 2>/dev/null; then
        break
    fi
    if body="$(curl -fsS --max-time 2 http://127.0.0.1:8080/health 2>/dev/null)" \
        && printf '%s' "$body" | grep -q '"ok"'; then
        llama_ready=true
        break
    fi
    printf "."
    sleep 3
done
printf "\n"

if [ "$llama_ready" = true ]; then
    echo "llama.cpp is ready (Metal)."
else
    echo "WARNING: llama.cpp is not available (no model found or failed to load)."
    echo "         The scorer will use the VADER fallback. Continuing."
fi
echo ""

# ── PYTHON / STREAMLIT PROCESSES ──────────────────────────────
# Each launched with nohup, stderr merged into stdout, logged to logs/<name>.log,
# and its PID recorded to .pids as a `name=pid` line.
start_service() {
    local name="$1"; shift
    nohup "$@" >> "$LOGS_DIR/$name.log" 2>&1 &
    local pid=$!
    echo "$name=$pid" >> "$PIDS_FILE"
    echo "  [$name] started  (PID $pid)  ->  logs/$name.log"
}

start_service reddit    "$PYTHON" producers/reddit_producer.py
start_service youtube   "$PYTHON" producers/youtube_producer.py
start_service news      "$PYTHON" producers/news_producer.py
start_service flink     "$PYTHON" flink/sentiment_pipeline.py
start_service scorer    "$PYTHON" llm_service/sentiment_scorer.py
# --server.headless true skips Streamlit's first-run interactive email prompt
# (which otherwise hangs a backgrounded launch) and the auto browser-open.
start_service dashboard "$STREAMLIT" run dashboard/app.py --server.headless true

echo ""
echo "Zeitgeist is running."
echo "  Dashboard : http://localhost:8501"
echo "  Kafdrop   : http://localhost:9000"
echo "  Logs      : $LOGS_DIR"
echo "  Stop      : ./stop.sh"
