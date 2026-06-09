#!/usr/bin/env bash
# run_llama.sh — launch a llama.cpp server for the Zeitgeist sentiment scorer.
#
# The scorer (llm_service/sentiment_scorer.py) is model-agnostic: it just POSTs
# to LLAMA_SERVER_URL and falls back to VADER if no server is reachable. This
# script starts that server from a LOCAL GGUF file with 8 GB-friendly defaults.
# It never downloads models — if it can't find one it tells you how to get it.
#
# Models are looked up in ../models (relative to the repo), overridable via
# LLAMA_MODELS_DIR, or pinned exactly with LLAMA_MODEL_FILE=/path/to/model.gguf.
#
# Usage:
#   ./scripts/run_llama.sh                  # 1.5b (default)
#   LLAMA_MODEL=3b ./scripts/run_llama.sh   # better-quality upgrade
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Share config with the rest of the stack via .env (ignores comments/blank lines).
set -a
[ -f "$REPO_ROOT/.env" ] && . "$REPO_ROOT/.env"
set +a

# Map the model choice to a filename pattern (quant-agnostic — q4, q5, etc).
LLAMA_MODEL="${LLAMA_MODEL:-1.5b}"
case "$LLAMA_MODEL" in
  1.5b) MODEL_GLOB="qwen2.5-1.5b-instruct-*.gguf" ;;
  3b)   MODEL_GLOB="qwen2.5-3b-instruct-*.gguf" ;;
  *)
    echo "error: unknown LLAMA_MODEL='$LLAMA_MODEL' (valid options: 1.5b, 3b)" >&2
    exit 1
    ;;
esac

# Where to look for local GGUF files (../models by default).
MODELS_DIR="${LLAMA_MODELS_DIR:-$REPO_ROOT/../models}"

# Resolve the GGUF: explicit LLAMA_MODEL_FILE wins, else first match in MODELS_DIR.
# Never downloads.
if [ -n "${LLAMA_MODEL_FILE:-}" ]; then
  MODEL_FILE="$LLAMA_MODEL_FILE"
else
  MODEL_FILE="$(ls -1 "$MODELS_DIR"/$MODEL_GLOB 2>/dev/null | head -1 || true)"
fi

if [ -z "${MODEL_FILE:-}" ] || [ ! -f "$MODEL_FILE" ]; then
  echo "error: no local GGUF found for LLAMA_MODEL='$LLAMA_MODEL'." >&2
  echo "  looked in: $MODELS_DIR" >&2
  echo "  pattern:   $MODEL_GLOB" >&2
  echo "  Download one manually (then re-run), e.g.:" >&2
  echo "    hf download Qwen/Qwen2.5-3B-Instruct-GGUF qwen2.5-3b-instruct-q5_k_m.gguf --local-dir \"$MODELS_DIR\"" >&2
  echo "  or point at a specific file: LLAMA_MODEL_FILE=/path/to/model.gguf $0" >&2
  exit 1
fi

LLAMA_SERVER_EXE="${LLAMA_SERVER_EXE:-llama-server}"

# Derive the port from LLAMA_SERVER_URL (default 8080).
LLAMA_SERVER_URL="${LLAMA_SERVER_URL:-http://localhost:8080}"
PORT="${LLAMA_SERVER_URL##*:}"
PORT="${PORT%%/*}"
[ -n "$PORT" ] || PORT=8080

NGL="${LLAMA_NGL:-99}"
CTX="${LLAMA_CTX:-4096}"
PARALLEL="${LLAMA_PARALLEL:-2}"

echo "Starting llama-server: model=$LLAMA_MODEL file=$MODEL_FILE port=$PORT parallel=$PARALLEL"
echo "(the sentiment scorer falls back to VADER if this server isn't running)"

exec "$LLAMA_SERVER_EXE" \
  -m "$MODEL_FILE" \
  --port "$PORT" \
  --n-gpu-layers "$NGL" \
  --ctx-size "$CTX" \
  --parallel "$PARALLEL" \
  "$@"
