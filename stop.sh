#!/usr/bin/env bash
# stop.sh — Shut down the Zeitgeist pipeline (macOS / Linux)
#
# Best-effort kills are expected to return non-zero; do NOT use bare `set -e`.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || { echo "ERROR: cannot cd to $SCRIPT_DIR"; exit 1; }

PIDS_FILE="$SCRIPT_DIR/.pids"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

if [ -f "$PIDS_FILE" ]; then
    while IFS='=' read -r name pid; do
        # Skip blank lines / malformed entries.
        [ -z "${name:-}" ] && continue
        [ -z "${pid:-}" ] && continue
        if kill -0 "$pid" 2>/dev/null; then
            # macOS has no process-tree kill: signal the process, then reap any
            # children directly (e.g. streamlit's worker, run_llama's llama-server).
            kill "$pid" 2>/dev/null
            pkill -P "$pid" 2>/dev/null
            echo "  Stopped $name (PID $pid)"
        else
            # Process is gone but may have orphaned children — clean those too.
            pkill -P "$pid" 2>/dev/null
            echo "  $name (PID $pid) already gone"
        fi
    done < "$PIDS_FILE"
    rm -f "$PIDS_FILE"
else
    echo "No .pids found - pipeline may not be running."
fi

echo ""
echo "Stopping Docker..."
docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1
echo "Done."
