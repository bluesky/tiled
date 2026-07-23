#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

CONFIG_PATH="$HERE/graph_example_config.yml"
HOST="${TILED_HOST:-127.0.0.1}"
PORT="${TILED_PORT:-8000}"
API_KEY="${TILED_API_KEY:-secret}"

SERVER_PID=""

cd "$ROOT"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}

handle_interrupt() {
  echo
  echo "Stopping server..."
  cleanup
  exit 130
}

trap cleanup EXIT
trap handle_interrupt INT TERM

port_is_open() {
  "${PYTHON:-python}" - "$1" "$2" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.settimeout(0.2)
    sys.exit(0 if s.connect_ex((host, port)) == 0 else 1)
PY
}

if port_is_open "$HOST" "$PORT"; then
  CANDIDATE_PORT=$((PORT + 1))
  while port_is_open "$HOST" "$CANDIDATE_PORT"; do
    CANDIDATE_PORT=$((CANDIDATE_PORT + 1))
  done
  echo "Port $PORT is in use; switching to free port $CANDIDATE_PORT"
  PORT="$CANDIDATE_PORT"
fi

BASE_URL="${TILED_BASE_URL:-http://$HOST:$PORT}"
export TILED_BASE_URL="$BASE_URL"
export TILED_API_KEY="$API_KEY"

echo "Starting tiled server using $CONFIG_PATH"
"${PYTHON:-python}" "$HERE/serve_with_config.py" --config "$CONFIG_PATH" --host "$HOST" --port "$PORT" --api-key "$API_KEY" &
SERVER_PID=$!

echo "Waiting for server at $BASE_URL/api/v1"
for _ in $(seq 1 40); do
  if curl -fsS "$BASE_URL/api/v1" >/dev/null; then
    break
  fi
  if [[ -n "${SERVER_PID}" ]] && ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "Server process exited before becoming ready."
    wait "$SERVER_PID" || true
    exit 1
  fi
  sleep 0.25
done

curl -fsS "$BASE_URL/api/v1" >/dev/null

echo "Creating catalog datasets"
python "$HERE/create_datasets.py"

echo "Creating graph links through GraphQL and exporting JSON-LD"
python "$HERE/create_links_and_export_jsonld.py"

echo "Done. JSON-LD export is at $HERE/exported_graph.jsonld"
echo "Tiled server is still running at $BASE_URL"
echo "Press Ctrl+C to stop."

wait "$SERVER_PID"
