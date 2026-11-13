#!/usr/bin/env bash
# Start or stop a throwaway PostgreSQL server for the access-tag benchmarks.
#
#   performance/postgres.sh start   # listens on localhost:${PGPORT:-5433}
#   performance/postgres.sh stop    # removes the container and its data
#
# Uses podman if available, otherwise docker.
set -euo pipefail

NAME=tiled-perf-postgres
PORT=${PGPORT:-5433}
IMAGE=${PGIMAGE:-docker.io/library/postgres:17}
ENGINE=$(command -v podman || command -v docker || true)

if [ -z "$ENGINE" ]; then
    echo "Neither podman nor docker found." >&2
    exit 1
fi

case "${1:-}" in
start)
    "$ENGINE" run -d --rm --name "$NAME" \
        -e POSTGRES_USER=tiled -e POSTGRES_PASSWORD=tiled -e POSTGRES_DB=tiled_perf \
        -p "$PORT":5432 \
        --shm-size=1g \
        "$IMAGE" \
        -c shared_buffers=1GB -c work_mem=64MB -c maintenance_work_mem=512MB \
        -c max_wal_size=4GB -c synchronous_commit=off >/dev/null
    echo -n "Waiting for PostgreSQL"
    until "$ENGINE" exec "$NAME" pg_isready -U tiled -d tiled_perf >/dev/null 2>&1; do
        echo -n "."
        sleep 1
    done
    echo
    echo "Ready: postgresql+asyncpg://tiled:tiled@localhost:$PORT/tiled_perf"
    ;;
stop)
    "$ENGINE" rm -f "$NAME" >/dev/null
    echo "Stopped $NAME."
    ;;
*)
    echo "usage: $0 start|stop" >&2
    exit 2
    ;;
esac
