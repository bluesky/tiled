#!/usr/bin/env bash
# Benchmark the base commit and the current checkout, then compare them.
#
#   performance/run_ab.sh --build        # build both databases, then benchmark
#   performance/run_ab.sh                # benchmark existing databases
#   BASE=main performance/run_ab.sh      # any commit-ish (default: merge-base
#                                        # with upstream/main)
#   performance/run_ab.sh --only deep    # other args go to bench_sql.py
#   BUILD_ARGS="--children 1000000" performance/run_ab.sh --build
#
# The base is checked out (detached) into a sibling git worktree, and
# PYTHONPATH points the scripts at it. Each version gets its own database,
# built by its own code, because tiled refuses to open a catalog whose schema
# revision differs from the one it expects. The data is identical in both.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
PY=${PY:-$REPO/.pixi/envs/py313/bin/python}
BASE=${BASE:-$(git -C "$REPO" merge-base HEAD upstream/main)}
WORKTREE=${WORKTREE:-$REPO/../tiled-perf-base}
SERVER=${SERVER:-postgresql+asyncpg://tiled:tiled@localhost:5433}
BASE_URI=${BASE_URI:-$SERVER/tiled_perf_base}
BRANCH_URI=${BRANCH_URI:-$SERVER/tiled_perf_branch}

BUILD=0
if [ "${1:-}" = "--build" ]; then
    BUILD=1
    shift
fi

if [ ! -d "$WORKTREE" ]; then
    git -C "$REPO" worktree add --detach "$WORKTREE" "$BASE"
else
    git -C "$WORKTREE" checkout --detach --quiet "$BASE"
fi
# Generated at install time and gitignored, so the worktree lacks it.
cp "$REPO/tiled/_version.py" "$WORKTREE/tiled/_version.py"

if [ "$BUILD" = 1 ]; then
    echo "== building base database"
    # shellcheck disable=SC2086
    PYTHONPATH="$WORKTREE" "$PY" "$HERE/build_dataset.py" --uri "$BASE_URI" --reset ${BUILD_ARGS:-}
    echo
    echo "== building branch database"
    # shellcheck disable=SC2086
    "$PY" "$HERE/build_dataset.py" --uri "$BRANCH_URI" --reset ${BUILD_ARGS:-}
    echo
fi

echo "== base: $(git -C "$WORKTREE" log -1 --format='%h %s')"
PYTHONPATH="$WORKTREE" "$PY" "$HERE/bench_sql.py" --uri "$BASE_URI" --label base "$@"

echo
echo "== branch: $(git -C "$REPO" log -1 --format='%h %s')"
"$PY" "$HERE/bench_sql.py" --uri "$BRANCH_URI" --label branch "$@"

echo
"$PY" "$HERE/compare.py" "$HERE/results/base" "$HERE/results/branch"
