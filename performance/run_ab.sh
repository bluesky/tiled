#!/usr/bin/env bash
# Benchmark the base commit and the current checkout against the same database,
# then compare them.
#
#   performance/run_ab.sh                # base = merge-base with upstream/main
#   BASE=main performance/run_ab.sh      # any commit-ish
#   performance/run_ab.sh --only deep    # extra args go to bench_sql.py
#
# The base is checked out (detached) into a sibling git worktree, and
# PYTHONPATH points the benchmark at it. Assumes the database was built with
# build_dataset.py and that both versions share its schema.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)
PY=${PY:-$REPO/.pixi/envs/py313/bin/python}
BASE=${BASE:-$(git -C "$REPO" merge-base HEAD upstream/main)}
WORKTREE=${WORKTREE:-$REPO/../tiled-perf-base}

if [ ! -d "$WORKTREE" ]; then
    git -C "$REPO" worktree add --detach "$WORKTREE" "$BASE"
else
    git -C "$WORKTREE" checkout --detach --quiet "$BASE"
fi
# Generated at install time and gitignored, so the worktree lacks it.
cp "$REPO/tiled/_version.py" "$WORKTREE/tiled/_version.py"

echo "== base: $(git -C "$WORKTREE" log -1 --format='%h %s')"
PYTHONPATH="$WORKTREE" "$PY" "$HERE/bench_sql.py" --label base "$@"

echo
echo "== branch: $(git -C "$REPO" log -1 --format='%h %s')"
"$PY" "$HERE/bench_sql.py" --label branch "$@"

echo
"$PY" "$HERE/compare.py" "$HERE/results/base" "$HERE/results/branch"
