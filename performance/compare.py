"""
Compare two bench_sql.py runs side by side.

    python performance/compare.py performance/results/base performance/results/branch

Prints the median time of each scenario in both runs and the ratio
(second / first). Result counts that differ are flagged with '*': that is
expected where inheritance makes untagged nodes visible on the branch
(half_untagged, deep), and a bug anywhere else.
"""

import json
import sys
from pathlib import Path


def load(path):
    path = Path(path)
    if path.is_dir():
        path = path / "summary.json"
    return json.loads(path.read_text())


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    a, b = load(sys.argv[1]), load(sys.argv[2])
    print(f"A = {a['label']}: {a['commit']}")
    print(f"B = {b['label']}: {b['commit']}")
    print()
    header = (
        f"{'scenario':32} {'A ms':>10} {'B ms':>10} {'B/A':>8}"
        f" {'A stmts':>8} {'B stmts':>8} {'A rows':>7} {'B rows':>7}"
    )
    print(header)
    print("-" * len(header))
    for name, ra in a["scenarios"].items():
        rb = b["scenarios"].get(name)
        if rb is None:
            continue
        ratio = rb["median_ms"] / ra["median_ms"] if ra["median_ms"] else float("inf")
        flag = " *" if ra["results"] != rb["results"] else ""
        print(
            f"{name:32} {ra['median_ms']:10.1f} {rb['median_ms']:10.1f}"
            f" {ratio:7.1f}x {ra['statements']:8} {rb['statements']:8}"
            f" {ra['results']:7} {rb['results']:7}{flag}"
        )


if __name__ == "__main__":
    main()
