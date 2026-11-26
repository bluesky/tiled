"""
Convert pytest-benchmark JSON output into the dashboard's data format.

The dashboard (github-action-benchmark, `customSmallerIsBetter`) takes a list
of `{"name", "unit", "value", "range", "extra"}` entries. The value reported
is the median of the rounds, in milliseconds, with the interquartile range
as the uncertainty: both are robust to the occasional slow round on a shared
CI runner. Other sources of measurements (e.g. load tests) can be emitted in
the same format and shown on the same dashboard.

Usage: python benchmarks/to_dashboard.py benchmark.json > dashboard.json
"""

import json
import sys


def convert(report):
    entries = []
    for bench in report["benchmarks"]:
        stats = bench["stats"]
        entries.append(
            {
                "name": bench["fullname"].removeprefix("benchmarks/"),
                "unit": "ms",
                "value": round(stats["median"] * 1000, 3),
                "range": f"IQR {stats['iqr'] * 1000:.3f} ms",
                "extra": (
                    f"min: {stats['min'] * 1000:.3f} ms\n"
                    f"mean: {stats['mean'] * 1000:.3f} ms\n"
                    f"rounds: {stats['rounds']}"
                ),
            }
        )
    return entries


if __name__ == "__main__":
    with open(sys.argv[1]) as file:
        report = json.load(file)
    json.dump(convert(report), sys.stdout, indent=1)
