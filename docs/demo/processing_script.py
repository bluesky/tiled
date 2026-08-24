"""Reduce a measured detector image stack to an integrated image.

This is the analysis script referenced by the Tiled demo's provenance graph
(the ``software`` entity ``processing_script.py``). It reproduces the reduction
pipeline that produced the ``linked`` datasets in ``tiled serve demo``::

    measured - background -> subtracted -> normalized -> integrated
                                                     \\-> summary (table)

It reads ``measured`` and ``background`` from a Tiled server, computes the
derived products, and (optionally) writes them back under the same container.
Run it against the demo server like::

    python processing_script.py http://localhost:8000 --api-key secret

The numbers here intentionally match ``tiled/examples/demo.py`` so that the
graph's lineage links describe a computation you can actually re-run.
"""

from __future__ import annotations

import argparse

import numpy
import pandas
from tiled.client import from_uri


def subtract_background(measured: numpy.ndarray, background: numpy.ndarray) -> numpy.ndarray:
    """Remove the shared background frame from every measured frame."""
    return measured - background[None, :, :]


def normalize(subtracted: numpy.ndarray) -> numpy.ndarray:
    """Scale the stack into the unit interval [0, 1]."""
    lo, hi = subtracted.min(), subtracted.max()
    return (subtracted - lo) / (hi - lo)


def integrate(normalized: numpy.ndarray) -> numpy.ndarray:
    """Average over frames to get a per-pixel integrated image."""
    return normalized.mean(axis=0)


def summarize(normalized: numpy.ndarray) -> pandas.DataFrame:
    """Build a per-frame summary table from the normalized stack."""
    n_frames = normalized.shape[0]
    frames = numpy.arange(n_frames)
    intensity = normalized.mean(axis=(1, 2))
    phase = numpy.where(
        frames < n_frames / 3,
        "early",
        numpy.where(frames < 2 * n_frames / 3, "middle", "late"),
    )
    return pandas.DataFrame(
        {
            "intensity": intensity,
            "cumulative_intensity": numpy.cumsum(intensity),
            "sqrt_intensity": numpy.sqrt(intensity),
            "log_intensity": numpy.log1p(intensity),
            "is_peak": intensity > intensity.mean(),
            "phase": phase,
        },
        index=pandas.Index(frames, name="frame"),
    )


def reduce_container(client, *, write: bool = False) -> dict:
    """Run the full reduction on a `linked`-style container of datasets."""
    measured = client["measured"][:]
    background = client["background"][:]

    subtracted = subtract_background(measured, background)
    normalized = normalize(subtracted)
    integrated = integrate(normalized)
    summary = summarize(normalized)

    if write:
        client.write_array(subtracted, key="subtracted")
        client.write_array(normalized, key="normalized")
        client.write_array(integrated, key="integrated")
        client.write_dataframe(summary, key="summary")

    return {
        "subtracted": subtracted,
        "normalized": normalized,
        "integrated": integrated,
        "summary": summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Base URL of the Tiled server.")
    parser.add_argument(
        "--container",
        default="linked",
        help="Container holding `measured` and `background` (default: linked).",
    )
    parser.add_argument("--api-key", default=None, help="Tiled API key for writes.")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the derived datasets back to the container.",
    )
    args = parser.parse_args()

    client = from_uri(args.url, api_key=args.api_key)[args.container]
    results = reduce_container(client, write=args.write)

    integrated = results["integrated"]
    print(f"integrated image: shape={integrated.shape}, mean={integrated.mean():.4f}")
    print(results["summary"])


if __name__ == "__main__":
    main()
