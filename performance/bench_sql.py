"""
Time access-filtered catalog queries and capture their PostgreSQL plans.

Runs against whichever `tiled` is importable, so the same script benchmarks
`main` and this branch (see run_ab.sh, which points PYTHONPATH at a worktree).

The access filter is applied by calling the real
`tiled.server.utils.filter_for_access` and `tiled.server.dependencies.get_entry`
with a stub access policy that grants the user the tags in USER_TAGS. That way
each version's own request-path code runs, including any extra queries it
makes, without having to set up authentication and tag grants.

For every scenario this records:

- wall time over `--runs` repetitions (after one warm-up run)
- the number of SQL statements issued and the number of results
- `EXPLAIN (ANALYZE, BUFFERS)` of each SELECT issued, in plans/<scenario>.txt

Output goes to <out>/<label>/summary.json and <out>/<label>/plans/.
Compare two labels with compare.py.

Usage:

    python performance/bench_sql.py --label branch
    PYTHONPATH=../tiled-perf-base python performance/bench_sql.py --label base
"""

import argparse
import asyncio
import collections
import json
import statistics
import subprocess
import time
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import event

import tiled
from tiled.access_control.scopes import ALL_SCOPES
from tiled.catalog import from_uri
from tiled.queries import AccessTagsFilter, Key
from tiled.server.dependencies import get_entry
from tiled.server.utils import filter_for_access

DEFAULT_URI = "postgresql+asyncpg://tiled:tiled@localhost:5433/tiled_perf"
USER_TAGS = ["bench_user", "grp_0"]
PAGE = 100


class StubPolicy:
    """Grants USER_TAGS, like a TagBasedAccessPolicy for a non-admin user.

    `allowed_scopes` makes the same database call that TagBasedAccessPolicy
    makes for the node it is checking: `effective_access_tags()` where it
    exists (this branch), otherwise the node's own `access_tags` (main).
    """

    async def filters(self, node, principal, authn_access_tags, authn_scopes, scopes):
        return [AccessTagsFilter(list(USER_TAGS))]

    async def allowed_scopes(self, node, principal, authn_access_tags, authn_scopes):
        if hasattr(node, "effective_access_tags"):
            await node.effective_access_tags()
        elif hasattr(node, "access_tags"):
            node.access_tags
        return set(ALL_SCOPES)


POLICY = StubPolicy()


def metrics():
    "A fresh per-request metrics dict, shaped as the server makes it."
    return collections.defaultdict(lambda: collections.defaultdict(lambda: 0))


async def restrict(entry):
    "Apply the access filter to `entry`, as the server does on every request."
    return await filter_for_access(
        entry, POLICY, None, None, set(), ["read:metadata"], metrics()
    )


async def lookup(root, path):
    "Resolve `path` as the server does: one segment at a time, filtered."
    return await get_entry(
        path,
        ["read:metadata"],
        None,
        None,
        set(),
        root,
        {},
        metrics(),
        None,
        POLICY,
    )


def scenarios(root):
    """(name, coroutine function) pairs.

    Each returns a list of results, a count (int), or None if the path is not
    visible to the user (a 404 from the server).

    Listing scenarios look the container up without access filtering and then
    apply the filter to it, exactly as `get_entry` does for the last path
    segment. Going through `get_entry` for the whole path would 404 on
    `inherit_false/`, which the user cannot see; in practice that shape arises
    when listing the root (whose tags are not inherited) or a container
    reached another way. Path resolution is measured separately, by `walk.*`.
    """

    async def listing(path, op):
        entry = await restrict(await root.lookup_adapter([path]))
        return await op(entry)

    async def count(entry):
        return await entry.exact_len()

    async def keys_page(entry):
        return await entry.keys_range(0, PAGE)

    async def items_page(entry):
        return await entry.items_range(0, PAGE)

    async def metadata_search(entry):
        return await entry.search(Key("color") == "blue").keys_range(0, PAGE)

    async def deep_keys_page(entry):
        return await entry.search_deep().keys_range(0, PAGE)

    async def deep_count(entry):
        return await entry.search_deep().exact_len()

    async def deep_metadata_search(entry):
        return await (
            entry.search_deep().search(Key("color") == "blue").keys_range(0, PAGE)
        )

    async def walk(path):
        try:
            await lookup(root, path)
        except HTTPException as err:
            if err.status_code == 404:
                return None
            raise
        return [path]

    result = []
    for container in ["inherit_true", "inherit_false", "half_untagged", "half_match"]:
        for name, op in [
            ("count", count),
            ("keys_page", keys_page),
            ("items_page", items_page),
            ("metadata_search", metadata_search),
        ]:
            result.append(
                (f"{container}.{name}", lambda c=container, o=op: listing(c, o))
            )
    for container in ["deep", "deep_tagged"]:
        for name, op in [
            ("keys_page", deep_keys_page),
            ("count", deep_count),
            ("metadata_search", deep_metadata_search),
        ]:
            result.append(
                (f"{container}.{name}", lambda c=container, o=op: listing(c, o))
            )
    # Path resolution only: the per-request cost of reaching a node.
    # `chain/` is tagged all the way down, so it is visible on both versions.
    # `deep/n1/n2/n2/n2` is untagged below `deep/`: visible only with inheritance.
    result.append(("walk.tagged_depth2", lambda: walk("inherit_true/n0")))
    result.append(("walk.tagged_depth5", lambda: walk("chain/c/c/c/c")))
    result.append(("walk.untagged_depth5", lambda: walk("deep/n1/n2/n2/n2")))
    return result


class Recorder:
    "Capture the SQL statements executed while `active` is True."

    def __init__(self, engine):
        self.active = False
        self.statements = []
        event.listen(engine.sync_engine, "before_cursor_execute", self._capture)

    def _capture(self, conn, cursor, statement, parameters, context, executemany):
        if self.active:
            self.statements.append((statement, parameters))

    def start(self):
        self.statements = []
        self.active = True

    def stop(self):
        self.active = False
        return self.statements


async def explain(engine, statement, parameters):
    "EXPLAIN (ANALYZE, BUFFERS) one captured statement with its parameters."
    async with engine.connect() as conn:
        raw = await conn.get_raw_connection()
        driver = raw.driver_connection
        rows = await driver.fetch(
            "EXPLAIN (ANALYZE, BUFFERS) " + statement, *(parameters or ())
        )
        await conn.rollback()
    return "\n".join(r[0] for r in rows)


def n_results(results):
    if results is None:
        return -1
    if isinstance(results, int):
        return results
    return len(results)


def git_describe():
    here = Path(tiled.__file__).resolve().parent
    try:
        return subprocess.check_output(
            ["git", "-C", str(here), "log", "-1", "--format=%h %s"], text=True
        ).strip()
    except Exception:
        return "unknown"


async def run(args):
    root = from_uri(args.uri)
    for task in root.startup_tasks:
        await task()
    engine = root.context.engine
    if engine.dialect.name != "postgresql":
        raise SystemExit("These benchmarks are meant for PostgreSQL.")
    recorder = Recorder(engine)

    out = Path(args.out) / args.label
    (out / "plans").mkdir(parents=True, exist_ok=True)
    summary = {
        "label": args.label,
        "tiled": str(Path(tiled.__file__).resolve().parent),
        "commit": git_describe(),
        "user_tags": USER_TAGS,
        "runs": args.runs,
        "scenarios": {},
    }
    print(f"[{args.label}] tiled from {summary['tiled']}")
    print(f"[{args.label}] commit {summary['commit']}")

    selected = [
        (name, fn)
        for name, fn in scenarios(root)
        if not args.only or any(s in name for s in args.only)
    ]
    for name, fn in selected:
        await fn()  # warm up caches and connections
        times = []
        for _ in range(args.runs):
            recorder.start()
            start = time.perf_counter()
            results = await fn()
            times.append(time.perf_counter() - start)
            statements = recorder.stop()

        plans = []
        if not args.no_explain:
            for i, (sql, params) in enumerate(statements):
                if not sql.lstrip().upper().startswith(("SELECT", "WITH")):
                    continue
                plan = await explain(engine, sql, params)
                plans.append(
                    f"-- statement {i}\n{sql}\n-- parameters: {params!r}\n\n{plan}\n"
                )
            (out / "plans" / f"{name}.txt").write_text("\n\n".join(plans))

        record = {
            "median_ms": statistics.median(times) * 1000,
            "min_ms": min(times) * 1000,
            "statements": len(statements),
            # A count, the number of results, or -1 if not visible (404).
            "results": n_results(results),
        }
        summary["scenarios"][name] = record
        shown = "not visible" if results is None else f"{record['results']:6} results"
        print(
            f"  {name:32} {record['median_ms']:10.1f} ms   "
            f"{record['statements']:3} stmts   {shown}"
        )

    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"Wrote {out / 'summary.json'}")
    for task in root.shutdown_tasks:
        await task()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--uri", default=DEFAULT_URI)
    parser.add_argument("--label", required=True, help="e.g. base or branch")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument(
        "--out", default=str(Path(__file__).parent / "results"), help="Output dir."
    )
    parser.add_argument(
        "--only", nargs="*", help="Run only scenarios whose name contains any of these."
    )
    parser.add_argument(
        "--no-explain", action="store_true", help="Skip EXPLAIN ANALYZE capture."
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
