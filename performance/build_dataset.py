"""
Build a PostgreSQL catalog for benchmarking access-tag filtering.

The schema is created by tiled itself (as `tiled catalog init` does); the rows
are then bulk-inserted with SQL, which is far faster than going through the
API. The database triggers that maintain `nodes_closure` and
`node_access_tags_association.parent_id` fire as usual.

Layout (all containers sit directly under the root):

    inherit_true/   tagged 'bench_user'. Every child tagged grp_<i % groups>.
    inherit_false/  tagged 'other'.      Every child tagged grp_<i % groups>.
    half_untagged/  tagged 'bench_user'. Even children untagged, odd ones
                    tagged grp_<i % groups>.
    half_match/     like half_untagged, but the even children are tagged
                    'grp_0' instead of untagged. Every version sees the same
                    children here as the branch sees in half_untagged/, so it
                    is the fair reference for it.
    deep/           tagged 'bench_user'. A tree `--deep-levels` deep with
                    `--deep-fanout` children per node. At level 1, every 10th
                    node is tagged 'other' (a hidden subtree); the rest are
                    untagged. Below that, even nodes are untagged and odd ones
                    tagged grp_<i % groups>.
    deep_tagged/    like deep/, but the untagged nodes are tagged 'grp_0': the
                    fair reference for deep/.
    chain/c/c/c/c   five nested containers, all tagged 'bench_user': a path
                    the user can walk on both main and this branch.
    filler/         tagged 'other'. `--filler` children tagged grp_*, to make
                    the tables closer to production size.

The benchmark user holds tags ['bench_user', 'grp_0'] (see bench_sql.py), so:

    inherit_true   the search root's inheritable tags match the user
    inherit_false  they do not: this is the case `main` already handled
    half_untagged  inheritance applies to half the children

Every child has metadata {"color": "red"|"blue", "i": <int>}.

Build one database per version being compared (run_ab.sh does this), since
each version's code requires its own schema revision. The database in `--uri`
is created if it does not exist.

Usage:

    python performance/build_dataset.py --uri postgresql+asyncpg://tiled:tiled@localhost:5433/tiled_perf --reset
"""

import argparse
import asyncio
import time

import asyncpg

from tiled.catalog.core import initialize_catalog_database

DEFAULT_URI = "postgresql+asyncpg://tiled:tiled@localhost:5433/tiled_perf"
USER_TAGS = ["bench_user", "grp_0"]


def plain_dsn(uri):
    "asyncpg wants postgresql://, not postgresql+asyncpg://."
    return uri.replace("postgresql+asyncpg://", "postgresql://", 1)


class Timer:
    def __init__(self, label):
        self.label = label

    def __enter__(self):
        print(f"  {self.label} ...", end="", flush=True)
        self.start = time.perf_counter()

    def __exit__(self, *exc):
        print(f" {time.perf_counter() - self.start:.1f}s")


async def make_container(conn, parent, key, tag):
    node_id = await conn.fetchval(
        """
        INSERT INTO nodes (parent, key, structure_family, metadata, specs)
        VALUES ($1, $2, 'container', '{}'::jsonb, '[]'::jsonb)
        RETURNING id
        """,
        parent,
        key,
    )
    if tag is not None:
        await tag_nodes(conn, "n.id = $1", [node_id], f"'{tag}'")
    return node_id


async def insert_children(conn, parent, count):
    """Insert `count` empty containers under `parent`, keyed n0..n<count-1>."""
    await conn.execute(
        """
        INSERT INTO nodes (parent, key, structure_family, metadata, specs)
        SELECT $1,
               'n' || g,
               'container',
               jsonb_build_object(
                   'color', CASE WHEN g % 2 = 0 THEN 'blue' ELSE 'red' END,
                   'i', g),
               '[]'::jsonb
        FROM generate_series(0, $2 - 1) AS g
        """,
        parent,
        count,
    )


async def tag_nodes(conn, where, args, tag_name_sql):
    """Tag the nodes matching `where` with the tag named by `tag_name_sql`.

    `tag_name_sql` is a SQL expression over `n` (the nodes row).
    """
    await conn.execute(
        f"""
        INSERT INTO node_access_tags_association (node_id, tag_id)
        SELECT n.id, t.id
        FROM nodes AS n
        JOIN access_tags AS t ON t.name = {tag_name_sql}
        WHERE {where}
        """,
        *args,
    )


def group_tag(groups):
    return f"'grp_' || (((n.metadata->>'i')::int) % {int(groups)})"


async def create_database_if_missing(uri):
    dsn = plain_dsn(uri)
    base, _, name = dsn.rpartition("/")
    conn = await asyncpg.connect(base + "/postgres")
    exists = await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", name)
    if not exists:
        await conn.execute(f'CREATE DATABASE "{name}"')
        print(f"Created database {name}")
    await conn.close()


async def build_tree(conn, key, fanout, levels, groups, fill_tag):
    """A container `levels` deep with `fanout` children per node.

    The nodes that deep/ leaves untagged are tagged `fill_tag`, if given.
    """
    root = await make_container(conn, 0, key, "bench_user")
    with Timer("insert"):
        parents = [root]
        for level in range(1, levels + 1):
            for p in parents:
                await insert_children(conn, p, fanout)
            parents = [
                r["descendant"]
                for r in await conn.fetch(
                    "SELECT descendant FROM nodes_closure "
                    "WHERE ancestor = $1 AND depth = $2",
                    root,
                    level,
                )
            ]
    level_1 = "n.parent = $1"
    below = (
        "n.id IN (SELECT descendant FROM nodes_closure "
        "         WHERE ancestor = $1 AND depth >= 2)"
    )
    is_i = "((n.metadata->>'i')::int)"
    with Timer("tag"):
        # Level 1: every 10th node hides its subtree; the rest are untagged.
        await tag_nodes(conn, f"{level_1} AND {is_i} % 10 = 0", [root], "'other'")
        # Deeper levels: odd nodes tagged, even nodes untagged.
        await tag_nodes(conn, f"{below} AND {is_i} % 2 = 1", [root], groups)
        if fill_tag is not None:
            await tag_nodes(
                conn, f"{level_1} AND {is_i} % 10 <> 0", [root], f"'{fill_tag}'"
            )
            await tag_nodes(
                conn, f"{below} AND {is_i} % 2 = 0", [root], f"'{fill_tag}'"
            )


async def build(args):
    await create_database_if_missing(args.uri)
    if args.reset:
        conn = await asyncpg.connect(plain_dsn(args.uri))
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        await conn.close()

    print("Initializing catalog schema with tiled")
    initialize_catalog_database(args.uri)

    conn = await asyncpg.connect(plain_dsn(args.uri))
    existing = await conn.fetchval("SELECT count(*) FROM nodes WHERE id <> 0")
    if existing:
        raise SystemExit(
            f"The catalog already has {existing} nodes. Pass --reset to rebuild."
        )

    print("Creating tags")
    names = ["bench_user", "other"] + [f"grp_{i}" for i in range(args.groups)]
    await conn.executemany(
        "INSERT INTO access_tags (name) VALUES ($1)", [(n,) for n in names]
    )

    groups = group_tag(args.groups)

    print(f"inherit_true: {args.children} children")
    parent = await make_container(conn, 0, "inherit_true", "bench_user")
    with Timer("insert"):
        await insert_children(conn, parent, args.children)
    with Timer("tag"):
        await tag_nodes(conn, "n.parent = $1", [parent], groups)

    print(f"inherit_false: {args.children} children")
    parent = await make_container(conn, 0, "inherit_false", "other")
    with Timer("insert"):
        await insert_children(conn, parent, args.children)
    with Timer("tag"):
        await tag_nodes(conn, "n.parent = $1", [parent], groups)

    print(f"half_untagged: {args.children} children")
    parent = await make_container(conn, 0, "half_untagged", "bench_user")
    with Timer("insert"):
        await insert_children(conn, parent, args.children)
    with Timer("tag"):
        await tag_nodes(
            conn,
            "n.parent = $1 AND ((n.metadata->>'i')::int) % 2 = 1",
            [parent],
            groups,
        )

    print(f"half_match: {args.children} children")
    parent = await make_container(conn, 0, "half_match", "bench_user")
    with Timer("insert"):
        await insert_children(conn, parent, args.children)
    with Timer("tag"):
        await tag_nodes(
            conn,
            "n.parent = $1 AND ((n.metadata->>'i')::int) % 2 = 1",
            [parent],
            groups,
        )
        await tag_nodes(
            conn,
            "n.parent = $1 AND ((n.metadata->>'i')::int) % 2 = 0",
            [parent],
            "'grp_0'",
        )

    fanout, levels = args.deep_fanout, args.deep_levels
    total = sum(fanout**level for level in range(1, levels + 1))
    print(f"deep: {levels} levels x fanout {fanout} = {total} nodes")
    await build_tree(conn, "deep", fanout, levels, groups, fill_tag=None)
    print(f"deep_tagged: {levels} levels x fanout {fanout} = {total} nodes")
    await build_tree(conn, "deep_tagged", fanout, levels, groups, fill_tag="grp_0")

    print("chain: 4 nested containers, all tagged 'bench_user'")
    parent = await make_container(conn, 0, "chain", "bench_user")
    for _ in range(4):
        parent = await make_container(conn, parent, "c", "bench_user")

    if args.filler:
        print(f"filler: {args.filler} children")
        parent = await make_container(conn, 0, "filler", "other")
        with Timer("insert"):
            await insert_children(conn, parent, args.filler)
        with Timer("tag"):
            await tag_nodes(conn, "n.parent = $1", [parent], groups)

    with Timer("VACUUM ANALYZE"):
        # A larger sample than the default (100) keeps the planner's estimates,
        # and so its plans, the same across rebuilds and across the base and
        # branch databases.
        await conn.execute("SET default_statistics_target = 1000")
        await conn.execute("VACUUM ANALYZE")

    counts = await conn.fetchrow(
        """
        SELECT (SELECT count(*) FROM nodes) AS nodes,
               (SELECT count(*) FROM nodes_closure) AS closure,
               (SELECT count(*) FROM node_access_tags_association) AS tag_rows
        """
    )
    await conn.close()
    print(
        f"Done: {counts['nodes']} nodes, {counts['closure']} closure rows, "
        f"{counts['tag_rows']} tag associations."
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--uri", default=DEFAULT_URI)
    parser.add_argument(
        "--children",
        type=int,
        default=200_000,
        help="Children in each of inherit_true, inherit_false and half_untagged.",
    )
    parser.add_argument(
        "--groups",
        type=int,
        default=100,
        help="Number of grp_* tags. The user holds grp_0, so 1/groups match.",
    )
    parser.add_argument("--deep-fanout", type=int, default=20)
    parser.add_argument("--deep-levels", type=int, default=4)
    parser.add_argument(
        "--filler", type=int, default=0, help="Extra nodes to enlarge the tables."
    )
    parser.add_argument(
        "--reset", action="store_true", help="Drop and recreate the schema first."
    )
    asyncio.run(build(parser.parse_args()))


if __name__ == "__main__":
    main()
