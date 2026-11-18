"""Fixtures for the benchmark suite.

One server, backed by one PostgreSQL catalog, is built for the whole session
and populated once (see `populated`). Benchmarks read from it, or write under
keys of their own that they remove afterwards, so that the populated
collections keep their sizes.

Most tests in the suite require `TILED_TEST_POSTGRESQL_URI`; they are skipped otherwise.
"""

import asyncio
import os
import uuid

import numpy
import pandas
import pytest

from tests.utils import temp_postgres_sync
from tiled.catalog import from_uri
from tiled.client import Context, from_context
from tiled.server.app import build_app
from tiled.structures.core import StructureFamily

TILED_TEST_POSTGRESQL_URI = os.getenv("TILED_TEST_POSTGRESQL_URI")

# pytest-benchmark (up to 5.3.0) reads the private `_nodeid` attribute of
# test items, which pytest 9.2 removed in favor of the public `nodeid`.
# Remove once pytest-benchmark ships the fix
# (https://github.com/ionelmc/pytest-benchmark/pull/323).
if not hasattr(pytest.Item, "_nodeid"):
    pytest.Item._nodeid = property(lambda self: self.nodeid)

# Sizes of the populated collections. Benchmarks refer to these by name, so
# that the dashboard series keep their identity when a size is tuned.
SMALL = 1_000
LARGE = 10_000

ARRAY_SHAPE = (1024, 1024)  # 8 MB of float64
TABLE_ROWS = 100_000

COLORS = ["red", "green", "blue"]


@pytest.fixture(scope="session")
def server_uri():
    "URI of a fresh PostgreSQL catalog database for the session."
    if not TILED_TEST_POSTGRESQL_URI:
        pytest.skip("Benchmarks require TILED_TEST_POSTGRESQL_URI")
    with temp_postgres_sync(TILED_TEST_POSTGRESQL_URI) as uri:
        yield uri


def flat_metadata(i):
    """
    Metadata of the i-th child of a flat container.

    `number` is unique, `color` takes three values, `label` is a phrase for
    full-text search, and `nested.depth` is a nested key.
    """
    return {
        "number": i,
        "color": COLORS[i % 3],
        "label": f"sample {i} measured in {COLORS[i % 3]} light",
        "nested": {"depth": i % 10},
    }


async def create_containers(parent, keys_and_metadata, batch=50):
    "Create child containers of the adapter `parent`, `batch` at a time."
    children = []
    items = list(keys_and_metadata)
    for start in range(0, len(items), batch):
        children.extend(
            await asyncio.gather(
                *(
                    parent.create_node(
                        StructureFamily.container, metadata, key=key, specs=[]
                    )
                    for key, metadata in items[start : start + batch]
                )
            )
        )
    return children


async def create_tree(parent, breadth, depth):
    "Create a complete tree, `breadth` children per node, `depth` levels."
    if depth == 0:
        return
    children = await create_containers(
        parent,
        ((f"level{depth}_{i}", {"level": depth, "index": i}) for i in range(breadth)),
    )
    for child in children:
        await create_tree(child, breadth, depth - 1)


@pytest.fixture(scope="session")
def populated(server_uri, tmp_path_factory):
    """
    Populate the catalog, once per session, directly through the catalog
    adapter (no HTTP). Contents:

    - `small`: container of SMALL children (see `flat_metadata`)
    - `large`: container of LARGE children
    - `tree`: complete tree, 4 children per node, 5 levels (1364 nodes)
    - `scratch`: empty container for the write benchmarks

    The adapter used here gets a connection pool of its own (by a distinct
    pool size): a pool is bound to the event loop it was created on, and the
    server runs on another one.
    """

    async def populate():
        catalog = from_uri(
            server_uri,
            writable_storage=str(tmp_path_factory.mktemp("populate")),
            init_if_not_exists=True,
            catalog_pool_size=4,
        )
        await catalog.startup()
        try:
            small, large, tree, _ = await create_containers(
                catalog,
                [("small", {}), ("large", {}), ("tree", {}), ("scratch", {})],
            )
            await create_containers(
                small, ((f"node_{i:06d}", flat_metadata(i)) for i in range(SMALL))
            )
            await create_containers(
                large, ((f"node_{i:06d}", flat_metadata(i)) for i in range(LARGE))
            )
            await create_tree(tree, breadth=4, depth=5)
        finally:
            await catalog.shutdown()

    asyncio.run(populate())


@pytest.fixture(scope="session")
def context(server_uri, populated, tmp_path_factory):
    "The server. The PostgreSQL database doubles as the tabular storage."
    catalog = from_uri(
        server_uri,
        writable_storage=[str(tmp_path_factory.mktemp("data")), server_uri],
    )
    with Context.from_app(build_app(catalog)) as context:
        yield context


def sample_table(rows):
    rng = numpy.random.default_rng(0)
    return pandas.DataFrame(
        {
            "a": rng.random(rows),
            "b": rng.integers(0, 1000, rows),
            "c": rng.random(rows),
            "label": numpy.array(["x", "y", "z"])[rng.integers(0, 3, rows)],
        }
    )


@pytest.fixture(scope="session")
def client(context):
    """
    Client on the populated catalog (see `populated`), plus:

    - `array`: float64 array of ARRAY_SHAPE
    - `table`: table of TABLE_ROWS rows, 4 columns
    """
    client = from_context(context)
    client.write_array(numpy.random.default_rng(0).random(ARRAY_SHAPE), key="array")
    client.write_table(sample_table(TABLE_ROWS), key="table")
    return client


@pytest.fixture
def scratch(client):
    "An empty container, deleted after the benchmark, for write benchmarks."
    container = client["scratch"].create_container(key=uuid.uuid4().hex)
    yield container
    client["scratch"].delete_contents(
        container.item["id"], recursive=True, external_only=False
    )
