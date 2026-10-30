"Bulk operations on subtrees, and server and client startup."

import numpy
import pytest

from tiled.catalog import from_uri
from tiled.client import Context, from_context
from tiled.server.app import build_app

from .conftest import flat_metadata

pytestmark = pytest.mark.benchmark(group="tree")


def populate_flat(container, n):
    "Create `n` children with `flat_metadata`, one request each."
    for i in range(n):
        container.create_container(key=f"node_{i:06d}", metadata=flat_metadata(i))


def populate_tree(container, breadth, depth):
    "Create a complete tree, `breadth` children per node, `depth` levels."
    if depth == 0:
        return
    for i in range(breadth):
        child = container.create_container(
            key=f"level{depth}_{i}", metadata={"level": depth, "index": i}
        )
        populate_tree(child, breadth, depth - 1)


def bench_create_100_containers(benchmark, scratch):
    "Create 100 flat children, one request each."
    counter = iter(range(10**6))
    benchmark.pedantic(
        lambda parent: populate_flat(parent, 100),
        setup=lambda: ((scratch.create_container(key=f"flat_{next(counter)}"),), {}),
        warmup_rounds=1,
        rounds=5,
    )


def bench_create_tree(benchmark, scratch):
    "Create a complete tree of 3 children per node, 4 levels (120 nodes)."
    counter = iter(range(10**6))
    benchmark.pedantic(
        lambda parent: populate_tree(parent, breadth=3, depth=4),
        setup=lambda: ((scratch.create_container(key=f"tree_{next(counter)}"),), {}),
        warmup_rounds=1,
        rounds=5,
    )


def bench_delete_subtree(benchmark, scratch):
    "Recursively delete a 120-node subtree with 27 small arrays at the leaves."

    def make_subtree():
        root = scratch.create_container(key=f"del_{next(counter)}")
        populate_tree(root, breadth=3, depth=3)
        for level3 in root.values():
            for level2 in level3.values():
                for level1 in level2.values():
                    level1.write_array(numpy.ones(10), key="data")
        return (root.item["id"],), {}

    counter = iter(range(10**6))
    benchmark.pedantic(
        lambda key: scratch.delete_contents(key, recursive=True, external_only=False),
        setup=make_subtree,
        warmup_rounds=1,
        rounds=5,
    )


def bench_server_startup(benchmark, server_uri, tmp_path):
    "Build the app on the populated catalog and run its lifespan."

    def start_and_stop():
        # A distinct pool size keys a connection pool of its own: the running
        # server's pool is bound to another event loop.
        catalog = from_uri(
            server_uri, writable_storage=str(tmp_path), catalog_pool_size=3
        )
        with Context.from_app(build_app(catalog)):
            pass

    benchmark.pedantic(start_and_stop, warmup_rounds=1, rounds=5)


def bench_client_connect(benchmark, context):
    "Connect a client to a running server (fetches the root node)."
    benchmark(from_context, context)
