"Reading arrays, tables, and metadata."

import pytest

pytestmark = pytest.mark.benchmark(group="read")


@pytest.fixture(scope="module")
def array(client):
    return client["array"]


@pytest.fixture(scope="module")
def table(client):
    return client["table"]


def bench_lookup(benchmark, client):
    "Fetch one node by key (one metadata request)."
    benchmark(lambda: client["small"]["node_000500"])


def bench_lookup_deep(benchmark, client):
    "Fetch a node five levels down by path (one request per level)."
    benchmark(
        lambda: client["tree"]["level5_0"]["level4_0"]["level3_0"]["level2_0"][
            "level1_0"
        ]
    )


def bench_read_array_full(benchmark, array):
    benchmark(array.read)


def bench_read_array_slice(benchmark, array):
    "Read a 64x64 corner of the array (one block, sliced server-side)."
    benchmark(lambda: array[:64, :64])


def bench_read_array_block(benchmark, array):
    benchmark(array.read_block, (0, 0))


def bench_read_table_full(benchmark, table):
    benchmark(table.read)


def bench_read_table_columns(benchmark, table):
    benchmark(table.read, ["a", "b"])


def bench_list_keys(benchmark, client):
    "List the keys of a SMALL container, paginated."
    benchmark(lambda: list(client["small"]))
