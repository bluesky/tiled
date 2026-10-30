"""
Writing nodes and data.

Every benchmark writes into a fresh `scratch` container and so runs in
`pedantic` mode: one call per round, with a unique key generated in `setup`,
so that the catalog grows by at most a few nodes per round.
"""

import uuid

import numpy
import pandas
import pyarrow
import pytest

from .conftest import sample_table

pytestmark = pytest.mark.benchmark(group="write")

ROUNDS = 20


def unique_key():
    return (uuid.uuid4().hex,), {}


def bench_create_container(benchmark, scratch):
    benchmark.pedantic(
        lambda key: scratch.create_container(key=key, metadata={"number": 1}),
        setup=unique_key,
        warmup_rounds=1,
        rounds=ROUNDS,
    )


def bench_write_array_small(benchmark, scratch):
    "Write a 100x100 float64 array (80 kB): catalog node + one zarr chunk."
    array = numpy.ones((100, 100))
    benchmark.pedantic(
        lambda key: scratch.write_array(array, key=key),
        setup=unique_key,
        warmup_rounds=1,
        rounds=ROUNDS,
    )


def bench_write_array_large(benchmark, scratch):
    "Write a 1024x1024 float64 array (8 MB)."
    array = numpy.random.default_rng(1).random((1024, 1024))
    benchmark.pedantic(
        lambda key: scratch.write_array(array, key=key),
        setup=unique_key,
        warmup_rounds=1,
        rounds=5,
    )


def bench_write_table(benchmark, scratch):
    "Write a 10,000-row table (Parquet)."
    table = sample_table(10_000)
    benchmark.pedantic(
        lambda key: scratch.write_table(table, key=key),
        setup=unique_key,
        warmup_rounds=1,
        rounds=ROUNDS,
    )


def bench_append_table(benchmark, scratch):
    "Create an appendable (SQL-backed) table and append 10 batches of 1,000 rows."
    batch = pyarrow.Table.from_pandas(sample_table(1_000), preserve_index=False)

    def create_and_append(key):
        node = scratch.create_appendable_table(batch.schema, key=key)
        for _ in range(10):
            node.append_partition(0, batch)

    benchmark.pedantic(create_and_append, setup=unique_key, warmup_rounds=1, rounds=5)


def bench_update_metadata(benchmark, scratch):
    "Patch the metadata of one node (creates a revision each time)."
    node = scratch.create_container(key="node", metadata={"counter": 0})
    counter = iter(range(10**6))
    benchmark.pedantic(
        lambda: node.update_metadata(metadata={"counter": next(counter)}),
        warmup_rounds=1,
        rounds=ROUNDS,
    )


def bench_write_dataframe_wide(benchmark, scratch):
    "Write a 1,000-row table with 100 columns."
    table = pandas.DataFrame(
        numpy.random.default_rng(2).random((1_000, 100)),
        columns=[f"c{i}" for i in range(100)],
    )
    benchmark.pedantic(
        lambda key: scratch.write_table(table, key=key),
        setup=unique_key,
        warmup_rounds=1,
        rounds=ROUNDS,
    )
