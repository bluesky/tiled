"""
Serialization of arrays and tables, without the server.

These isolate the encoding cost that `read` benchmarks pay on top of the
database and HTTP round-trips. They need no PostgreSQL.
"""

import asyncio

import numpy
import pandas
import pytest

from tiled.media_type_registration import (
    default_deserialization_registry,
    default_serialization_registry,
)
from tiled.structures.core import StructureFamily

from .conftest import sample_table

pytestmark = pytest.mark.benchmark(group="serialization")

ARRAY = numpy.random.default_rng(0).random((1024, 1024))
TABLE = sample_table(100_000)


def serialize(family, media_type, payload, metadata=None):
    serializer = default_serialization_registry.dispatch(family, media_type)
    result = serializer(media_type, payload, metadata or {})
    if asyncio.iscoroutine(result):
        result = asyncio.run(result)
    return result


@pytest.mark.parametrize("media_type", ["application/octet-stream", "text/csv"])
def bench_serialize_array(benchmark, media_type):
    benchmark(serialize, StructureFamily.array, media_type, ARRAY)


@pytest.mark.parametrize(
    "media_type",
    ["application/vnd.apache.arrow.file", "application/x-parquet", "text/csv"],
)
def bench_serialize_table(benchmark, media_type):
    benchmark(serialize, StructureFamily.table, media_type, TABLE)


def bench_deserialize_array_octet_stream(benchmark):
    payload = serialize(StructureFamily.array, "application/octet-stream", ARRAY)
    deserializer = default_deserialization_registry.dispatch(
        StructureFamily.array, "application/octet-stream"
    )
    benchmark(deserializer, payload, ARRAY.dtype, ARRAY.shape)


def bench_deserialize_table_arrow(benchmark):
    media_type = "application/vnd.apache.arrow.file"
    payload = serialize(StructureFamily.table, media_type, TABLE)
    deserializer = default_deserialization_registry.dispatch(
        StructureFamily.table, media_type
    )
    result = benchmark(deserializer, payload)
    assert isinstance(result, pandas.DataFrame)
