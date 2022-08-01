"""
This tests tiled's writing routes with an in-memory store.

Persistent stores are being developed externally to the tiled package.
"""
import uuid

import dask.dataframe
import numpy
import pandas.testing

from ..adapters.array import ArrayAdapter, slice_and_shape_from_block_and_chunks
from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..client import from_tree, record_history
from ..queries import Key
from ..serialization.dataframe import deserialize_arrow
from ..structures.core import StructureFamily


class WritableArrayAdapter(ArrayAdapter):
    def put_data(self, body, block=None):
        macrostructure = self.macrostructure()
        if block is None:
            shape = macrostructure.shape
            slice_ = numpy.s_[:]
        else:
            slice_, shape = slice_and_shape_from_block_and_chunks(
                block, macrostructure.chunks
            )
        array = numpy.frombuffer(
            body, dtype=self.microstructure().to_numpy_dtype()
        ).reshape(shape)
        self._array[slice_] = array


class WritableDataFrameAdapter(DataFrameAdapter):
    def put_data(self, body, partition=0):
        df = deserialize_arrow(body)
        self._partitions[partition] = df


class WritableMapAdapter(MapAdapter):
    def post_metadata(self, metadata, structure_family, structure, specs):
        key = str(uuid.uuid4())
        if structure_family == StructureFamily.array:
            # Initialize an array of zeros, similar to how chunked storage
            # formats (e.g. HDF5, Zarr) use a fill_value.
            array = dask.array.zeros(
                structure.macro.shape,
                dtype=structure.micro.to_numpy_dtype(),
                chunks=structure.macro.chunks,
            )
            self._mapping[key] = WritableArrayAdapter(
                array, metadata=metadata, specs=specs
            )
        elif structure_family == StructureFamily.dataframe:
            # Initialize an empty DataFrame with the right columns/types.
            meta = deserialize_arrow(structure.micro.meta)
            divisions_wrapped_in_df = deserialize_arrow(structure.micro.divisions)
            divisions = tuple(divisions_wrapped_in_df["divisions"].values)
            self._mapping[key] = WritableDataFrameAdapter(
                [None] * structure.macro.npartitions,
                meta=meta,
                divisions=divisions,
                metadata=metadata,
                specs=specs,
            )
        else:
            raise NotImplementedError(structure_family)
        return key


API_KEY = "secret"


def test_write_array_full():

    tree = WritableMapAdapter({})
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    a = numpy.ones((5, 5))

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    with record_history() as history:
        client.write_array(a, metadata, specs)
    # one request for metadata, one for data
    assert len(history.requests) == 2

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_array = result.read()

    numpy.testing.assert_equal(result_array, a)
    assert result.metadata == metadata
    assert result.specs == specs


def test_write_array_chunked():

    tree = WritableMapAdapter({})
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    a = dask.array.arange(2500).reshape((50, 50)).rechunk((20, 20))

    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]
    with record_history() as history:
        client.write_array(a, metadata, specs)
    # one request for metadata, multiple for data
    assert len(history.requests) > 2

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_array = result.read()

    numpy.testing.assert_equal(result_array, a.compute())
    assert result.metadata == metadata
    assert result.specs == specs


def test_write_dataframe_full():

    tree = WritableMapAdapter({})
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    data = {f"Column{i}": (1 + i) * numpy.ones(5) for i in range(5)}
    df = pandas.DataFrame(data)
    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]

    with record_history() as history:
        client.write_dataframe(df, metadata, specs)
    # one request for metadata, one for data
    assert len(history.requests) == 2

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_dataframe = result.read()

    pandas.testing.assert_frame_equal(result_dataframe, df)
    assert result.metadata == metadata
    # TODO In the future this will be accessible via result.specs.
    assert result.item["attributes"]["specs"] == specs


def test_write_dataframe_partitioned():

    tree = WritableMapAdapter({})
    client = from_tree(
        tree, api_key=API_KEY, authentication={"single_user_api_key": API_KEY}
    )

    data = {f"Column{i}": (1 + i) * numpy.ones(10) for i in range(5)}
    df = pandas.DataFrame(data)
    ddf = dask.dataframe.from_pandas(df, npartitions=3)
    metadata = {"scan_id": 1, "method": "A"}
    specs = ["SomeSpec"]

    with record_history() as history:
        client.write_dataframe(ddf, metadata, specs)
    # one request for metadata, multiple for data
    assert len(history.requests) == 1 + 3

    results = client.search(Key("scan_id") == 1)
    result = results.values().first()
    result_dataframe = result.read()

    pandas.testing.assert_frame_equal(result_dataframe, df)
    assert result.metadata == metadata
    # TODO In the future this will be accessible via result.specs.
    assert result.item["attributes"]["specs"] == specs
