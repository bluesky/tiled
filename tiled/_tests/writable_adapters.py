import uuid

import dask
import numpy

from ..adapters.array import ArrayAdapter, slice_and_shape_from_block_and_chunks
from ..adapters.dataframe import DataFrameAdapter
from ..adapters.mapping import MapAdapter
from ..adapters.sparse import COOAdapter
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


class WritableCOOAdapter(COOAdapter):
    def put_data(self, body, block=None):
        if not block:
            block = (0,) * len(self.shape)
        df = deserialize_arrow(body)
        coords = df[df.columns[:-1]].values.T
        data = df["data"].values
        self.blocks[block] = (coords, data)


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
        elif structure_family == StructureFamily.sparse:
            self._mapping[key] = WritableCOOAdapter(
                {},
                shape=structure.shape,
                chunks=structure.chunks,
                metadata=metadata,
                specs=specs,
            )
        else:
            raise NotImplementedError(structure_family)
        return key
