from __future__ import annotations

import contextlib
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import awkward
import dask_awkward
import numpy as np
import ragged
from awkward.contents import ListArray, ListOffsetArray
from dask_awkward.lib.core import calculate_known_divisions

from tiled.structures.array import BuiltinDtype, StructDtype
from tiled.structures.root import Structure

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

OffsetArrayType = list[int]
"""Represents a list of offsets for ``awkward.contents.ListOffsetArray`` layouts."""
StartAndStopArraysType = tuple[list[int], list[int]]
"""Represents a pair of lists, ``[starts, stops]``, for ``awkward.contents.ListArray`` layouts.

While ``ListArray`` is convertible to ``ListOffsetArray``, we need this to retain information
when slicing and dicing ragged arrays.
"""


@dataclass(kw_only=True)
class RaggedStructure(Structure):
    data_type: BuiltinDtype | StructDtype
    shape: tuple[int | None, ...]
    offsets: list[OffsetArrayType | StartAndStopArraysType]
    size: int
    partitions: tuple[int, ...]
    dims: tuple[str, ...] | None = None  # None or tuple of names like ("x", "y")
    resizable: bool | tuple[bool, ...] = False

    @property
    def npartitions(self) -> int:
        # partitions are of the form (0, [i1, ..., iN], size), so subtract by 1
        return len(self.partitions) - 1

    @classmethod
    def from_array(
        cls,
        array: Iterable,
        shape: tuple[int | None, ...] | None = None,
        partitions: tuple[int, ...] | None = None,
        dims: tuple[str, ...] | None = None,
    ) -> Self:
        array = make_ragged_array(array)

        if shape is None:
            shape = array.shape

        size = int(array.size)  # should never not be an int

        if partitions is None:
            # default to a single partition containing the whole array
            partitions = (0, size)

        if array.dtype.fields is not None:
            data_type = StructDtype.from_numpy_dtype(array.dtype)
        else:
            data_type = BuiltinDtype.from_numpy_dtype(array.dtype)

        offsets: list[OffsetArrayType | StartAndStopArraysType] = []

        content = array._impl  # noqa: SLF001
        if hasattr(content, "layout"):
            content = content.layout

        while isinstance(content, (ListOffsetArray, ListArray)):
            if isinstance(content, ListOffsetArray):
                offsets.append(np.array(content.offsets).tolist())
            if isinstance(content, ListArray):
                start = np.array(content.starts).tolist()
                stop = np.array(content.stops).tolist()
                offsets.append([start, stop])
            content = content.content

        return cls(
            data_type=data_type,
            shape=shape,
            dims=dims,
            resizable=False,
            offsets=offsets,
            size=size,
            partitions=partitions,
        )

    @classmethod
    def from_json(cls, structure: Mapping[str, Any]) -> Self:
        if "fields" in structure["data_type"]:
            data_type = StructDtype.from_json(structure["data_type"])
        else:
            data_type = BuiltinDtype.from_json(structure["data_type"])
        dims = structure["dims"]
        if dims is not None:
            dims = tuple(dims)
        return cls(
            data_type=data_type,
            shape=tuple(structure["shape"]),
            dims=dims,
            resizable=structure.get("resizable", False),
            offsets=structure.get("offsets", []),
            size=structure["size"],
            partitions=tuple(structure.get("partitions", (0, structure["size"]))),
        )


def make_ragged_array(array: Iterable) -> ragged.array:
    """Best-effort conversion of any numeric iterable to a ``ragged`` array."""
    if isinstance(array, ragged.array):
        return array
    if isinstance(array, np.ndarray):
        # this assumes that any nested-arrays do *not* have an object dtype
        if array.dtype.name == "object":
            return ragged.array([row.tolist() for row in array])
        return ragged.array(array)
    if isinstance(array, awkward.Array) or hasattr(array, "__dlpack_device__"):
        with contextlib.suppress(ValueError):
            # this tries to regularize the array dimensions if possible, to
            # reduce unneeded nulls in the resulting shape.
            return ragged.array(awkward.to_numpy(array))
        return ragged.array(array)
    if hasattr(array, "tolist"):
        return ragged.array(array.tolist())
    return ragged.array(list(array))


def make_ragged_partitions(array: ragged.array, limit_bytes: int) -> tuple[int, ...]:
    """Row-wise partitioning of a ragged array into blocks of at most ``limit_bytes`` bytes."""
    ak_array = awkward.Array(array._impl)  # noqa: SLF001
    if ak_array.nbytes <= limit_bytes:
        return (0, len(ak_array))

    dak_array = dask_awkward.from_awkward(source=ak_array, npartitions=1)
    partition_index = 0

    while partition_index < dak_array.npartitions:
        part = dask_awkward.to_packed(dak_array.partitions[partition_index]).compute()
        part_rows = len(part)
        if part.nbytes > limit_bytes:
            if part_rows == 1:
                # We can't partition more finely than this
                msg = f"cannot partition individual rows to fit within {limit_bytes} bytes"
                raise ValueError(msg)
            divisions = list(dak_array.divisions)
            next_division = divisions[partition_index] + int(part_rows / 2)
            divisions.insert(partition_index + 1, next_division)
            dak_array = dak_array.repartition(divisions=tuple(divisions))
        else:
            partition_index += 1

    if not dak_array.known_divisions:
        return calculate_known_divisions(dak_array)
    return dak_array.defined_divisions
