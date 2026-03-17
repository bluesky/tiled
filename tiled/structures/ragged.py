from __future__ import annotations

import contextlib
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast


if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import awkward
import numpy as np
import ragged

from tiled.structures.array import BuiltinDtype, StructDtype
from tiled.structures.root import Structure

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


@dataclass(kw_only=True)
class RaggedStructure(Structure):
    data_type: BuiltinDtype | StructDtype
    shape: tuple[int | None, ...]
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
            partitions = (0, cast("int", shape[0]))

        if array.dtype.fields is not None:
            data_type = StructDtype.from_numpy_dtype(array.dtype)
        else:
            data_type = BuiltinDtype.from_numpy_dtype(array.dtype)

        return cls(
            data_type=data_type,
            shape=shape,
            dims=dims,
            resizable=False,
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

    partitions = [0, array.shape[0]]
    partition_index = 0

    while partition_index < len(partitions) - 1:
        start, end = partitions[partition_index], partitions[partition_index + 1]
        part = awkward.to_packed(ak_array[start:end])
        if part.nbytes > limit_bytes:
            if end - start == 1:
                # We can't partition more finely than this
                msg = f"cannot partition individual rows to fit within {limit_bytes} bytes"
                raise ValueError(msg)
            next_partition = start + (end - start) // 2
            partitions.insert(partition_index + 1, next_partition)
        else:
            partition_index += 1
    return tuple(partitions)
