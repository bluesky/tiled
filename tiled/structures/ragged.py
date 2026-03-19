from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast, runtime_checkable

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import awkward
import numpy as np
import ragged
from ragged._typing import SupportsDLPack

from tiled.structures.array import BuiltinDtype, StructDtype
from tiled.structures.root import Structure

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


@dataclass(kw_only=True)
class RaggedStructure(Structure):
    data_type: BuiltinDtype | StructDtype
    """Serializable representation of the array's data type"""
    shape: tuple[int | None, ...]
    """The shape of the array, where the first dimension is always a known integer,
    and any variable dimensions are represented by None."""
    size: int
    """The total number of elements in the array."""
    partitions: tuple[int, ...]
    """Defines the boundaries for partitioning the array.
    Note that the final value is the row count from `shape[0]`."""
    dims: tuple[str, ...] | None = None  # None or tuple of names like ("x", "y")
    """Optional tuple of dimension names."""
    resizable: bool | tuple[bool, ...] = False

    @property
    def npartitions(self) -> int:
        # partitions are of the form (0, [i1, ..., iN], shape[0]), so subtract by 1
        return len(self.partitions) - 1

    @classmethod
    def from_array(
        cls,
        array: Iterable,
        shape: tuple[int | None, ...] | None = None,
        partitions: tuple[int, ...] | None = None,
        dims: tuple[str, ...] | None = None,
    ) -> Self:
        """
        Construct a RaggedStructure from an array-like object.

        Parameters
        ----------
        array : Iterable
            The array-like object to extract information from.
        shape : tuple[int | None, ...] | None, optional
            The shape of the array. If None, the shape is inferred from the array.
        partitions : tuple[int, ...] | None, optional
            Defines the boundaries for partitioning the array.
            This defaults to ``(0, shape[0])`` for a single partition.
        dims : tuple[str, ...] | None, optional
            The names of the dimensions.
        """
        array = make_ragged_array(array)

        if shape is None:
            shape = array.shape

        size = cast("int", array.size)

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
        """For internal use. Construct a RaggedStructure from a dictionary mapping."""
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


_SupportsDLPack = runtime_checkable(cast("type[SupportsDLPack]", SupportsDLPack))


def make_ragged_array(array: Iterable) -> ragged.array:
    """Best-effort conversion of any numeric iterable to a ``ragged`` array."""
    if isinstance(array, ragged.array):
        return array
    if isinstance(array, np.ndarray):
        # this assumes that any nested-arrays do *not* have an object dtype
        if array.dtype.name == "object":
            return ragged.array([row.tolist() for row in array])
        return ragged.array(array)
    if isinstance(array, (awkward.Array, _SupportsDLPack)):
        return ragged.array(array)
    if hasattr(array, "tolist"):
        return ragged.array(array.tolist())
    return ragged.array(list(array))


def make_ragged_partitions(array: ragged.array, limit_bytes: int) -> tuple[int, ...]:
    """Row-wise partitioning of a ragged array into blocks of at most ``limit_bytes`` bytes."""
    ak_array = awkward.Array(array._impl)  # noqa: SLF001
    if ak_array.nbytes <= limit_bytes:
        return (0, len(ak_array))

    partitions: list[int] = [0, cast("int", array.shape[0])]
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
