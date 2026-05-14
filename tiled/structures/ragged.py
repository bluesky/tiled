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
    """A structure representing a ragged array

    Ragged arrays are arrays with variable-length trailing dimensions (rows). The first
    dimension is always a known integer, while any variable dimensions are represented
    by None in its shape.

    Parameters
    ----------
    data_type : BuiltinDtype | StructDtype
        Serializable representation of the array's data type.
    shape : tuple[int | None, ...]
        The shape of the array, where the first dimension is always a known integer,
        and any variable dimensions are represented by None.
    size : int
        The total number of elements in the array.
    chunks : tuple[tuple[int, ...] | None, ...]
        The dask-like chunks of the array, where the first dimension is always
        partitioned into known integer chunks, and any variable dimensions are null.
    dims : tuple[str, ...] | None, optional
        Optional tuple of dimension names, e.g. ("time", "x"), or None for unnamed dimensions.
    resizable : bool | tuple[bool, ...], optional
        Whether the array is resizable along any dimension.
    """

    data_type: BuiltinDtype | StructDtype
    shape: tuple[int | None, ...]
    size: int
    chunks: tuple[tuple[int, ...] | None, ...]
    dims: tuple[str, ...] | None = None
    resizable: bool | tuple[bool, ...] = False

    @classmethod
    def from_array(
        cls,
        array: Iterable,
        shape: tuple[int | None, ...] | None = None,
        chunks: tuple[tuple[int, ...] | None, ...] | None = None,
        dims: tuple[str, ...] | None = None,
    ) -> Self:
        """Construct a RaggedStructure from an array-like object.

        Parameters
        ----------
        array : Iterable
            The array-like object to extract information from.
        shape : tuple[int | None, ...] | None, optional
            The shape of the array. If None, the shape is inferred from the array.
        chunks : tuple[tuple[int, ...] | None, ...] | None, optional
            Defines the boundaries for partitioning the array, i.e. row counts for each chunk.
            If not given, the array is partitioned into a single chunk along the first dimension.
        dims : tuple[str, ...] | None, optional
            The names of the dimensions.
        """

        array = make_ragged_array(array)
        shape = shape or array.shape
        chunks = chunks or ((shape[0],),) + (None,) * (len(shape) - 1)

        if array.dtype.fields is not None:
            data_type = StructDtype.from_numpy_dtype(array.dtype)
        else:
            data_type = BuiltinDtype.from_numpy_dtype(array.dtype)

        return cls(
            data_type=data_type,
            shape=shape,
            size=array.size,
            chunks=chunks,
            dims=dims,
        )

    @classmethod
    def from_json(cls, structure: Mapping[str, Any]) -> Self:
        """Construct a RaggedStructure from a dictionary mapping.
        
        For internal use.
        """

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
            size=structure["size"],
            chunks=tuple(map(tuple, structure["chunks"])),
            dims=dims,
            resizable=structure.get("resizable", False),
        )


_SupportsDLPack = runtime_checkable(cast("type[SupportsDLPack]", SupportsDLPack))


def make_ragged_array(array: Iterable) -> ragged.array:
    """Best-effort conversion of any numeric iterable to a `ragged` array."""
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


def make_ragged_chunks(array: ragged.array, limit_bytes: int) -> tuple[int, ...]:
    """Row-wise partitioning of a ragged array into chunks of at most `limit_bytes` bytes."""
    ak_array = awkward.Array(array._impl)
    if ak_array.nbytes <= limit_bytes:
        return (len(ak_array),)

    # Work with boundary indices internally, convert to sizes at the end.
    boundaries: list[int] = [0, cast("int", array.shape[0])]
    partition_index = 0

    while partition_index < len(boundaries) - 1:
        start, end = boundaries[partition_index], boundaries[partition_index + 1]
        part = awkward.to_packed(ak_array[start:end])
        if part.nbytes > limit_bytes:
            if end - start == 1:
                msg = f"cannot partition individual rows to fit within {limit_bytes} bytes"
                raise ValueError(msg)
            mid = start + (end - start) // 2
            boundaries.insert(partition_index + 1, mid)
        else:
            partition_index += 1

    return tuple(end - start for start, end in zip(boundaries, boundaries[1:]))
