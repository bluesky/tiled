from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    NewType,
    Optional,
    Union,
    cast,
    runtime_checkable,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import awkward
import numpy
import ragged
from ragged._typing import SupportsDLPack

from ..ndslice import NDSlice
from ..type_aliases import JSON
from .array import BuiltinDtype, StructDtype
from .root import Structure

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

RaggedCompatibleType = Union[
    ragged.array, awkward.Array, numpy.ndarray, SupportsDLPack, Iterable
]
CanonicalRaggedArray = NewType("CanonicalRaggedArray", ragged.array)


class RaggedSlicingError(ValueError):
    """Raised when an invalid slicing operation is attempted on a ragged array."""

    pass


_SupportsDLPack = runtime_checkable(cast("type[SupportsDLPack]", SupportsDLPack))


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
        From the storage perspective, each chunk represents a row in the underlying table,
        which may contain information about multiple rows of the ragged array.
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

    def __post_init__(self):
        if self.shape[0] is None:
            raise ValueError(
                "The first dimension of a ragged array must be a known integer"
            )
        if self.chunks[0] is None:
            raise ValueError(
                "The first chunks dimension must be a known integer partitioning"
            )
        if len(self.shape) != len(self.chunks):
            raise ValueError("Shape and chunks must have the same number of dimensions")
        for ch in self.chunks[1:]:
            if (ch is not None) and (len(ch) != 1):
                raise ValueError(
                    "Only the first dimension can be partitioned into chunks"
                )

    @classmethod
    def from_array(
        cls,
        array: RaggedCompatibleType,
        shape: tuple[int | None, ...] | None = None,
        chunks: tuple[tuple[int, ...] | None, ...] | None = None,
        dims: tuple[str, ...] | None = None,
    ) -> Self:
        """Construct a RaggedStructure from an array-like object.

        Parameters
        ----------
        array : RaggedCompatibleType
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
        chunks = chunks or tuple((sh,) if sh is not None else None for sh in shape)

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
    def from_json(cls, structure: JSON) -> Self:
        "Construct a RaggedStructure from a dictionary mapping"

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
            chunks=tuple(
                tuple(c) if c is not None else None for c in structure["chunks"]
            ),
            dims=dims,
            resizable=structure.get("resizable", False),
        )

    @property
    def awk_form(self) -> awkward.forms.Form:
        """Construct a canonical Awkward Form representing the ragged array structure

        The Awkward Form is constructed by iterating through the dimensions of the array starting
        from the innermost dimension. The innermost dimension is always represented by a NumpyForm
        with the appropriate primitive type and inner_shape derived by greedily accumulating the
        fixed-size inner dimensions.

        For each subsequent dimension, if it is variable-length (None), a ListOffsetForm is used
        with the appropriate offsets and content. If it is fixed-size, a RegularForm is created.
        """
        primitive = awkward.types.numpytype.dtype_to_primitive(
            self.data_type.to_numpy_dtype()
        )
        inner_shape, form, ndims = (), None, len(self.shape)

        for dim in reversed(range(1, ndims)):
            if self.shape[dim] is None:
                if form is None:
                    # Encountered first inner variable-length dimension, create a NumpyForm
                    form = awkward.forms.NumpyForm(
                        primitive, inner_shape=inner_shape, form_key=f"node{dim}"
                    )
                # Subsequent variable-length dimension, wrap the existing form in a ListOffsetForm
                form = awkward.forms.ListOffsetForm(
                    offsets="i64",
                    content=form,
                    form_key=f"node{dim-1}",
                )

            elif form is not None:
                # Fixed-size dimension, wrap the existing (variable-length) form in a RegularForm
                form = awkward.forms.RegularForm(
                    content=form,
                    size=self.shape[dim],
                    form_key=f"node{dim-1}",
                )

            else:
                # Fixed-size inner dimension, accumulate it into the inner_shape of the NumpyForm
                inner_shape = (self.shape[dim], *inner_shape)

        # If the loop has completed but we haven't created a form yet,
        # which means the array is fully fixed-sized, create a NumpyForm for the entire shape.
        form = form or awkward.forms.NumpyForm(primitive, inner_shape, form_key="node0")

        return form

    @property
    def awk_length(self) -> int:
        "Length of corresponding awkward buffers, i.e. the first dimension of the array"
        return self.shape[0]

    @property
    def ndim_fixed(self) -> int:
        "Number of fixed-size dimensions, i.e. leading dimensions with known integer shape"
        return self.shape.index(None) if None in self.shape else len(self.shape)


def _canonicalize_awkward_layout(
    layout: awkward.contents.Content,
) -> awkward.contents.Content:
    """Make a canonical Awkward layout with only ListOffsetForms for variable-length dimensions.

    Convert an Awkward layout with possibly mixed ListForms and ListOffsetForms into a canonical
    form with only ListOffsetForms for variable-length dimensions. RegularArrays are preserved as
    they are already compatible with the RaggedStructure representation.

    Even though the `ragged` library _may_ support ListForms, ListOffsetForms are preferred and is
    the only officially supported variable-length dimension type for the RaggedStructure.

    Arrays with ListOffsetForm provide several advantages:

    * More compact metadata -- need N+1 offsets instead of N starts and N stops.
    * Canonical representation -- one logical ragged structure maps to a unique offsets array; a
      ListArray can represent the same logical array in many ways.
    * Contiguous partitions of content buffer -- prefix-sum offsets are a standard representation
      used by Arrow, Parquet, etc.
    """

    # Leaf nodes
    if isinstance(layout, awkward.contents.NumpyArray):
        return layout

    # Convert ListArray -> ListOffsetArray
    if isinstance(layout, awkward.contents.ListArray):
        layout = layout.to_ListOffsetArray64()
        content = _canonicalize_awkward_layout(layout.content)
        return awkward.contents.ListOffsetArray(
            layout.offsets,
            content,
            parameters=layout.parameters,
        )

    # Already ListOffsetArray
    if isinstance(layout, awkward.contents.ListOffsetArray):
        content = _canonicalize_awkward_layout(layout.content)

        if content is layout.content:
            return layout  # Nothing has changed inside, return original layout

        return awkward.contents.ListOffsetArray(
            layout.offsets,
            content,
            parameters=layout.parameters,
        )

    # Potentially accumulate RegularArray into NumpyArray, if possible
    if isinstance(layout, awkward.contents.RegularArray):
        # Check if this is a terminal chain of RegularArrays leading to a NumpyArray,
        # which can be collapsed into a single NumpyArray with the right shape.
        node, sizes = layout, []
        while isinstance(node, awkward.contents.RegularArray):
            sizes.append(node.size)
            node = node.content

        if isinstance(node, awkward.contents.NumpyArray):
            return awkward.contents.NumpyArray(
                node.data.reshape(-1, *sizes),
                parameters=node.parameters,
            )

        # Some of the nested content is ragged, preserve the RegularArray structure
        content = _canonicalize_awkward_layout(layout.content)

        if content is layout.content:
            return layout  # Nothing has changed inside, return original layout

        return awkward.contents.RegularArray(
            content,
            layout.size,
            parameters=layout.parameters,
        )

    raise TypeError(f"Unsupported layout type: {type(layout)}")


def make_ragged_array(
    array: RaggedCompatibleType, slice: Optional[NDSlice] = None
) -> CanonicalRaggedArray:
    """Best-effort conversion of any numeric iterable to a ``ragged`` array.

    This function converts the underlying Awkward layout of the ragged array to a canonical form
    with only ListOffsetForms for variable-length dimensions. This ensures that the array is in a
    consistent format that is compatible with the RaggedStructure representation and can be
    efficiently stored and processed.

    Furthermore, if a slice is provided, it is applied to the array and only the relevant
    part of the data is retained. If the slice is invalid for the given array shape,
    a RaggedSlicingError is raised.

    Parameters
    ----------
    array : RaggedCompatibleType
        The input array-like object to be converted to a ragged array.
    slice : NDSlice, optional
        An optional NDSlice to apply to the array.
    """

    if isinstance(array, ragged.array):
        pass
    elif isinstance(array, numpy.ndarray):
        # this assumes that any nested-arrays do *not* have an object dtype
        if array.dtype.name == "object":
            array = ragged.array([row.tolist() for row in array])
        array = ragged.array(array)
    elif isinstance(array, (awkward.Array, _SupportsDLPack)):
        array = ragged.array(array)
    elif hasattr(array, "tolist"):
        array = ragged.array(array.tolist())
    else:
        array = ragged.array(list(array))

    array = ragged.array(_canonicalize_awkward_layout(array._impl.layout))

    if slice:
        try:
            array = array[slice]

        except IndexError as e:
            if "cannot slice" in str(e):
                raise RaggedSlicingError(
                    f"Invalid slice {slice} for the given ragged array with shape {array.shape}."
                ) from e

        # Drop data that are not included in the slice from the buffers
        if array.ndim > 0:
            array = make_ragged_array(awkward.to_packed(array._impl))

    return cast(CanonicalRaggedArray, array)


def make_ragged_chunks(array: ragged.array, limit_bytes: int) -> tuple[int, ...]:
    """Row-wise partitioning of a ragged array into chunks of at most `limit_bytes` bytes."""
    ak_array = awkward.Array(array._impl)
    if ak_array.nbytes <= limit_bytes:
        return ((len(ak_array),),) + (None,) * (ak_array.ndim - 1)

    # Work with boundary indices internally, convert to sizes at the end.
    boundaries: list[int] = [0, cast("int", array.shape[0])]
    chunk_index = 0

    while chunk_index < len(boundaries) - 1:
        start, end = boundaries[chunk_index], boundaries[chunk_index + 1]
        part = awkward.to_packed(ak_array[start:end])
        if part.nbytes > limit_bytes:
            if end - start == 1:
                msg = f"cannot partition individual rows to fit within {limit_bytes} bytes"
                raise ValueError(msg)
            mid = start + (end - start) // 2
            boundaries.insert(chunk_index + 1, mid)
        else:
            chunk_index += 1

    return (tuple(end - start for start, end in zip(boundaries, boundaries[1:])),) + (
        None,
    ) * (ak_array.ndim - 1)


def is_ragged_compatible_form(form: awkward.forms.form.Form) -> bool:
    """Check if an Awkward Form represents a ragged (or a uniform) array structure."""
    if isinstance(form, awkward.forms.NumpyForm):
        return form.primitive in {
            "bool",
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
            "float32",
            "float64",
            "complex64",
            "complex128",
        }
    elif isinstance(
        form,
        (
            awkward.forms.ListOffsetForm,
            awkward.forms.ListForm,
            awkward.forms.RegularForm,
        ),
    ):
        return is_ragged_compatible_form(form.content)
    else:
        return False
