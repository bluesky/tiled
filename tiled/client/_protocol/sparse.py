"""Pure request builders and response parsers for the sparse (COO) client.

Sparse nodes serve data through the array endpoints, encoded as an Apache
Arrow table with one `dim{i}` column of coordinates per dimension plus a
`data` column of values. Reads are described by `SparseReadRecipe`: the
request draft to send plus the shape needed to decode the response into a
`sparse.COO` array. Writes have nothing to decode, so they are described by
plain `RequestDraft`s (see the package docstring for the convention).

No I/O happens in this module; the client class sends the drafts and feeds
response content back through the parser function.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence

import numpy
import sparse

from ...ndslice import NDSlice
from ...serialization.table import deserialize_arrow, serialize_arrow
from ...structures.core import StructureFamily
from ...structures.data_source import DataSource
from ...structures.sparse import COOStructure
from ...utils import APACHE_ARROW_FILE_MIME_TYPE
from ..utils import params_from_slice
from .request import RequestDraft, fill_link_template


@dataclass(frozen=True)
class SparseReadRecipe:
    """A recipe for one read of sparse data: the request plus the result shape.

    Attributes
    ----------
    draft : RequestDraft
        The request that fetches the COO coordinates and values as Arrow bytes.
    shape : tuple
        Shape of the resulting `sparse.COO` array: the array's shape, narrowed by
        the slice when one is requested. The response carries only coordinates and
        values, so the shape must travel with the draft to interpret it.
    """

    draft: RequestDraft
    shape: tuple[int, ...]

    def parse_response(self, content: bytes) -> sparse.COO:
        """Convert an Arrow response body into a `sparse.COO` array.

        The table has columns named `dim0, dim1, ..., dimN, data`.
        """
        df = deserialize_arrow(content)
        return sparse.COO(
            data=df["data"].values,
            coords=numpy.stack([df[f"dim{i}"].values for i in range(len(self.shape))]),
            shape=self.shape,
        )

    @classmethod
    def for_full(
        cls, link: str, structure: COOStructure, slice: Any = None
    ) -> "SparseReadRecipe":
        "A recipe that reads the whole array or a slice of it, via `/array/full`."
        draft = RequestDraft(
            "GET",
            link,
            params=params_from_slice(slice),
            headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
        )
        return cls(draft, NDSlice(slice).shape_after_slice(structure.shape))

    @classmethod
    def for_block(
        cls,
        link: str,
        structure: COOStructure,
        block: Iterable[int],
        slice: Any = None,
    ) -> "SparseReadRecipe":
        """A recipe that reads one block (chunk) of the array, via `/array/block`.

        The block index is sent as a comma-joined `block` query parameter,
        overriding the placeholder template baked into the server-provided
        link. The result shape is derived from the overall array shape,
        exactly as in `for_full`.
        """
        params = params_from_slice(slice)
        params["block"] = ",".join(map(str, block))
        draft = RequestDraft(
            "GET",
            link,
            params=params,
            headers={"Accept": APACHE_ARROW_FILE_MIME_TYPE},
        )
        return cls(draft, NDSlice(slice).shape_after_slice(structure.shape))


def _coo_arrow_body(coords: Any, data: Any) -> bytes:
    """Encode COO coordinates and values as the Arrow bytes of a request body.

    `coords` holds one array of coordinates per dimension; each becomes a
    `dim{i}` column, alongside the `data` column of values.
    """
    # Defer the pandas import; it is only needed for writes.
    import pandas

    d = {f"dim{i}": coords for i, coords in enumerate(coords)}
    d["data"] = data
    df = pandas.DataFrame(d)
    return bytes(serialize_arrow(APACHE_ARROW_FILE_MIME_TYPE, df, {}))


def draft_request_write_sparse_full(link: str, coords: Any, data: Any) -> RequestDraft:
    "Draft the request that overwrites the full array, via `PUT /array/full`."
    return RequestDraft(
        "PUT",
        link,
        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        content=_coo_arrow_body(coords, data),
    )


def draft_request_write_sparse_block(
    link_template: str, block: Iterable[int], coords: Any, data: Any
) -> RequestDraft:
    """Draft the request that overwrites one block of the array, via `PUT /array/block`.

    The server provides the block link as a URL template with one positional
    placeholder per dimension, filled in with the block index. (Unlike reads,
    which send the block index as an explicit query parameter.)
    """
    return RequestDraft(
        "PUT",
        fill_link_template(link_template, *block),
        headers={"Content-Type": APACHE_ARROW_FILE_MIME_TYPE},
        content=_coo_arrow_body(coords, data),
    )


def data_source_from_coo(
    coords: Any, data: Any, shape: tuple[int, ...], dims: Optional[Sequence[str]]
) -> DataSource:
    "The data source that describes a single-chunk sparse (COO) array upload."
    from ...structures.array import BuiltinDtype

    structure = COOStructure(
        shape=shape,
        # This describes a single-chunk COO array.
        chunks=tuple((dim,) for dim in shape),
        dims=dims,
        data_type=BuiltinDtype.from_numpy_dtype(data.dtype),
        coord_data_type=BuiltinDtype.from_numpy_dtype(coords.dtype),
    )
    return DataSource(structure=structure, structure_family=StructureFamily.sparse)
