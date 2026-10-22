"""Unit tests for the sparse protocol layer (`tiled.client._protocol.sparse`).

These tests exercise read recipes, write-request builders, data-source
builders, and response parsers as plain functions, with no server and no I/O.
"""

import numpy
import pytest
import sparse

from tiled.client._protocol.sparse import (
    SparseReadRecipe,
    data_source_from_coo,
    draft_request_write_sparse_block,
    draft_request_write_sparse_full,
)
from tiled.structures.core import StructureFamily
from tiled.structures.sparse import COOStructure
from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE

# Sparse nodes are served through the array endpoints.
FULL_LINK = "http://localhost:8000/api/v1/array/full/a/b"
# The server provides the block link with the `block` query parameter as a
# URL template, one positional placeholder per dimension.
BLOCK_LINK = "http://localhost:8000/api/v1/array/block/a/b?block={0},{1}"


def make_structure(shape=(3, 4), chunks=((3,), (4,))):
    return COOStructure(chunks=chunks, shape=shape)


def make_coo(shape=(3, 4)):
    coords = numpy.array([[0, 1], [0, 1]])
    data = numpy.array([3.1, 2.8])
    return sparse.COO(coords=coords, data=data, shape=shape)


@pytest.mark.parametrize(
    "array_slice, expected_params, expected_shape",
    [
        # Without a slice, no `slice` param is sent and the shape is the
        # array's own.
        (None, {}, (3, 4)),
        # A slice adds the `slice` param and narrows the shape.
        (numpy.s_[0:2, 1], {"slice": ":2,1"}, (2,)),
        (numpy.s_[1], {"slice": "1"}, (4,)),
    ],
)
def test_for_full(array_slice, expected_params, expected_shape):
    recipe = SparseReadRecipe.for_full(FULL_LINK, make_structure(), array_slice)
    assert recipe.draft.method == "GET"
    assert recipe.draft.url == FULL_LINK
    assert recipe.draft.params == expected_params
    assert recipe.draft.headers == {"Accept": APACHE_ARROW_FILE_MIME_TYPE}
    assert recipe.draft.content is None
    assert recipe.shape == expected_shape


def test_for_block():
    recipe = SparseReadRecipe.for_block(BLOCK_LINK, make_structure(), (0, 1))
    assert recipe.draft.method == "GET"
    assert recipe.draft.url == BLOCK_LINK
    # The baked `block` template in the link's query is overridden by the
    # actual block index, sent as a comma-joined query parameter.
    assert recipe.draft.params == {"block": "0,1"}
    assert recipe.draft.headers == {"Accept": APACHE_ARROW_FILE_MIME_TYPE}
    assert recipe.shape == (3, 4)


def test_for_block_with_slice():
    recipe = SparseReadRecipe.for_block(
        BLOCK_LINK, make_structure(), (0, 1), numpy.s_[0:2]
    )
    assert recipe.draft.params == {"block": "0,1", "slice": ":2"}
    # The shape is derived from the overall array shape, as in `for_full`.
    assert recipe.shape == (2, 4)


def test_draft_request_write_sparse_full():
    coo = make_coo()
    draft = draft_request_write_sparse_full(FULL_LINK, coo.coords, coo.data)
    assert draft.method == "PUT"
    assert draft.url == FULL_LINK
    assert draft.params == {}
    assert draft.headers == {"Content-Type": APACHE_ARROW_FILE_MIME_TYPE}
    # The body is Arrow bytes encoding the coordinates and values.
    parsed = SparseReadRecipe(draft, coo.shape).parse(draft.content)
    numpy.testing.assert_array_equal(parsed.todense(), coo.todense())


def test_draft_request_write_sparse_block():
    coo = make_coo(shape=(2, 2))
    draft = draft_request_write_sparse_block(BLOCK_LINK, (1, 0), coo.coords, coo.data)
    # The block index fills the URL template and is folded back into params.
    assert draft.method == "PUT"
    assert draft.url == BLOCK_LINK.format(1, 0)
    assert draft.params == {"block": ["1,0"]}
    assert draft.headers == {"Content-Type": APACHE_ARROW_FILE_MIME_TYPE}
    parsed = SparseReadRecipe(draft, coo.shape).parse(draft.content)
    numpy.testing.assert_array_equal(parsed.todense(), coo.todense())


# Data source submitted when creating a sparse node


def test_data_source_from_coo():
    coords = numpy.array([[2, 5]], dtype="int64")
    data = numpy.array([1.3, 7.5], dtype="float64")
    data_source = data_source_from_coo(coords, data, (10,), ["x"])
    assert data_source.structure_family == StructureFamily.sparse
    structure = data_source.structure
    assert structure.shape == (10,)
    assert structure.chunks == ((10,),)  # a single chunk
    assert structure.dims == ["x"]
    assert structure.data_type.to_numpy_dtype() == numpy.dtype("float64")
    assert structure.coord_data_type.to_numpy_dtype() == numpy.dtype("int64")
