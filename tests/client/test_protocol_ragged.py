"""Unit tests for the ragged protocol layer (`tiled.client._protocol.ragged`).

These tests exercise read recipes, write-request builders, response parsers,
and failure classifiers as plain functions, with no server and no I/O.
"""

import httpx
import orjson
import pytest
import ragged

from tiled.client._protocol.ragged import (
    RaggedReadRecipe,
    draft_patch_request,
    draft_write_block_request,
    draft_write_request,
    parse_patch_response,
    patch_conflict_exception,
)
from tiled.structures.core import StructureFamily
from tiled.structures.ragged import RaggedStructure, make_ragged_array
from tiled.utils import Conflicts

FULL_LINK = "http://localhost:8000/api/v1/ragged/full/a/b"
# The server provides the block link with the `block` query parameter as a
# URL template.
BLOCK_LINK = "http://localhost:8000/api/v1/ragged/block/a/b?block={0}"
ZIP_MIMETYPE = "application/zip"
JSON_MIMETYPE = "application/json"


def make_array(data=((1.0, 2.0), (3.0,))):
    return make_ragged_array([list(row) for row in data])


def make_structure(array=None):
    return RaggedStructure.from_array(array if array is not None else make_array())


def test_for_full_zipped():
    recipe = RaggedReadRecipe.for_full(FULL_LINK, make_structure())
    assert recipe.draft.method == "GET"
    assert recipe.draft.url == FULL_LINK
    assert recipe.draft.params == {}
    # A non-scalar read negotiates zipped buffers.
    assert recipe.draft.headers == {"Accept": ZIP_MIMETYPE}
    assert recipe.draft.content is None
    assert recipe.scalar is False


def test_for_full_with_slice():
    recipe = RaggedReadRecipe.for_full(FULL_LINK, make_structure(), (slice(0, 1),))
    assert recipe.draft.params == {"slice": ":1"}
    assert recipe.scalar is False
    assert recipe.draft.headers == {"Accept": ZIP_MIMETYPE}


def test_for_full_scalar_slice():
    # A slice selecting a single element negotiates JSON instead.
    recipe = RaggedReadRecipe.for_full(FULL_LINK, make_structure(), (0, 1))
    assert recipe.draft.params == {"slice": "0,1"}
    assert recipe.scalar is True
    assert recipe.draft.headers == {"Accept": JSON_MIMETYPE}


def test_parse_zipped_round_trip():
    array = make_array()
    structure = make_structure(array)
    recipe = RaggedReadRecipe.for_full(FULL_LINK, structure)
    # A write body and a read response use the same zipped-buffers encoding.
    body = draft_write_request(FULL_LINK, array, persist=True).content
    parsed = recipe.parse(body)
    assert isinstance(parsed, ragged.array)
    assert parsed.tolist() == array.tolist()


def test_parse_scalar_json():
    recipe = RaggedReadRecipe.for_full(FULL_LINK, make_structure(), (0, 1))
    parsed = recipe.parse(orjson.dumps(3.5))
    assert isinstance(parsed, ragged.array)
    assert parsed.ndim == 0
    assert float(parsed) == 3.5


def test_draft_write_request():
    array = make_array()
    draft = draft_write_request(FULL_LINK, array)
    assert draft.method == "PUT"
    assert draft.url == FULL_LINK
    assert draft.params == {}
    assert draft.headers == {"Content-Type": ZIP_MIMETYPE}
    assert draft.content is not None


def test_draft_write_request_converts_list_of_lists():
    draft = draft_write_request(FULL_LINK, [[1.0, 2.0], [3.0]])
    recipe = RaggedReadRecipe.for_full(FULL_LINK, make_structure())
    assert recipe.parse(draft.content).tolist() == [[1.0, 2.0], [3.0]]


@pytest.mark.parametrize(
    "build",
    [
        lambda array, persist: draft_write_request(FULL_LINK, array, persist=persist),
        lambda array, persist: draft_write_block_request(
            BLOCK_LINK, (0,), array, persist=persist
        ),
        lambda array, persist: draft_patch_request(
            FULL_LINK, array, offset=2, persist=persist
        ),
    ],
    ids=["write", "write_block", "patch"],
)
def test_persist_param_only_when_non_default(build):
    array = make_array()
    assert "persist" not in build(array, True).params
    assert build(array, False).params["persist"] is False


@pytest.mark.parametrize(
    "block, expected_block_param",
    [
        # An integer block index and its 1-tuple form are sent identically,
        # comma-joined (not filled into the URL template).
        (0, "0"),
        ((0,), "0"),
    ],
)
def test_draft_write_block_request(block, expected_block_param):
    array = make_array()
    draft = draft_write_block_request(BLOCK_LINK, block, array)
    assert draft.method == "PUT"
    # The URL template is left as-is; the explicit `block` parameter
    # overrides the baked template in the link's query.
    assert draft.url == BLOCK_LINK
    assert draft.params == {"block": expected_block_param}
    assert draft.headers == {"Content-Type": ZIP_MIMETYPE}


def test_draft_patch_request():
    array = make_array([[4.0], [5.0, 6.0]])
    draft = draft_patch_request(FULL_LINK, array, offset=(2,), extend=True)
    assert draft.method == "PATCH"
    assert draft.url == FULL_LINK
    # `shape` carries only the leading dimension.
    assert draft.params == {"shape": "2", "offset": "2", "extend": True}
    assert draft.headers == {"Content-Type": ZIP_MIMETYPE}
    # The body is zipped buffers encoding the appended rows.
    recipe = RaggedReadRecipe.for_full(FULL_LINK, make_structure(array))
    assert recipe.parse(draft.content).tolist() == array.tolist()


def test_draft_patch_request_integer_offset():
    array = make_array([[4.0], [5.0, 6.0]])
    draft = draft_patch_request(FULL_LINK, array, offset=2)
    assert draft.params == {"shape": "2", "offset": "2", "extend": False}


@pytest.mark.parametrize(
    "status_code, expected_type",
    [
        # A 409 and a 400 map to different exception types.
        (httpx.codes.CONFLICT, Conflicts),
        (httpx.codes.BAD_REQUEST, ValueError),
    ],
)
def test_patch_conflict_exception(status_code, expected_type):
    # The server phrases conflicts in terms of URL query parameters; the
    # client rephrases them in terms of Python keyword arguments.
    body = {"detail": "Use ?extend=true to extend array dimension."}
    exc = patch_conflict_exception(status_code, body)
    assert isinstance(exc, expected_type)
    assert str(exc) == "Pass keyword argument extend=true to extend array dimension."


def test_patch_conflict_exception_default_message():
    exc = patch_conflict_exception(httpx.codes.BAD_REQUEST, {})
    assert str(exc) == "Array parameters conflict."


def test_parse_patch_response():
    body = {
        "data_type": {
            "endianness": "little",
            "kind": "f",
            "itemsize": 8,
            "dt_units": None,
        },
        "shape": [3, None],
        "size": 5,
        "chunks": [[3], None],
        "dims": None,
        "resizable": False,
    }
    structure = parse_patch_response(body, StructureFamily.ragged)
    assert isinstance(structure, RaggedStructure)
    assert structure.shape == (3, None)
    assert structure.chunks == ((3,), None)
    assert structure.size == 5
