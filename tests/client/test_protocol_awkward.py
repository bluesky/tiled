"""Unit tests for the awkward protocol layer (`tiled.client._protocol.awkward`).

These tests exercise read recipes, write-request builders, and response
parsers as plain functions, with no server and no I/O.
"""

import awkward
import orjson
import pytest

from tiled.client._protocol.awkward import (
    AwkwardReadRecipe,
    draft_write_request,
    parse_buffers_response,
)
from tiled.structures.awkward import AwkwardStructure

BUFFERS_LINK = "http://localhost:8000/api/v1/awkward/buffers/a/b"
FULL_LINK = "http://localhost:8000/api/v1/awkward/full/a/b"
ZIP_MIMETYPE = "application/zip"
JSON_MIMETYPE = "application/json"


def make_array():
    return awkward.Array(
        [
            {"x": 1.1, "y": [1]},
            {"x": 2.2, "y": [1, 2]},
            {"x": 3.3, "y": [1, 2, 3]},
        ]
    )


def make_structure_and_container(array):
    form, length, container = awkward.to_buffers(array)
    return AwkwardStructure(length=length, form=form.to_dict()), container


def test_for_buffers_full():
    array = make_array()
    structure, _ = make_structure_and_container(array)
    recipe = AwkwardReadRecipe.for_buffers(BUFFERS_LINK, structure, ...)
    assert recipe.draft.method == "POST"
    assert recipe.draft.url == BUFFERS_LINK
    assert recipe.draft.params == {}
    assert recipe.draft.headers == {
        "Accept": ZIP_MIMETYPE,
        "Content-Type": JSON_MIMETYPE,
    }
    # The whole array touches every form key, sent sorted (deterministic).
    assert orjson.loads(recipe.draft.content) == ["node1", "node2", "node3"]
    # Projecting onto all keys preserves the full form.
    assert recipe.form.to_dict() == structure.awkward_form.to_dict()
    assert recipe.length == structure.length


def test_for_buffers_projection_touches_only_needed_keys():
    # Reading one field requests only the form keys that field touches; the
    # form is projected onto them.
    array = make_array()
    structure, _ = make_structure_and_container(array)
    full_recipe = AwkwardReadRecipe.for_buffers(BUFFERS_LINK, structure, ...)
    field_recipe = AwkwardReadRecipe.for_buffers(BUFFERS_LINK, structure, "x")
    full_keys = set(orjson.loads(full_recipe.draft.content))
    field_keys = set(orjson.loads(field_recipe.draft.content))
    assert field_keys < full_keys
    assert field_recipe.form.fields == ["x"]
    assert field_recipe.length == structure.length


@pytest.mark.parametrize(
    "data",
    [
        [[1, 2, 3], [], [4, 5]],
        [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
    ],
)
def test_write_body_round_trip(data):
    array = awkward.Array(data)
    structure, container = make_structure_and_container(array)
    draft = draft_write_request(FULL_LINK, structure, container)
    assert draft.method == "PUT"
    assert draft.url == FULL_LINK
    assert draft.params == {}
    assert draft.headers == {"Content-Type": ZIP_MIMETYPE}
    # The body is zipped buffers; decoding with the structure's own form
    # recovers the array.
    parsed = parse_buffers_response(
        draft.content, structure.awkward_form, structure.length
    )
    assert parsed.tolist() == array.tolist()
