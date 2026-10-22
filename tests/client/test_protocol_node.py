"""Unit tests for the node protocol layer (`tiled.client._protocol.node`).

These tests exercise the node-operation request builders, the create
recipe, response parsers/mergers, and the create-failure classification as
plain functions and dataclasses, with no server and no I/O.
"""

import uuid
from dataclasses import asdict

import httpx
import numpy
import orjson
import pytest

from tiled.client._protocol.node import (
    CreateFailureAction,
    CreateNodeRecipe,
    draft_request_asset_manifest,
    draft_request_delete_node,
    draft_request_lookup_node,
    draft_request_patch_metadata,
    draft_request_refresh_node,
    draft_request_replace_metadata,
    merge_mutated_attributes,
)
from tiled.client._protocol.request import RetryPolicy
from tiled.client.utils import (
    ClientError,
    apply_patch,
    collapse_key_segments,
    normalize_specs_patch,
)
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.container import ContainerStructure
from tiled.structures.core import Spec, StructureFamily
from tiled.structures.data_source import Asset, DataSource
from tiled.utils import patch_mimetypes

SELF_LINK = "http://localhost:8000/api/v1/metadata/a/b"
MSGPACK_MIMETYPE = "application/x-msgpack"
JSON_MIMETYPE = "application/json"


@pytest.mark.parametrize(
    "include_data_sources, expected_params",
    [(False, {}), (True, {"include_data_sources": True})],
)
def test_draft_request_refresh_node(include_data_sources, expected_params):
    draft = draft_request_refresh_node(
        SELF_LINK, include_data_sources=include_data_sources
    )
    assert draft.method == "GET"
    assert draft.url == SELF_LINK
    assert draft.params == expected_params
    assert draft.headers == {"Accept": MSGPACK_MIMETYPE}


def test_draft_request_asset_manifest():
    manifest_link = SELF_LINK.replace("/metadata", "/asset/manifest", 1)
    draft = draft_request_asset_manifest(manifest_link, 7)
    assert draft.method == "GET"
    assert draft.url == manifest_link
    assert draft.params == {"id": 7}
    # The asset endpoints respond in plain JSON; no Accept header is sent.
    assert draft.headers == {}


@pytest.mark.parametrize(
    "content_type, specs_patch, expected",
    [
        (patch_mimetypes.JSON_PATCH, None, None),
        (
            patch_mimetypes.JSON_PATCH,
            [{"op": "add", "path": "/0", "value": "spec1"}],
            [{"op": "add", "path": "/0", "value": {"name": "spec1", "version": None}}],
        ),
        # Non-string values pass through unchanged.
        (
            patch_mimetypes.JSON_PATCH,
            [{"op": "remove", "path": "/0"}],
            [{"op": "remove", "path": "/0"}],
        ),
        (
            patch_mimetypes.MERGE_PATCH,
            ["spec1"],
            [{"name": "spec1", "version": None}],
        ),
    ],
)
def test_normalize_specs_patch(content_type, specs_patch, expected):
    assert normalize_specs_patch(specs_patch, content_type) == expected


def test_apply_patch_rejects_unknown_content_type():
    with pytest.raises(ValueError):
        apply_patch({}, {}, "application/x-unknown-patch")


def test_draft_request_patch_metadata():
    metadata_patch = [{"op": "add", "path": "/color", "value": "red"}]
    draft = draft_request_patch_metadata(
        SELF_LINK,
        content_type=patch_mimetypes.JSON_PATCH,
        metadata_patch=metadata_patch,
        specs_patch=None,
        access_tags_patch=None,
        drop_revision=True,
    )
    assert draft.method == "PATCH"
    assert draft.url == SELF_LINK
    assert draft.params == {"drop_revision": True}
    assert draft.headers == {"Content-Type": JSON_MIMETYPE}
    body = orjson.loads(draft.content)
    assert body == {
        "content-type": patch_mimetypes.JSON_PATCH,
        "metadata": metadata_patch,
        "specs": None,
        "access_tags": None,
    }


def test_apply_patch():
    assert apply_patch(
        {"color": "blue"},
        [{"op": "replace", "path": "/color", "value": "red"}],
        patch_mimetypes.JSON_PATCH,
    ) == {"color": "red"}
    assert apply_patch(
        {"color": "blue"}, {"color": "red"}, patch_mimetypes.MERGE_PATCH
    ) == {"color": "red"}


LOCAL_OUTCOMES = {
    "metadata": {"color": "red"},
    "specs": [{"name": "spec1", "version": None}],
    "access_tags": ["tag1"],
}


@pytest.mark.parametrize(
    "content, expected",
    [
        # The server's response echoes nothing: the local outcomes stand.
        ({}, LOCAL_OUTCOMES),
        # A server-side validator modified the metadata and access tags: the
        # response's values are canonical. Specs are always the local outcome.
        (
            {
                "metadata": {"validated": True},
                "specs": [{"name": "rewritten", "version": None}],
                "access_tags": ["granted"],
            },
            {
                "metadata": {"validated": True},
                "specs": [{"name": "spec1", "version": None}],
                "access_tags": ["granted"],
            },
        ),
    ],
    ids=["local-outcomes", "server-values"],
)
def test_merge_mutated_attributes(content, expected):
    assert merge_mutated_attributes(content, **LOCAL_OUTCOMES) == expected


def test_merge_mutated_attributes_untouched_fields_are_absent():
    assert merge_mutated_attributes({"metadata": {"validated": True}}) == {}


def test_draft_request_replace_metadata():
    draft = draft_request_replace_metadata(
        SELF_LINK,
        metadata={"color": "red"},
        specs=[{"name": "spec1", "version": None}],
        access_tags=["tag1"],
    )
    assert draft.method == "PUT"
    assert draft.url == SELF_LINK
    assert draft.params == {}
    assert draft.headers == {"Content-Type": JSON_MIMETYPE}
    body = orjson.loads(draft.content)
    assert body == {
        "metadata": {"color": "red"},
        "specs": [{"name": "spec1", "version": None}],
        "access_tags": ["tag1"],
    }


def make_array_data_source(**kwargs):
    structure = ArrayStructure(
        shape=(3,),
        chunks=((3,),),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("int64")),
    )
    return DataSource(
        structure=structure, structure_family=StructureFamily.array, **kwargs
    )


def make_create_recipe(
    structure_family="array", data_sources=None, **overrides
) -> CreateNodeRecipe:
    if data_sources is None:
        data_sources = [make_array_data_source()] if structure_family == "array" else []
    kwargs = {
        "key": "x",
        "metadata": {"color": "blue"},
        "specs": ["SomeSpec"],
        "access_tags": None,
        "library_version": "0.2.4",
        **overrides,
    }
    return CreateNodeRecipe.for_new_node(
        SELF_LINK + "/", ["a", "b"], structure_family, data_sources, **kwargs
    )


# Create flow


def test_for_new_node():
    recipe = make_create_recipe()
    assert recipe.key == "x"
    assert recipe.draft.method == "POST"
    assert recipe.draft.url == SELF_LINK + "/"
    assert recipe.draft.params == {}
    assert recipe.draft.headers == {
        "Accept": MSGPACK_MIMETYPE,
        "Content-Type": JSON_MIMETYPE,
    }
    # The create flow orchestrates its own retries across attempts.
    assert recipe.draft.retry_policy is RetryPolicy.NONE
    body = orjson.loads(recipe.draft.content)
    assert body["id"] == "x"
    assert body["ancestors"] == ["a", "b"]
    assert body["structure_family"] == "array"
    assert body["metadata"] == {"color": "blue"}
    assert body["specs"] == [{"name": "SomeSpec", "version": None}]
    assert body["access_tags"] is None
    (data_source,) = body["data_sources"]
    assert data_source["structure_family"] == "array"
    assert data_source["structure"]["shape"] == [3]
    # The item skeleton carries the same attributes, to be completed by
    # `CreateNodeRecipe.merge` once the create succeeds.
    attributes = recipe.item["attributes"]
    assert attributes["ancestors"] == ["a", "b"]
    assert attributes["structure_family"] == StructureFamily.array
    assert attributes["specs"] == [{"name": "SomeSpec", "version": None}]
    # The normalized context used by `merge` and `matches_existing` is
    # stored on the recipe itself.
    assert recipe.structure_family == StructureFamily.array
    assert recipe.specs == [{"name": "SomeSpec", "version": None}]
    assert recipe.metadata == {"color": "blue"}


def test_for_new_node_generates_key():
    # With no key supplied, one is generated client-side so that all retries
    # of this request share it (an idempotency token), and it is sent as the
    # node's id.
    recipe = make_create_recipe(key=None)
    uuid.UUID(recipe.key)  # a valid UUID
    assert orjson.loads(recipe.draft.content)["id"] == recipe.key
    assert make_create_recipe(key=None).key != recipe.key


def test_for_new_node_defaults_metadata_to_empty():
    recipe = make_create_recipe(metadata=None)
    assert orjson.loads(recipe.draft.content)["metadata"] == {}
    assert recipe.item["attributes"]["metadata"] == {}
    assert recipe.metadata == {}


def test_for_new_node_normalizes_numpy_metadata():
    # `safe_json_dump` normalizes numpy scalars and arrays to JSON types,
    # both in the request body and in the recipe's stored metadata.
    recipe = make_create_recipe(
        metadata={"count": numpy.int64(3), "vals": numpy.arange(2)}
    )
    assert orjson.loads(recipe.draft.content)["metadata"] == {
        "count": 3,
        "vals": [0, 1],
    }
    assert recipe.metadata == {"count": 3, "vals": [0, 1]}


@pytest.mark.parametrize(
    "assets, expected_url",
    [
        # A create whose data sources reference existing assets registers
        # them, via `/register`; otherwise it posts to the node's own
        # `/metadata` endpoint.
        ([], SELF_LINK + "/"),
        (
            [
                Asset(
                    data_uri="file:///tmp/x.csv",
                    is_directory=False,
                    parameter="data_uri",
                )
            ],
            SELF_LINK.replace("/metadata/", "/register/", 1) + "/",
        ),
    ],
    ids=["metadata", "register"],
)
def test_for_new_node_endpoint_choice(assets, expected_url):
    recipe = make_create_recipe(
        data_sources=[make_array_data_source(assets=assets)],
        metadata=None,
        specs=None,
    )
    assert recipe.draft.url == expected_url


@pytest.mark.parametrize(
    "library_version, properties_sent",
    [
        # Servers older than 0.2.4 can not accept the data source
        # "properties" field, so it is stripped from the body.
        ("0.2.3", False),
        ("0.2.4", True),
        ("0.3.0", True),
    ],
)
def test_for_new_node_properties_backcompat(library_version, properties_sent):
    recipe = make_create_recipe(library_version=library_version)
    (data_source,) = orjson.loads(recipe.draft.content)["data_sources"]
    assert ("properties" in data_source) is properties_sent


def _http_status_error(status_code, cls=httpx.HTTPStatusError):
    request = httpx.Request("POST", "http://example.com/api/v1/metadata/")
    response = httpx.Response(status_code, request=request)
    return cls(f"{status_code}", request=request, response=response)


@pytest.mark.parametrize(
    "exc, flag_in, expected_flag, expected_action",
    [
        # Response lost after the request was sent: set the flag, raise into
        # the retry machinery.
        (httpx.ReadTimeout("x"), False, True, CreateFailureAction.RAISE),
        (httpx.WriteError("x"), False, True, CreateFailureAction.RAISE),
        (httpx.RemoteProtocolError("x"), False, True, CreateFailureAction.RAISE),
        # Request never sent: flag unchanged, raise (retry machinery decides).
        (httpx.ConnectError("x"), False, False, CreateFailureAction.RAISE),
        (httpx.ConnectTimeout("x"), False, False, CreateFailureAction.RAISE),
        # 5xx sets the flag but never adopts on the same attempt.
        (_http_status_error(500), False, True, CreateFailureAction.RAISE),
        (_http_status_error(502), False, True, CreateFailureAction.RAISE),
        (_http_status_error(502), True, True, CreateFailureAction.RAISE),
        # A clean 409 (no attempt may have reached the server) raises ...
        (_http_status_error(409), False, False, CreateFailureAction.RAISE),
        # ... but a 409 after a possibly-received attempt offers adoption,
        # including for the ClientError subclass that handle_error raises.
        (_http_status_error(409), True, True, CreateFailureAction.TRY_ADOPT),
        (
            _http_status_error(409, ClientError),
            True,
            True,
            CreateFailureAction.TRY_ADOPT,
        ),
        # Other 4xx never adopt, and never set or clear the flag.
        (_http_status_error(400), True, True, CreateFailureAction.RAISE),
        (_http_status_error(400), False, False, CreateFailureAction.RAISE),
        # Arbitrary exceptions: flag unchanged (in particular, never reset).
        (ValueError("x"), False, False, CreateFailureAction.RAISE),
        (ValueError("x"), True, True, CreateFailureAction.RAISE),
    ],
)
def test_classify_failure(exc, flag_in, expected_flag, expected_action):
    decision = CreateNodeRecipe.classify_failure(exc, flag_in)
    assert decision.may_have_reached_server is expected_flag
    assert decision.action is expected_action


EXISTING_ATTRS = {
    "structure_family": "container",
    "specs": [
        {"name": "SpecA", "version": None},
        {"name": "SpecB", "version": "2"},
    ],
    "metadata": {"color": "blue", "count": 3},
}


@pytest.mark.parametrize(
    "overrides, existing, expected_mismatch",
    [
        # Specs may be given as Spec objects or strings; they compare
        # normalized.
        ({}, EXISTING_ATTRS, None),
        (
            {"specs": None, "metadata": None},
            {"structure_family": "container", "specs": [], "metadata": {}},
            None,
        ),
        # The submitted metadata is compared after the same JSON round-trip
        # normalization that a create applies: numpy types become JSON types.
        (
            {"metadata": {"color": "blue", "count": numpy.int64(3)}},
            EXISTING_ATTRS,
            None,
        ),
        ({"structure_family": "array"}, EXISTING_ATTRS, "structure_family"),
        ({"specs": ["SpecA"]}, EXISTING_ATTRS, "specs"),
        # Spec order is significant (e.g. client dispatch uses the first
        # recognized spec), so the same specs in a different order mismatch.
        (
            {"specs": [Spec("SpecB", version="2"), "SpecA"]},
            EXISTING_ATTRS,
            "specs",
        ),
        ({"metadata": {"color": "red", "count": 3}}, EXISTING_ATTRS, "metadata"),
    ],
    ids=[
        "match",
        "match-empty",
        "match-numpy-normalized",
        "family-mismatch",
        "specs-mismatch",
        "specs-order-mismatch",
        "metadata-mismatch",
    ],
)
def test_matches_existing(overrides, existing, expected_mismatch):
    recipe = make_create_recipe(
        **{
            "structure_family": "container",
            "specs": ["SpecA", Spec("SpecB", version="2")],
            "metadata": {"color": "blue", "count": 3},
            **overrides,
        }
    )
    mismatch = recipe.matches_existing(existing)
    if expected_mismatch is None:
        assert mismatch is None
    else:
        assert expected_mismatch in mismatch


def test_merge_container_defaults():
    # With nothing relevant in the response, the structure is the container
    # default and the submitted metadata is replaced by the recipe's
    # JSON-normalized copy.
    recipe = make_create_recipe(
        structure_family="container",
        metadata={"count": numpy.int64(3)},
        specs=None,
    )
    content = {"id": "x", "links": {"self": SELF_LINK + "/x"}}
    created_node = recipe.merge(content)
    assert created_node.item is recipe.item  # merged in place
    assert created_node.structure == {"contents": None, "count": None}
    assert created_node.item["attributes"]["structure"] == created_node.structure
    assert created_node.item["attributes"]["metadata"] == {"count": 3}
    assert created_node.item["id"] == "x"
    assert created_node.item["links"] == {"self": SELF_LINK + "/x"}


def test_merge_server_overrides():
    # Metadata, structure, data sources, and access tags returned by the
    # server (e.g. filled in by a validator) replace the submitted copies.
    recipe = make_create_recipe(structure_family="container")
    raw_structure = {"keys": ["c"]}
    content = {
        "id": "x",
        "links": {"self": SELF_LINK + "/x"},
        "metadata": {"color": "blue", "server_added": 1},
        "structure": raw_structure,
        "data_sources": [{"id": 7}],
        "access_tags": ["tag"],
    }
    created_node = recipe.merge(content)
    attributes = created_node.item["attributes"]
    assert attributes["metadata"] == {"color": "blue", "server_added": 1}
    # The response's structure is handed to the family's structure type as is.
    assert attributes["structure"] == ContainerStructure(raw_structure)
    assert attributes["data_sources"] == [{"id": 7}]
    assert attributes["access_tags"] == ["tag"]
    # The returned structure, used for client dispatch, is still the
    # container default; only the item records the server's version.
    assert created_node.structure == {"contents": None, "count": None}


def test_merge_coerces_structure_dataclass():
    # A data source may carry its structure as a dict; the structure used
    # for client dispatch is coerced to the matching dataclass.
    structure = ArrayStructure(
        shape=(3,),
        chunks=((3,),),
        data_type=BuiltinDtype.from_numpy_dtype(numpy.dtype("int64")),
    )
    data_source = DataSource(
        structure=asdict(structure), structure_family=StructureFamily.array
    )
    recipe = make_create_recipe(data_sources=[data_source])
    content = {"id": "x", "links": {}}
    created_node = recipe.merge(content)
    assert created_node.structure == structure
    assert isinstance(created_node.structure, ArrayStructure)


def test_merge_keeps_structure_dataclass():
    recipe = make_create_recipe()
    created_node = recipe.merge({"id": "x"})
    assert isinstance(created_node.structure, ArrayStructure)


@pytest.mark.parametrize(
    "keys, expected",
    [
        (("a",), ("a",)),
        (("a", "b", "c"), ("a", "b", "c")),
        # Each key may itself contain slashes.
        (("a/b", "c"), ("a", "b", "c")),
        (("/a/b/",), ("a", "b")),
    ],
)
def test_collapse_key_segments(keys, expected):
    assert collapse_key_segments(keys) == expected


@pytest.mark.parametrize(
    "include_data_sources, expected_params",
    [(False, {}), (True, {"include_data_sources": True})],
)
def test_draft_request_lookup_node(include_data_sources, expected_params):
    # The path segments are appended to the node's own (trailing-slash
    # normalized) metadata link, jumping down the tree in one request.
    draft = draft_request_lookup_node(
        SELF_LINK + "/", ("c", "d"), include_data_sources=include_data_sources
    )
    assert draft.method == "GET"
    assert draft.url == f"{SELF_LINK}/c/d"
    assert draft.params == expected_params
    assert draft.headers == {"Accept": MSGPACK_MIMETYPE}


def test_draft_request_delete_node():
    draft = draft_request_delete_node(
        SELF_LINK, "c", recursive=True, external_only=False
    )
    assert draft.method == "DELETE"
    assert draft.url == f"{SELF_LINK}/c"
    assert draft.params == {"recursive": True, "external_only": False}
    assert draft.content is None
