"""Back-compatibility for clients that speak `access_blob` instead of `access_tags`.

BACK-COMPAT: every test in this file exists only to support clients older than
v0.2.19. Delete the whole file together with the access_blob helpers in
tiled/server/_backcompat.py and the deprecated fields in tiled/server/schemas.py.

These cover the translation helpers and the request schemas. The end-to-end
HTTP behavior is covered in test_access_control.py, which has a server with a
real access policy.
"""

import uuid
import warnings

import packaging.version
import pytest

from tiled.server._backcompat import (
    ConflictingAccessFields,
    access_blob_from_access_tags,
    access_tags_from_access_blob,
    access_tags_from_request,
    client_expects_access_blob,
    legacy_access_blob,
    legacy_access_blob_error,
)
from tiled.server.schemas import (
    PatchMetadataRequest,
    PostMetadataRequest,
    PutMetadataRequest,
)


@pytest.mark.parametrize(
    "access_blob, expected",
    [
        # An empty blob carries no tag information: the old server left
        # ownership to the access policy, which is what None means today.
        (None, None),
        ({}, None),
        ({"tags": ["a", "b"]}, frozenset({"a", "b"})),
        ({"tags": ["a", "a"]}, frozenset({"a"})),
        # NOT the same as an empty blob: an explicit request to clear the
        # tags, which the policy admits only for admins.
        ({"tags": []}, frozenset()),
    ],
)
def test_access_tags_from_access_blob(access_blob, expected):
    assert access_tags_from_access_blob(access_blob) == expected


@pytest.mark.parametrize(
    "access_blob",
    [
        # The old server generated {"user": ...} but never accepted it.
        {"user": "alice"},
        # A merge patch onto a user-owned node lands here.
        {"user": "alice", "tags": ["a"]},
        {"foo": "bar"},
        {"tags": "not-a-list"},
        {"tags": [1, 2]},
        {"tags": None},
    ],
)
def test_access_tags_from_malformed_access_blob(access_blob):
    with pytest.raises(ValueError):
        access_tags_from_access_blob(access_blob)


def test_access_blob_from_access_tags():
    assert access_blob_from_access_tags(None) is None
    assert access_blob_from_access_tags(frozenset()) == {"tags": []}
    # Sorted, so that index-based patches from a client are stable.
    assert access_blob_from_access_tags(frozenset({"b", "a"})) == {"tags": ["a", "b"]}


def test_access_blob_from_user_owned_tags():
    """A principal tag is rendered as the old {"user": <id>} blob."""
    assert access_blob_from_access_tags(frozenset({"user:alice"})) == {"user": "alice"}
    # A service principal carries both prefixes; both name the same identifier.
    identifier = str(uuid.uuid4())
    tags = frozenset({f"user:{identifier}", f"service:{identifier}"})
    assert access_blob_from_access_tags(tags) == {"user": identifier}
    # A principal tag wins over any ordinary tag, whatever the sort order.
    assert access_blob_from_access_tags(frozenset({"aaa", "user:alice"})) == {
        "user": "alice"
    }


def test_access_tags_round_trip():
    tags = frozenset({"chemists_tag", "public"})
    assert access_tags_from_access_blob(access_blob_from_access_tags(tags)) == tags


@pytest.mark.parametrize(
    "version, expected",
    [
        (None, False),  # not a python-tiled client; leave the payload alone
        ("0.2.18", True),
        ("0.1.0b16", True),
        ("0.2.19", False),
        ("0.3.0", False),
        # A pre-release of the version that introduced access_tags already
        # speaks the new dialect, even though it sorts before the release.
        ("0.2.19.dev47+gdc58483ae", False),
        ("0.2.19rc1", False),
        ("0.2.18.dev1", True),
    ],
)
def test_client_expects_access_blob(version, expected):
    parsed = None if version is None else packaging.version.parse(version)
    assert client_expects_access_blob(parsed) is expected


@pytest.mark.parametrize("model", [PostMetadataRequest, PutMetadataRequest])
def test_request_models_accept_access_blob(model):
    """Without this the field is silently dropped by pydantic's extra='ignore'."""
    kwargs = {"structure_family": "container"} if model is PostMetadataRequest else {}
    body = model.model_validate({**kwargs, "access_blob": {"tags": ["x"]}})
    assert access_tags_from_request(body) == frozenset({"x"})


@pytest.mark.parametrize("model", [PostMetadataRequest, PutMetadataRequest])
def test_request_models_advertise_the_deprecation(model):
    schema = model.model_json_schema()
    assert schema["properties"]["access_blob"]["deprecated"] is True


@pytest.mark.parametrize("model", [PostMetadataRequest, PutMetadataRequest])
def test_access_tags_and_access_blob_conflict(model):
    kwargs = {"structure_family": "container"} if model is PostMetadataRequest else {}
    body = model.model_validate(
        {**kwargs, "access_blob": {"tags": ["x"]}, "access_tags": ["y"]}
    )
    with pytest.raises(ConflictingAccessFields):
        access_tags_from_request(body)


def test_legacy_access_blob_error_maps_status():
    """A malformed blob was a 403 on the old server; sending both is a new 422."""
    conflict = legacy_access_blob_error(ConflictingAccessFields("both"))
    assert conflict.status_code == 422
    malformed = legacy_access_blob_error(ValueError("bad shape"))
    assert malformed.status_code == 403
    assert "bad shape" in malformed.detail


@pytest.mark.parametrize("model", [PostMetadataRequest, PutMetadataRequest])
def test_modern_request_does_not_enter_the_shim(model):
    """The router gates the shim on this, and reading the field must stay quiet.

    `access_blob` is declared deprecated, so touching it warns. A modern client
    must not provoke a warning about a field it never sent.
    """
    kwargs = {"structure_family": "container"} if model is PostMetadataRequest else {}
    bodies = [
        model.model_validate(kwargs),
        model.model_validate({**kwargs, "access_tags": ["y"]}),
    ]
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        for body in bodies:
            assert legacy_access_blob(body) is None


def test_patch_request_model_accepts_an_access_blob_patch():
    body = PatchMetadataRequest.model_validate(
        {
            "content-type": "application/json-patch+json",
            "specs": None,
            "access_blob": [{"op": "add", "path": "/tags", "value": ["a"]}],
        }
    )
    assert body.access_blob == [{"op": "add", "path": "/tags", "value": ["a"]}]
    assert body.access_tags is None
