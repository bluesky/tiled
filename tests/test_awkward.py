import io
import json
import zipfile
from pathlib import Path
from unittest import mock

import awkward
import numpy
import pyarrow.feather
import pyarrow.parquet
import pytest
from fastapi import HTTPException
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_422_UNPROCESSABLE_CONTENT

from tiled.adapters.awkward import AwkwardBuffersAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context, record_history
from tiled.server.app import build_app
from tiled.server.router import require_expected_buffers
from tiled.storage import DirectoryContainer
from tiled.structures.awkward import AwkwardStructure
from tiled.utils import APACHE_ARROW_FILE_MIME_TYPE, UnsafePath, path_from_uri

from .utils import URL_LIMITS


@pytest.fixture
def catalog(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    yield catalog


@pytest.fixture
def app(catalog):
    app = build_app(catalog)
    yield app


@pytest.fixture
def context(app):
    with Context.from_app(app) as context:
        yield context


@pytest.fixture
def client(context):
    client = from_context(context)
    yield client


def test_slicing(client):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = awkward.Array(
        [
            [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
            [],
            [{"x": 3.3, "y": [1, 2, 3]}],
        ]
    )
    returned = client.write_awkward(array, key="test")
    # Test with client returned, and with client from lookup.
    for aac in [returned, client["test"]]:
        # Read the data back out from the AwkwardArrayClient, progressively sliced.
        assert awkward.almost_equal(aac.read(), array)
        assert awkward.almost_equal(aac[:], array)
        assert awkward.almost_equal(aac[0], array[0])
        assert awkward.almost_equal(aac[0, "y"], array[0, "y"])
        assert awkward.almost_equal(aac[0, "y", :1], array[0, "y", :1])

        # When sliced, the serer sends less data.
        with record_history() as h:
            aac[:]
        assert len(h.responses) == 1  # sanity check
        full_response_size = len(h.responses[0].content)
        with record_history() as h:
            aac[0, "y"]
        assert len(h.responses) == 1  # sanity check
        sliced_response_size = len(h.responses[0].content)
        assert sliced_response_size < full_response_size


def test_export_json(client, buffer):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = awkward.Array(
        [
            [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
            [],
            [{"x": 3.3, "y": [1, 2, 3]}],
        ]
    )
    aac = client.write_awkward(array, key="test")

    file = buffer
    aac.export(file, format="application/json")
    actual = bytes(file.getbuffer()).decode()
    assert actual == awkward.to_json(array)


def test_export_arrow(tmpdir, client):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = awkward.Array(
        [
            [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
            [],
            [{"x": 3.3, "y": [1, 2, 3]}],
        ]
    )
    aac = client.write_awkward(array, key="test")

    filepath = tmpdir / "actual.arrow"
    aac.export(str(filepath), format=APACHE_ARROW_FILE_MIME_TYPE)
    actual = pyarrow.feather.read_table(filepath)
    expected = awkward.to_arrow_table(array)
    assert actual == expected


def test_export_parquet(tmpdir, client):
    # Write data into catalog. It will be stored as directory of buffers
    # named like 'node0-offsets' and 'node2-data'.
    array = awkward.Array(
        [
            [{"x": 1.1, "y": [1]}, {"x": 2.2, "y": [1, 2]}],
            [],
            [{"x": 3.3, "y": [1, 2, 3]}],
        ]
    )
    aac = client.write_awkward(array, key="test")

    filepath = tmpdir / "actual.parquet"
    aac.export(str(filepath), format="application/x-parquet")
    actual = pyarrow.parquet.read_table(filepath)
    expected = awkward.to_arrow_table(array)
    assert actual == expected


def test_large_number_of_form_keys(client):
    "Request should succeed even when too many form_keys to fit in a URL."
    # https://github.com/bluesky/tiled/pull/577

    # The HTTP spec itself has no limit, but tools impose a pragmatic one.
    # This is a representative value.
    URL_LENGTH_LIMIT = URL_LIMITS.DEFAULT
    NUM_KEYS = 7_000  # meant to achieve a large body of form_keys
    array = awkward.Array([{f"key{i:05}": i for i in range(NUM_KEYS)}])
    aac = client.write_awkward(array, key="test")
    with record_history() as h:
        aac[0]
    (request,) = h.requests
    assert request.method == "POST"
    form_keys = json.loads(request.read())
    form_key_length = len(str(form_keys))
    assert form_key_length > URL_LENGTH_LIMIT
    url_length = len(str(request.url))
    assert url_length <= URL_LENGTH_LIMIT


def test_more_slicing_1(client):
    array = awkward.Array(
        [
            {
                "stuff": 123,
                "file": [
                    {"filename": 321, "other": 3.14},
                ],
            },
        ]
    )
    returned = client.write_awkward(array, key="test")
    # Test with client returned, and with client from lookup.
    for aac in [returned, client["test"]]:
        # Read the data back out from the AwkwardArrayClient, progressively sliced.
        assert awkward.almost_equal(aac.read(), array)
        assert awkward.almost_equal(aac[:], array)
        assert awkward.almost_equal(aac["file", "filename"], array["file", "filename"])


def test_more_slicing_2(client):
    array = awkward.from_numpy(
        numpy.arange(2 * 3 * 5).reshape(2, 3, 5), regulararray=True
    )
    returned = client.write_awkward(array, key="test")
    # Test with client returned, and with client from lookup.
    for aac in [returned, client["test"]]:
        # Read the data back out from the AwkwardArrayClient, progressively sliced.
        assert awkward.almost_equal(aac.read(), array)
        assert awkward.almost_equal(aac[:], array)
        assert awkward.almost_equal(aac[1:], array[1:])
        assert awkward.almost_equal(aac[1:, 1:], array[1:, 1:])


def test_more_slicing_3(client):
    array = awkward.Array(
        [
            {"good": 123, "bad": 123},
            {"good": 321, "bad": [1, 2, 3]},
        ]
    )
    returned = client.write_awkward(array, key="test")
    # Test with client returned, and with client from lookup.
    for aac in [returned, client["test"]]:
        # Read the data back out from the AwkwardArrayClient, progressively sliced.
        assert awkward.almost_equal(aac.read(), array)
        assert awkward.almost_equal(aac[:], array[:])
        assert awkward.almost_equal(aac["good"], array["good"])


record_arrays_examples = [
    (
        "simple_forms",
        [
            {"a": 1, "b": "val_1", "c": 0.1, "d": True},
            {"a": 2, "b": "val_2", "c": 0.2, "d": False},
            {"a": 3, "b": "val_3", "c": 0.3, "d": True},
        ],
    ),
    (
        "nested_forms",
        [
            {"a": [1, 2, 3], "b": {"x": 1, "y": 2}},
            {"a": [4, 5, 6], "b": {"x": 1, "y": 2}},
            {"a": [7, 8, 9], "b": {"x": 1, "y": 2}},
        ],
    ),
    (
        "empty_forms",
        [
            {"a": [], "b": {}, "c": None, "d": 0.1},
            {"a": [], "b": {}, "c": None},
            {"a": [], "b": {}, "c": None},
        ],
    ),
    (
        "union_forms",
        [
            {"a": 1, "b": True, "c": None},
            {"a": 0.2, "b": [1, 2, 3]},
            {"a": "three", "b": {"x": 1, "y": 2}},
        ],
    ),
    (
        "numpy_forms",
        [
            {"a": numpy.ones(5), "b": numpy.empty(0), "c": numpy.zeros(1)},
            {"a": numpy.ones(5), "b": numpy.empty(1), "c": numpy.zeros((2, 2))},
            {"a": numpy.ones(5), "b": numpy.empty(2), "c": numpy.zeros((3, 3, 3))},
        ],
    ),
]


@pytest.mark.parametrize("name, data", record_arrays_examples)
def test_record_arrays(client, name, data):
    array = awkward.Array(data)
    returned = client.write_awkward(array, key=f"test_{name}")

    # Test with client returned, and with client from lookup.
    for aac in [returned, client[f"test_{name}"]]:
        assert awkward.almost_equal(aac.read(), array)
        assert awkward.almost_equal(aac[:], array[:])
        for field in array.fields:
            assert awkward.almost_equal(aac[field], array[field])


def node_directory(client, key):
    "The directory holding the buffers of an awkward node."
    (asset,) = client[key].include_data_sources().data_sources()[0].assets
    return path_from_uri(asset.data_uri)


def zip_of(members):
    file = io.BytesIO()
    with zipfile.ZipFile(file, "w") as zip:
        for name, content in members.items():
            zip.writestr(name, content)
    return file.getvalue()


@pytest.mark.parametrize(
    "member",
    ["../outside-data", "../../outside-data", "{tmpdir}/outside-data", "unexpected"],
)
def test_zip_upload_refuses_unexpected_member(client, tmpdir, member):
    "A zip upload may hold only the buffers that the node's form names."
    array = awkward.Array([[1, 2], [3]])
    client.write_awkward(array, key="x")
    directory = node_directory(client, "x")
    files_before = sorted(path.name for path in directory.iterdir())
    _form, _length, container = awkward.to_buffers(array)
    members = {key: numpy.asarray(value).tobytes() for key, value in container.items()}
    members[member.format(tmpdir=tmpdir)] = b"UNEXPECTED"

    with mock.patch.object(zipfile.ZipFile, "read", autospec=True) as read:
        response = client.context.http_client.put(
            "/api/v1/awkward/full/x",
            content=zip_of(members),
            headers={"Content-Type": "application/zip"},
        )

    assert response.status_code == HTTP_400_BAD_REQUEST
    assert not read.called  # Nothing is decompressed before the names are checked.
    written = [
        path
        for path in Path(tmpdir).rglob("*")
        if path.is_file() and path.read_bytes() == b"UNEXPECTED"
    ]
    assert not written
    assert sorted(path.name for path in directory.iterdir()) == files_before
    assert awkward.almost_equal(client["x"].read(), array)


def test_require_expected_buffers():
    expected = {"node0-offsets", "node1-data"}
    require_expected_buffers(["node1-data"], expected)  # A subset is fine.
    with pytest.raises(HTTPException) as info:
        require_expected_buffers(["node1-data", "../x", "node9-data"], expected)
    assert info.value.status_code == HTTP_400_BAD_REQUEST
    assert "['../x', 'node9-data']" in info.value.detail


@pytest.mark.parametrize(
    "form_key", ["../victim/node1", "../../outside", "{tmpdir}/victim/node1", "a/b"]
)
def test_form_key_cannot_name_a_path(client, tmpdir, form_key):
    "A form whose buffers would be stored outside of the node is refused."
    victim = awkward.Array([[1, 2], [3]])
    client.write_awkward(victim, key="victim")
    form = {
        "class": "NumpyArray",
        "primitive": "int64",
        "form_key": form_key.format(tmpdir=tmpdir),
    }
    body = {
        "id": "attacker",
        "structure_family": "awkward",
        "metadata": {},
        "specs": [],
        "data_sources": [
            {
                "structure_family": "awkward",
                "structure": {"length": 3, "form": form},
                "mimetype": "application/x-awkward-buffers",
                "management": "writable",
                "assets": [],
                "parameters": {},
            }
        ],
    }

    response = client.context.http_client.post("/api/v1/metadata/", json=body)

    assert response.status_code == HTTP_422_UNPROCESSABLE_CONTENT
    assert "attacker" not in client


@pytest.mark.parametrize("key", ["../outside", "sub/inside", "..", ".", ""])
def test_directory_container_refuses_keys_that_are_not_file_names(tmp_path, key):
    directory = tmp_path / "node"
    (directory / "sub").mkdir(parents=True)
    (tmp_path / "outside").write_bytes(b"OUTSIDE")
    (directory / "sub" / "inside").write_bytes(b"INSIDE")
    container = DirectoryContainer(directory)

    with pytest.raises(UnsafePath):
        container[key]
    with pytest.raises(UnsafePath):
        container[key] = b"WRITTEN"
    with pytest.raises(UnsafePath):
        del container[key]
    assert (tmp_path / "outside").read_bytes() == b"OUTSIDE"
    assert (directory / "sub" / "inside").read_bytes() == b"INSIDE"


def test_directory_container_refuses_absolute_key(tmp_path):
    directory = tmp_path / "node"
    directory.mkdir()
    outside = tmp_path / "outside"
    outside.write_bytes(b"OUTSIDE")
    container = DirectoryContainer(directory)

    with pytest.raises(UnsafePath):
        container[str(outside)]
    with pytest.raises(UnsafePath):
        container[str(outside)] = b"WRITTEN"
    assert outside.read_bytes() == b"OUTSIDE"


def test_stored_form_cannot_read_outside_of_node(tmp_path):
    "A form already in the catalog cannot be used to read another node's buffers."
    directory = tmp_path / "node"
    directory.mkdir()
    (tmp_path / "victim").mkdir()
    (tmp_path / "victim" / "node1-data").write_bytes(b"VICTIM")
    form = {"class": "NumpyArray", "primitive": "int64", "form_key": "../victim/node1"}
    adapter = AwkwardBuffersAdapter(
        DirectoryContainer(directory), AwkwardStructure(length=3, form=form)
    )

    with pytest.raises(UnsafePath):
        adapter.read_buffers()
