import asyncio

import numpy
import pytest

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.catalog import in_memory
from tiled.client import Context, from_context
from tiled.queries import FullText, Key, KeyLookup
from tiled.server.app import build_app

tree = MapAdapter(
    {
        "a": ArrayAdapter.from_array(
            numpy.arange(10), metadata={"apple": "red", "animal": "dog"}
        ),
        "b": ArrayAdapter.from_array(
            numpy.arange(10), metadata={"banana": "yellow", "animal": "dog"}
        ),
        "c": ArrayAdapter.from_array(
            numpy.arange(10), metadata={"cantalope": "orange", "animal": "cat"}
        ),
    }
)


@pytest.fixture(scope="module")
def map_client():
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


@pytest.fixture
def catalog_client(tmpdir):
    catalog = in_memory(writable_storage=str(tmpdir))
    app = build_app(catalog)
    with Context.from_app(app) as context:
        client = from_context(context)
        # Upload arrays and metadata from tree into client so that it has matching content.
        for key, value in tree.items():
            client.write_array(value.read(), metadata=value.metadata(), key=key)
        yield client


@pytest.fixture(
    params=("map_client", "catalog_client"),
)
def client(request: pytest.FixtureRequest):
    yield request.getfixturevalue(request.param)


@pytest.mark.parametrize(
    "key, value, expected_keys",
    [
        ("apple", "red", ["a"]),
        ("banana", "yellow", ["b"]),
        ("cantalope", "orange", ["c"]),
        ("animal", "dog", ["a", "b"]),
        ("animal", "cat", ["c"]),
    ],
)
def test_search(client, key, value, expected_keys):
    query = Key(key)
    results = client.search(query == value)
    assert list(results) == expected_keys


def test_compound_search(client):
    results = client.search(Key("animal") == "dog").search(Key("banana") == "yellow")
    assert list(results) == ["b"]


def test_indexing_over_search(client):
    results = client.search(Key("animal") == "dog")
    assert dict(results["a"].metadata) == tree["a"].metadata()


def test_key_into_results(client):
    results = client.search(Key("animal") == "dog")
    assert "apple" in results["a"].metadata
    assert "banana" in results["b"].metadata
    assert "c" not in results  # This *is* in the tree but not among the results.


def test_compound_key_into_results():
    nested_tree = MapAdapter(
        {
            "i": MapAdapter({"X": tree}, metadata={"temp": "hot"}),
            "j": MapAdapter({}, metadata={"temp": "cold"}),
        }
    )
    app = build_app(nested_tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        result = client.search(FullText("hot"))["i", "X", "a"]
        assert "apple" in result.metadata


class _ThreadRecordingMapAdapter(MapAdapter):
    """
    A container with only the synchronous interface (no ``keys_page`` etc.).

    Record whether the slicing ran on the server's event loop thread.
    """

    ran_on_event_loop = []  # slicing keys or items
    getitem_ran_on_event_loop = []

    @staticmethod
    def _record(record):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            record.append(False)
        else:
            record.append(True)

    def _keys_slice(self, *args, **kwargs):
        self._record(self.ran_on_event_loop)
        return super()._keys_slice(*args, **kwargs)

    def _items_slice(self, *args, **kwargs):
        self._record(self.ran_on_event_loop)
        return super()._items_slice(*args, **kwargs)

    def __getitem__(self, key):
        self._record(self.getitem_ran_on_event_loop)
        return super().__getitem__(key)


@pytest.mark.parametrize("method", ["keys", "items"])
def test_sync_container_listed_off_the_event_loop(method):
    """
    A container that has only the synchronous interface may block on I/O
    (e.g. a database query) when listing a page of its entries. The server
    must run that in a worker thread, or one slow request stalls every other
    request (and the health check) handled by the same process.
    """
    recording_tree = _ThreadRecordingMapAdapter(dict(tree.items()))
    app = build_app(recording_tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        _ThreadRecordingMapAdapter.ran_on_event_loop.clear()
        listed = list(getattr(client, method)())
    assert len(listed) == len(tree)
    assert _ThreadRecordingMapAdapter.ran_on_event_loop  # It was called...
    assert not any(_ThreadRecordingMapAdapter.ran_on_event_loop)  # ...in a thread.


def test_sync_container_searched_off_the_event_loop():
    """
    A container with the synchronous interface may evaluate a query when
    ``search`` is called (MapAdapter walks its entries), or block on I/O.
    The server must not do that on the event loop.
    """
    recording_tree = _ThreadRecordingMapAdapter(dict(tree.items()))
    app = build_app(recording_tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        _ThreadRecordingMapAdapter.ran_on_event_loop.clear()
        listed = list(client.search(Key("animal") == "dog"))
    assert listed == ["a", "b"]
    assert _ThreadRecordingMapAdapter.ran_on_event_loop  # It was called...
    assert not any(_ThreadRecordingMapAdapter.ran_on_event_loop)  # ...in a thread.


def test_sync_container_key_lookup_off_the_event_loop():
    """
    A lookup query is answered by ``tree[key]``, which may block on I/O in a
    container with the synchronous interface.
    """
    recording_tree = _ThreadRecordingMapAdapter(dict(tree.items()))
    record = _ThreadRecordingMapAdapter.getitem_ran_on_event_loop
    app = build_app(recording_tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        record.clear()
        listed = list(client.search(KeyLookup("a")))
        assert list(client.search(KeyLookup("does-not-exist"))) == []
    assert listed == ["a"]
    assert record  # It was called...
    assert not any(record)  # ...in a thread.
