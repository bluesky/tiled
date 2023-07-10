import sqlite3
from contextlib import closing

import numpy
import pytest

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..client import Context, from_context, record_history
from ..client.cache import Cache, CachedResponse
from ..server.app import build_app

tree = MapAdapter(
    {
        f"arr{i:03}": ArrayAdapter.from_array(i * numpy.arange(3), metadata={"i": i})
        for i in range(30)
    },
    metadata={"t": 1},
)


@pytest.fixture
def client():
    app = build_app(tree)
    with Context.from_app(app, cache=Cache()) as context:
        yield from_context(context)


def test_cache(client, tmpdir):
    cache = client.context.cache
    before_count = cache.count()
    before_size = cache.size()

    # First time: not cached
    with record_history() as h:
        list(client.keys())
    for response in h.responses:
        assert not isinstance(response, CachedResponse)

    after_count = cache.count()
    after_size = cache.size()
    assert after_count > before_count
    assert after_size > before_size

    # Second time: cached
    with record_history() as h:
        list(client.keys())
    for response in h.responses:
        assert isinstance(response, CachedResponse)


def test_no_cache(client):
    client.context.cache = None

    # First time: not cached
    with record_history() as h:
        list(client.keys())
    for response in h.responses:
        assert not isinstance(response, CachedResponse)

    # Second time: cached
    with record_history() as h:
        list(client.keys())
    for response in h.responses:
        assert not isinstance(response, CachedResponse)


def test_lru_eviction(client):
    # First time: not cached
    client.context.cache.capacity = 5000
    num_items = len(client)
    for i in range(num_items):
        client.values()[i]

    # Not all the items fit.
    # If this fails, perhaps the bytesize of an item has drifted.
    # Tweak the capacity.
    assert client.context.cache.count() < num_items

    # Most recently accessed: still cached
    with record_history() as h:
        client.values()[i]
    for response in h.responses:
        assert isinstance(response, CachedResponse)

    # Least recently accessed: has been evicted
    with record_history() as h:
        client.values()[0]
    for response in h.responses:
        assert not isinstance(response, CachedResponse)

    # Second time: cached
    with record_history() as h:
        client.metadata
    for response in h.responses:
        assert not isinstance(response, CachedResponse)


def test_item_too_large_to_store(client):
    client.context.cache.max_item_size = 10

    # First time: not cached
    with record_history() as h:
        list(client.keys())
    for response in h.responses:
        assert not isinstance(response, CachedResponse)

    # Second time: still not cached
    with record_history() as h:
        list(client.keys())
    for response in h.responses:
        assert not isinstance(response, CachedResponse)


def test_readonly_cache(client):
    # Start with a writable cache.

    # First time: not cached
    with record_history() as h:
        client.values()[0]
    for response in h.responses:
        assert not isinstance(response, CachedResponse)

    # Second time: cached
    with record_history() as h:
        client.values()[0]
    for response in h.responses:
        assert isinstance(response, CachedResponse)

    orig_size = client.context.cache.size()

    # Now use the same file as readonly cache.
    filepath = client.context.cache.filepath
    ro_cache = client.context.cache = Cache(filepath, readonly=True)

    # Still cached (from before)
    with record_history() as h:
        client.values()[0]
    for response in h.responses:
        assert isinstance(response, CachedResponse)

    # Now look at something new...
    # First time: not cached
    with record_history() as h:
        client.values()[1]
    for response in h.responses:
        assert not isinstance(response, CachedResponse)

    # Second time: still not cached
    with record_history() as h:
        client.values()[1]
    for response in h.responses:
        assert not isinstance(response, CachedResponse)

    # And cache size has not changed
    assert ro_cache.size() == orig_size

    # Implementation detail: database connection is read-only,
    # for defense in depth.
    with pytest.raises(sqlite3.OperationalError):
        with closing(ro_cache._conn.cursor()) as cur:
            cur.execute("DELETE FROM responses")


def test_clear_cache(client):
    cache = client.context.cache
    client.values()[0]
    assert cache.size() > 0
    assert cache.count() > 0
    cache.clear()
    assert cache.size() == cache.count() == 0
