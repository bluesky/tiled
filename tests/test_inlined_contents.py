"""
Test the feature wherein the descriptions (metadata, structure_family,
structure, etc.) for a node's children may be included with the
description of a node. This is used in xarray_dataset now and may
be used more widely later.
"""

import asyncio

import numpy
import pytest
import xarray

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.adapters.xarray import DatasetAdapter
from tiled.client import Context, from_context, record_history
from tiled.server.app import build_app
from tiled.server.core import INLINED_CONTENTS_LIMIT
from tiled.utils import BrokenLink

tree = MapAdapter(
    {
        "dataset": DatasetAdapter.from_dataset(
            xarray.Dataset(
                data_vars={"temperature": ("time", numpy.array([100, 99, 98]))},
                coords={"time": numpy.array([1, 2, 3])},
            )
        ),
    },
    metadata={"thing": "stuff"},
)


@pytest.fixture(scope="module")
def client():
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        yield client


def test_lookup(client):
    "Accessing a child node should trigger one request."
    with record_history() as history:
        client["dataset"]
    assert len(history.requests) == 1


def test_iter(client):
    "Iteration should be free because the contents were in-lined."
    expected = ["temperature", "time"]
    dsc = client["dataset"]
    with record_history() as history:
        assert list(dsc) == expected
    assert not history.requests

    # Implementation detail:
    # Without inlined contents, a request is needed.
    with record_history() as history:
        assert list(dsc.__iter__(_ignore_inlined_contents=True)) == expected
    assert history.requests


def test_keys_slice(client):
    "Iteration should be free because the contents were in-lined."
    expected = ["temperature", "time"]
    dsc = client["dataset"]
    with record_history() as history:
        assert list(dsc.keys()) == expected
    assert not history.requests

    with record_history() as history:
        assert dsc.keys()[:] == expected
    assert not history.requests

    with record_history() as history:
        assert dsc.keys().first() == "temperature"
        assert dsc.keys().last() == "time"
    assert not history.requests

    # Implementation detail:
    # Without inlined contents, a request is needed.
    with record_history() as history:
        list(dsc._keys_slice(0, 1, 1, _ignore_inlined_contents=True))
    assert history.requests


def test_items_slice(client):
    "Iteration should be free because the contents were in-lined."
    dsc = client["dataset"]
    with record_history() as history:
        list(dsc.items())
    assert not history.requests

    with record_history() as history:
        dsc.items()
    assert not history.requests

    with record_history() as history:
        dsc.items().first()
    assert not history.requests

    with record_history() as history:
        dsc.items().last()
    assert not history.requests

    # Implementation detail:
    # Without inlined contents, a request is needed.
    with record_history() as history:
        list(dsc._items_slice(0, 1, 1, _ignore_inlined_contents=True))
    assert history.requests


def test_too_wide_for_inline():
    """
    The server will not inline contents above a certain limit.

    It is fetched in pages on demand, as usual with nodes.
    """

    a = numpy.array([1])
    tree = MapAdapter(
        {
            f"item{i:05}": ArrayAdapter.from_array(i * a)
            for i in range(1 + 2 * INLINED_CONTENTS_LIMIT)
        }
    )
    app = build_app(tree)
    with Context.from_app(app) as context:
        client = from_context(context)
        assert client.item["attributes"]["structure"]["contents"] is None
        with record_history() as history:
            assert set(client) == set(tree)
        assert len(history.requests) >= 3


class _InliningMapAdapter(MapAdapter):
    """
    A container that has its contents inlined.

    Record whether it was walked on the server's event loop thread.
    """

    ran_on_event_loop = []

    def inlined_contents_enabled(self, depth):
        return True

    def _record(self):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            self.ran_on_event_loop.append(False)
        else:
            self.ran_on_event_loop.append(True)

    def _keys_slice(self, *args, **kwargs):
        self._record()
        return super()._keys_slice(*args, **kwargs)

    def __getitem__(self, key):
        self._record()
        return super().__getitem__(key)


def test_inlined_contents_walked_off_the_event_loop():
    """
    Inlining a node's contents walks it with the synchronous interface
    (its keys, and ``node[key]`` for each), which may block on I/O. The server
    must run that in a worker thread, or one slow node stalls every other
    request handled by the same process.
    """
    a = numpy.array([1])
    tree = MapAdapter(
        {
            "node": _InliningMapAdapter(
                {"x": ArrayAdapter.from_array(a), "y": ArrayAdapter.from_array(a)}
            )
        }
    )
    record = _InliningMapAdapter.ran_on_event_loop
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        record.clear()
        node = client["node"]
        with record_history() as history:
            assert list(node) == ["x", "y"]
        assert not history.requests  # The contents were inlined.
    assert record  # It was walked...
    assert not any(record)  # ...in a thread.


def test_inline_gives_up_when_count_was_underestimated():
    """
    A node may have more children than its count says (the count may be an
    estimate, or the node may have grown). Its contents are not inlined then.
    """

    class Underestimating(_InliningMapAdapter):
        def __len__(self):
            return 1

    a = numpy.array([1])
    tree = MapAdapter(
        {
            "node": Underestimating(
                {
                    f"item{i:05}": ArrayAdapter.from_array(a)
                    for i in range(1 + INLINED_CONTENTS_LIMIT)
                }
            )
        }
    )
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        structure = client["node"].item["attributes"]["structure"]
    assert structure["contents"] is None
    assert structure["count"] == 1


def test_inline_keeps_key_of_broken_link():
    "A broken link among the children is inlined as its key alone."

    class WithBrokenLink(_InliningMapAdapter):
        def __getitem__(self, key):
            if key == "broken":
                raise BrokenLink(key)
            return super().__getitem__(key)

    a = numpy.array([1])
    tree = MapAdapter(
        {
            "node": WithBrokenLink(
                {"x": ArrayAdapter.from_array(a), "broken": ArrayAdapter.from_array(a)}
            )
        }
    )
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
        structure = client["node"].item["attributes"]["structure"]
    assert structure["count"] == 2
    assert list(structure["contents"]) == ["x", "broken"]
    assert structure["contents"]["x"] is not None
    assert structure["contents"]["broken"] is None
