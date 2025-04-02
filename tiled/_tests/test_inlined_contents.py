"""
Test the feature wherein the descriptions (metadata, structure_family,
structure, etc.) for a node's children may be included with the
description of a node. This is used in xarray_dataset now and may
be used more widely later.
"""

import numpy
import pytest
import xarray

from ..adapters.array import ArrayAdapter
from ..adapters.mapping import MapAdapter
from ..adapters.xarray import DatasetAdapter
from ..client import Context, from_context, record_history
from ..server.app import build_app
from ..server.core import INLINED_CONTENTS_LIMIT

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
