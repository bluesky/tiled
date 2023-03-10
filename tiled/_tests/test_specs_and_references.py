import pydantic
import pytest

from ..adapters.mapping import MapAdapter
from ..client import Context, from_context
from ..server.app import build_app
from ..structures.core import Spec


def test_specs():
    tree = MapAdapter({}, specs=[Spec("spec_test")])
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert client.specs == [Spec("spec_test")]


def test_specs_give_as_str():
    tree = MapAdapter({}, specs=["spec_test"])
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert client.specs == [Spec("spec_test")]


def test_specs_with_version():
    tree = MapAdapter({}, specs=[Spec("spec_test", version="1.1")])
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert client.specs == [Spec("spec_test", version="1.1")]


def test_references():
    tree = MapAdapter(
        {}, references=[{"label": "ref_test", "url": "https://example.com"}]
    )
    with Context.from_app(build_app(tree)) as context:
        client = from_context(context)
    assert client.references == [{"label": "ref_test", "url": "https://example.com"}]


def test_bad_reference():
    # an invalid URL
    tree = MapAdapter({}, references=[{"label": "ref_test", "url": "not a URL"}])
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        with Context.from_app(build_app(tree)) as context:
            from_context(context)

    # a dict instead of a list of dicts
    tree = MapAdapter(
        {}, references={"label": "ref_test", "url": "https://example.com"}
    )
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        with Context.from_app(build_app(tree)) as context:
            from_context(context)

    # dict has too many items
    tree = MapAdapter(
        {},
        references=[
            {"label": "ref_test", "url": "https://example.com", "extra": "oops"}
        ],
    )
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        with Context.from_app(build_app(tree)) as context:
            from_context(context)

    # dict has not enough items
    tree = MapAdapter({}, references=[{}])
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        with Context.from_app(build_app(tree)) as context:
            from_context(context)
