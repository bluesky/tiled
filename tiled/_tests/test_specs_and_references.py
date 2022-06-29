import pydantic
import pytest

from ..adapters.mapping import MapAdapter
from ..client import from_tree


def test_specs():
    tree = MapAdapter({}, specs=["spec_test"])
    c = from_tree(tree)
    assert c.item["attributes"]["specs"] == ["spec_test"]


def test_spec_is_converted_to_str():
    # Interesting pydantic behavior here: it converts rather than raises.
    tree = MapAdapter({}, specs=[1])
    c = from_tree(tree)
    assert c.item["attributes"]["specs"] == ["1"]


def test_references():
    tree = MapAdapter({}, references={"ref_test": "https://example.com"})
    c = from_tree(tree)
    assert c.item["attributes"]["references"] == {"ref_test": "https://example.com"}


def test_bad_reference():
    tree = MapAdapter({}, references={"ref_test": "not a URL"})
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        from_tree(tree)
