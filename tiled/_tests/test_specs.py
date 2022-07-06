from ..adapters.mapping import MapAdapter
from ..client import from_tree


def test_specs():
    tree = MapAdapter({}, specs=["spec_test"])
    c = from_tree(tree)
    assert c.specs == ["spec_test"]


def test_spec_is_converted_to_str():
    # Interesting pydantic behavior here: it converts rather than raises.
    tree = MapAdapter({}, specs=[1])
    c = from_tree(tree)
    assert c.specs == ["1"]
