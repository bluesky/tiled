from tiled.adapters.mapping import MapAdapter
from tiled.utils import OneShotCachedMap


def test_sort_reverses_generic_mapping_items():
    mapping = OneShotCachedMap(
        {
            "first": lambda: object(),
            "second": lambda: object(),
        }
    )
    adapter = MapAdapter(mapping)

    reversed_adapter = adapter.sort([("_", -1)])

    assert list(reversed_adapter._mapping) == ["second", "first"]
