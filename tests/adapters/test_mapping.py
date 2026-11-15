from tiled.adapters.mapping import MapAdapter
from tiled.server.schemas import SortingDirection, SortingItem
from tiled.utils import OneShotCachedMap


def test_sort_reverses_generic_mapping_items() -> None:
    mapping: OneShotCachedMap[str, object] = OneShotCachedMap(
        {
            "first": lambda: object(),
            "second": lambda: object(),
        }
    )
    adapter = MapAdapter(mapping)

    reversed_adapter = adapter.sort(
        SortingItem(key="_", direction=SortingDirection.DESCENDING)
    )

    assert list(reversed_adapter._mapping) == ["second", "first"]
