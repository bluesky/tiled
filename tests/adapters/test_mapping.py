from typing import Any

import pytest

from tiled.adapters.mapping import MapAdapter
from tiled.server.schemas import SortingDirection, SortingItem
from tiled.utils import OneShotCachedMap


@pytest.mark.parametrize(
    "sorting",
    [
        [SortingItem(key="_", direction=SortingDirection.DESCENDING)],
        [("_", SortingDirection.DESCENDING)],
    ],
)
def test_sort_reverses_generic_mapping_items(sorting: list[Any]) -> None:
    mapping: OneShotCachedMap[str, MapAdapter[Any]] = OneShotCachedMap(
        {
            "first": lambda: MapAdapter({}),
            "second": lambda: MapAdapter({}),
        }
    )
    adapter = MapAdapter(mapping)

    reversed_adapter = adapter.sort(sorting)

    assert list(reversed_adapter._mapping) == ["second", "first"]
