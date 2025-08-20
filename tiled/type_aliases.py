import sys

if sys.version_info < (3, 10):
    EllipsisType = type(Ellipsis)
else:
    from types import EllipsisType

from typing import Any, List, Mapping, Sequence, Set, Union

JSON_ITEM = Union[
    str, int, float, Mapping[str, "JSON_ITEM"], Sequence["JSON_ITEM"], None
]
JSON = Mapping[str, JSON_ITEM]

Scopes = Set[str]
Query = Any  # for now...
Filters = List[Query]

__all__ = [
    "EllipsisType",
    "JSON",
    "Scopes",
    "Query",
    "Filters",
]
