import sys

if sys.version_info < (3, 10):
    EllipsisType = type(Ellipsis)
else:
    from types import EllipsisType

from typing import Any, Dict, List, Set, Tuple, Union

JSON = Dict[str, Union[str, int, float, bool, Dict[str, "JSON"], List["JSON"]]]
# NDSlice = Union[
#     int, slice, Tuple[Union[int, slice, EllipsisType], ...], EllipsisType
# ]  # TODO Replace this with our Union for a slice/tuple/.../etc.


class NDSlice(tuple):

    def __new__(cls, *args: Union[int, slice, EllipsisType]):
        return super().__new__(cls, tuple(args))

    @classmethod
    def from_json(cls, ser: list[JSON]):
        "Deserialize a json represenattion of an NDSlice"
        result = []
        for s in ser:
            if isinstance(s, dict):
                result.append(slice(s.get("start"), s.get('stop'), s.get('step')))
            elif isinstance(s, int):
                result.append(s)
            elif s == 'ellipsis':
                result.append(...)
            else:
                raise ValueError("Can not intialize NDSlice from %s", s)
        return tuple(result)

    def to_json(self) -> list[JSON]:
        "Convert NDSlice into a JSON-serializable representation"
        result = []
        for s in self:
            if isinstance(s, slice):
                result.append({"start": s.start, "stop": s.stop, "step": s.step})
            elif isinstance(s, int):
                result.append(s)
            elif s is Ellipsis:
                result.append('ellipsis')
            else:
                raise ValueError("Unprocessable entry in NDSlice, %s", s)
        return result

Scopes = Set[str]
Query = Any  # for now...
Filters = List[Query]
