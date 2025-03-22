import builtins
import re
import sys

from tiled.structures.array import ArrayStructure
from tiled.structures.awkward import AwkwardStructure
from tiled.structures.sparse import SparseStructure
from tiled.structures.table import TableStructure

if sys.version_info < (3, 10):
    EllipsisType = type(Ellipsis)
else:
    from types import EllipsisType

from typing import Any, Dict, List, Set, Union

JSON = Dict[str, Union[str, int, float, bool, Dict[str, "JSON"], List["JSON"]]]


class NDSlice(tuple):
    def __new__(cls, *args: Union[int, builtins.slice, EllipsisType]):
        return super().__new__(cls, tuple(args))

    @classmethod
    def from_json(cls, ser: list[JSON]):
        "Deserialize a json represenattion of an NDSlice"
        result = []
        for s in ser:
            if isinstance(s, dict):
                result.append(
                    builtins.slice(s.get("start"), s.get("stop"), s.get("step"))
                )
            elif isinstance(s, int):
                result.append(s)
            elif s == "ellipsis":
                result.append(...)
            else:
                raise ValueError("Can not intialize NDSlice from %s", s)
        return cls(*result)

    def to_json(self) -> list[JSON]:
        "Convert NDSlice into a JSON-serializable representation"
        result = []
        for s in self:
            if isinstance(s, builtins.slice):
                json_slice = (
                    ({} if s.start is None else {"start": s.start})
                    | ({} if s.stop is None else {"stop": s.stop})
                    | ({} if s.step is None else {"step": s.step})
                )
                result.append(json_slice)
            elif isinstance(s, int):
                result.append(s)
            elif s is Ellipsis:
                result.append("ellipsis")
            else:
                raise ValueError("Unprocessable entry in NDSlice, %s", s)
        return result

    @classmethod
    def from_numpy_str(cls, arg: str):
        """Parse and convert a numpy-style string representation of a slice

        For example, '(1:3, 4, 1:5:2, ...)' is converted to (slice(1, 3), 4, slice(1, 5, 2), ...).
        """

        arg = arg.strip("(][)").replace(" ", "")

        result = []
        for part in arg.split(","):
            if part.strip() == "...":
                result.append(Ellipsis)
            elif part.strip() == ":":
                result.append(builtins.slice(None))
            elif m := re.match(r"^(\d+)::?$", part.strip()):
                # "start:" or "start::"
                start, stop = int(m.group(1)), None
                result.append(builtins.slice(start, stop))
            elif m := re.match(r"^:(\d+):?$", part.strip()):
                # ":stop" or ":stop:"
                start, stop = None, int(m.group(1))
                result.append(builtins.slice(start, stop))
            elif m := re.match(r"^(\d+):(\d+):(\d+)$", part.strip()):
                # "start:stop:step"
                start, stop, step = map(int, m.groups())
                result.append(builtins.slice(start, stop, step))
            elif m := re.match(r"^(\d+):(\d+):*$", part.strip()):
                # "start:stop" or "start:stop:"
                start, stop = map(int, m.groups())
                result.append(builtins.slice(start, stop))
            elif m := re.match(r"^:(\d+):(\d+)$", part.strip()):
                # ":stop:step"
                stop, step = map(int, m.groups())
                result.append(builtins.slice(None, stop, step))
            elif m := re.match(r"^::(\d+)$", part.strip()):
                # "::step"
                step = int(m.group(1))
                result.append(builtins.slice(None, None, step))
            elif m := re.match(r"^(\d+)::(\d+)$", part.strip()):
                # "start::step"
                start, step = map(int, m.groups())
                result.append(builtins.slice(start, None, step))
            elif m := re.match(r"^(\d+)$", part.strip()):
                result.append(int(m.group()))
            else:
                raise ValueError(f"Invalid slice part: {part}")
        return cls(*result)

    def to_numpy_str(self) -> str:
        "Convert NDSlice to a numpy-style string representation"
        result = []
        for s in self:
            if isinstance(s, builtins.slice):
                string_slice = (
                    f"{(s.start or '')}:"
                    + ("" if s.stop is None else f"{s.stop}")
                    + (f":{str(s.step)}" if s.step else "")
                )
                result.append(string_slice)
            elif isinstance(s, int):
                result.append(str(s))
            elif s is Ellipsis:
                result.append("...")
            else:
                raise ValueError("Unprocessable entry in NDSlice, %s", s)
        return ",".join(result)


Scopes = Set[str]
Query = Any  # for now...
Filters = List[Query]

AnyStructure = Union[TableStructure, ArrayStructure, SparseStructure, AwkwardStructure]

__all__ = [
    "AnyStructure",
    "EllipsisType",
    "JSON",
    "NDSlice",
    "Scopes",
    "Query",
    "Filters",
]
