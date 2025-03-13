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
        return tuple(result)

    def to_json(self) -> list[JSON]:
        "Convert NDSlice into a JSON-serializable representation"
        result = []
        for s in self:
            if isinstance(s, builtins.slice):
                result.append({"start": s.start, "stop": s.stop, "step": s.step})
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

        if not (arg.startswith("[") and arg.endswith("]")) and not (
            arg.startswith("(") and arg.endswith(")")
        ):
            raise ValueError(
                "Slice must be enclosed in square brackets or parentheses."
            )

        result = []
        for part in arg[1:-1].split(","):
            if part.strip() == ":":
                result.append(builtins.slice(None))
            elif m := re.match(r"^(\d+):$", part.strip()):
                start, stop = int(m.group(1)), None
                result.append(builtins.slice(start, stop))
            elif m := re.match(r"^:(\d+)$", part.strip()):
                start, stop = 0, int(m.group(1))
                result.append(builtins.slice(start, stop))
            elif m := re.match(r"^(\d+):(\d+):(\d+)$", part.strip()):
                start, stop, step = map(int, m.groups())
                result.append(builtins.slice(start, stop, step))
            elif m := re.match(r"^(\d+):(\d+)$", part.strip()):
                start, stop = map(int, m.groups())
                result.append(builtins.slice(start, stop))
            elif m := re.match(r"^(\d+)$", part.strip()):
                result.append(int(m.group()))
            elif part.strip() == "...":
                result.append(Ellipsis)
            else:
                raise ValueError(f"Invalid slice part: {part}")
            # TODO: cases like "::n" or ":4:"
        return tuple(result)

    def to_numpy_str(self) -> str:
        "Convert NDSlice into a numpy-style string representation"
        raise NotImplementedError("Not yet implemented")


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
