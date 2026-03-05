import builtins
import re
from typing import Optional, Union
from ndindex import ndindex

from .type_aliases import JSON, EllipsisType, Chunks

def is_ellipsis(arg) -> bool:
    return isinstance(arg, EllipsisType) or (arg == Ellipsis)


def merge_slices(*args: Union["NDSlice", "NDBlock"]) -> Union["NDSlice", "NDBlock"]:
    """Merge multiple NDSlices or NDBlocks
    
    Creates a single NDSlice or (NDBlock if all arguemnts are NDBlocks) if they are
    contiguous and compatible.
    """
    if (len(args)) == 1:
        return args[0]

    # Check that all arguments have the same dimension
    if len(set(map(len, args))) != 1:
        raise ValueError("All NDSlices must have the same number of dimensions")
    
    # Loop over dimensions and check if they can be merged
    already_merged, result = False, []
    for dim in zip(*args):
        # Ellipsis at the same position can be merged
        if all(map(is_ellipsis, dim)):
            result.append(Ellipsis)
            continue

        # Integers must match exactly or be consecutive and have no gaps
        if all(isinstance(d, int) for d in dim):
            if len(vals := set(dim)) == 1:
                # All integers are the same, keep this dimension as is
                result.append(vals.pop())
                continue
            elif vals == set(range(min(vals), max(vals)+1)):
                # Integers are consecutive, merge into a slice
                if already_merged:
                    raise ValueError("Can not merge more than one dimension")
                result.append(builtins.slice(min(vals), max(vals) + 1))
                already_merged = True
                continue
            else:
                raise ValueError("Integer dimensions must match exactly "
                                 "or be consecutive and have no gaps")

        # Some dimensions could be integers and some are slices
        if any(isinstance(d, int) for d in dim):
            dim = tuple(builtins.slice(d, d+1) if isinstance(d, int) else d for d in dim)

        # Now we have only slices and can check for contiguity and compatibility
        if all(isinstance(d, builtins.slice) for d in dim):
            starts = set(d.start or 0 for d in dim)
            stops = set(d.stop for d in dim)

            if len(set(d.step or 1 for d in dim)) != 1:
                raise ValueError("Slice dimensions must have the same step")
            
            elif len(starts) == 1 and len(stops) == 1:
                # All slices are the same, keep this dimension as is
                result.append(dim[0])
                continue

            elif (len(starts) == len(stops)) and (len(first := starts-stops) == 1) and (len(last := stops-starts) == 1):
                # Slices are consecutive, merge into a single slice
                if already_merged:
                    raise ValueError("Can not merge more than one dimension")
                result.append(builtins.slice(first.pop(), last.pop(), dim[0].step))
                already_merged = True
                continue
            
            elif starts == stops:
                # This is an empty dimension, keep it as is
                val = starts.pop()
                result.append(builtins.slice(val, val, dim[0].step))
                continue

        # If we got here, the slices are not compatible and can not be merged
        raise ValueError("The slices are not compatible and can not be merged")
    
    _cls = NDBlock if all(isinstance(arg, NDBlock) for arg in args) else NDSlice

    return _cls(*result)


class NDSlice(tuple):
    """A representation of a slice for N-dimensional arrays.

    This class is a tuple of integers, slices, and Ellipsis objects. It is used to
    represent a slice of an N-dimensional array, similar to how numpy uses slices.

    It also provides methods to convert to and from JSON and numpy-style string
    representations of slices. To encode Ellipsis in JSON, it is represented as
    {"step": 0}, which is not a valid builtin.slice.
    """

    def __new__(cls, *args: Union[int, builtins.slice, EllipsisType]) -> "NDSlice":
        if len(args) == 1 and (isinstance(args[0], (tuple, list)) or args[0] is None):
            return cls(*(args[0] or ()))
        if any(not isinstance(s, (int, builtins.slice, EllipsisType)) for s in args):
            raise TypeError(
                f"NDSlice expected int, slice or Ellipsis; got {args} instead. Use "
                "NDSlice.from_numpy_str() or NDSlice.from_json() to parse strings or JSON."
            )
        if len([None for x in args if x == Ellipsis]) > 1:
            raise ValueError(
                f"NDSlice can only contain one Ellipsis; got {args} instead."
            )

        return super().__new__(cls, tuple(args))

    def __bool__(self) -> bool:
        "NDSlice is considered empty if all slices are '...' or ':', returning entire array"
        full_slices = (
            builtins.slice(None),
            builtins.slice(0, None),
            builtins.slice(0, None, 1),
            builtins.slice(None, None, 1),
            Ellipsis,
        )
        return bool(super()) and not all(is_ellipsis(s) or s in full_slices for s in self)

    @classmethod
    def from_json(cls, ser: list[JSON]) -> "NDSlice":
        "Deserialize a json representation of an NDSlice"
        result = []
        for s in ser:
            if isinstance(s, dict):
                result.append(
                    builtins.slice(s.get("start"), s.get("stop"), s.get("step"))
                )
            elif isinstance(s, int):
                result.append(s)
            else:
                raise ValueError("Can not initialize NDSlice from %s", s)
        return cls(*result)

    def to_json(self, ndim: Optional[int] = None) -> list[JSON]:
        "Convert NDSlice into a JSON-serializable representation"
        if not ndim and (Ellipsis in self) and self[-1] != Ellipsis:
            raise ValueError(
                "Converting to JSON an NDSlice with Ellipsis in other than the last element "
                "requires the number of dimensions `ndim` to be specified."
            )
        elif ndim is not None and ndim < len([x for x in self if x != Ellipsis]):
            raise ValueError(
                "Specified number of dimensions `ndim` is less than the number of elements."
            )
        ndim = ndim or len(self)

        def convert_dimension(slc: Union[int, builtins.slice]) -> JSON:
            if isinstance(slc, builtins.slice):
                return (
                    ({} if slc.start is None else {"start": slc.start})
                    | ({} if slc.stop is None else {"stop": slc.stop})
                    | ({} if slc.step is None else {"step": slc.step})
                )
            elif isinstance(slc, int):
                return slc

        fwd, bwd = [], []
        for s in self:
            if s == Ellipsis:
                break
            fwd.append(convert_dimension(s))
        for s in reversed(self[len(fwd) + 1 :]):  # noqa: E203
            bwd.append(convert_dimension(s))

        return fwd + [{} for _ in range(ndim - len(fwd) - len(bwd))] + [*reversed(bwd)]

    @classmethod
    def from_numpy_str(cls, arg: str) -> "NDSlice":
        """Parse and convert a numpy-style string representation of a slice

        For example, '(1:3, 4, 1:5:2, ...)' is converted to (slice(1, 3), 4, slice(1, 5, 2), ...).
        """

        arg = arg.strip("(][)").replace(" ", "")

        if not arg:
            return cls()

        result = []
        for part in arg.split(","):
            if part.strip() == "...":
                result.append(Ellipsis)
            elif part.strip() in {":", "::"}:
                result.append(builtins.slice(None))
            elif m := re.match(r"^(-?\d+)::?$", part.strip()):
                # "start:" or "start::"
                start, stop = int(m.group(1)), None
                result.append(builtins.slice(start, stop))
            elif m := re.match(r"^:(-?\d+):?$", part.strip()):
                # ":stop" or ":stop:"
                start, stop = None, int(m.group(1))
                result.append(builtins.slice(start, stop))
            elif m := re.match(r"^(-?\d+):(-?\d+):(-?\d+)$", part.strip()):
                # "start:stop:step"
                start, stop, step = map(int, m.groups())
                result.append(builtins.slice(start, stop, step))
            elif m := re.match(r"^(-?\d+):(-?\d+):*$", part.strip()):
                # "start:stop" or "start:stop:"
                start, stop = map(int, m.groups())
                result.append(builtins.slice(start, stop))
            elif m := re.match(r"^:(-?\d+):(-?\d+)$", part.strip()):
                # ":stop:step"
                stop, step = map(int, m.groups())
                result.append(builtins.slice(None, stop, step))
            elif m := re.match(r"^::(-?\d+)$", part.strip()):
                # "::step"
                step = int(m.group(1))
                result.append(builtins.slice(None, None, step))
            elif m := re.match(r"^(-?\d+)::(-?\d+)$", part.strip()):
                # "start::step"
                start, step = map(int, m.groups())
                result.append(builtins.slice(start, None, step))
            elif m := re.match(r"^(-?\d+)$", part.strip()):
                result.append(int(m.group()))
            else:
                raise ValueError(f'Invalid slice part: "{part}"')
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

    def normalize(self) -> "NDSlice":
        "Normalize the NDSlice by converting slices like '1:' to 'slice(1, None)'"
        result = []
        for s in self:
            if isinstance(s, builtins.slice):
                result.append(builtins.slice(s.start or 0, s.stop, s.step or 1))
            else:
                result.append(s)

        return self.__class__(*result)

    def is_valid_for_shape(self, shape: tuple[int, ...]) -> bool:
        "Check if this NDSlice is valid for an array of the given shape"
        return (not self) or ndindex(self).isvalid(shape)
    
    def expand_for_shape(self, shape: tuple[int, ...]) -> "NDSlice":
        "Expand this NDSlice (remove ':' or '...') for an array of the given shape"
        return self.__class__(*ndindex(self).expand(shape).raw)
    
    def shape_after_slice(self, shape: tuple[int, ...]) -> tuple[int, ...]:
        "Calculate the shape after applying NDSlice to an array of the given shape"
        return ndindex(self).newshape(shape) if self else shape


class NDBlock(NDSlice):
    """A slice used to specify a block index, i.e. a slice over the chunks of an array.
    
    A major requirement is for the sliced blocks/chunks to be contiguous,
    i.e. the `step` parameter along each dimension, if specified, must be 1.
    """

    def __new__(cls, *args: Union[int, builtins.slice, EllipsisType]) -> "NDBlock":
        inst = super().__new__(cls, *args)

        for s in inst:
            if isinstance(s, builtins.slice) and s.step not in (None, 1):
                raise ValueError(
                    f"NDBlock can only contain slices with step 1; got {s} instead."
                )

        return inst

    def shape_from_chunks(self, chunks: Chunks) -> tuple[int, ...]:
        "Find the shape of the block of chunks for an array with the given chunks"
        expanded = self.expand_for_shape(tuple(map(len, chunks)))
        selected = tuple(ch[sl] for ch, sl in zip(chunks, expanded))
        return tuple(sum(ch) if isinstance(ch, tuple) else ch for ch in selected)
    
    def slice_from_chunks(self, chunks: Chunks) -> NDSlice:
        "Find the slice over entire array with given chunks that selects this block"
        expanded = self.expand_for_shape(tuple(map(len, chunks)))
        slice_ = []
        for sl, ch in zip(expanded, chunks):
            if isinstance(sl, int):
                start = sum(ch[:sl])
                slice_.append(slice(start, start + ch[sl]))
            elif isinstance(sl, builtins.slice):
                start = sum(ch[: sl.start]) if sl.start is not None else None
                stop = sum(ch[: sl.stop]) if sl.stop is not None else None
                slice_.append(slice(start, stop))

        return NDSlice(*slice_)
