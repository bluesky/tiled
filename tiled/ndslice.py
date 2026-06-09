import bisect
import builtins
import itertools
import math
import re
from typing import Iterable, Optional, Union

from ndindex import ndindex

from .type_aliases import JSON, Chunks, EllipsisType


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
            elif vals == set(range(min(vals), max(vals) + 1)):
                # Integers are consecutive, merge into a slice
                if already_merged:
                    raise ValueError("Can not merge more than one dimension")
                result.append(builtins.slice(min(vals), max(vals) + 1))
                already_merged = True
                continue
            else:
                raise ValueError(
                    "Integer dimensions must match exactly "
                    "or be consecutive and have no gaps"
                )

        # Some dimensions could be integers and some are slices
        if any(isinstance(d, int) for d in dim):
            dim = tuple(
                builtins.slice(d, d + 1) if isinstance(d, int) else d for d in dim
            )

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

            elif (
                (len(starts) == len(stops))
                and (len(first := starts - stops) == 1)
                and (len(last := stops - starts) == 1)
            ):
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


def split_1d(start, stop, step, max_len: int, pref_splits: Optional[list[int]] = None):
    """Split a 1D slice into sub-slices that do not exceed max_len steps.

    Splits are chosen close to the ideal equal partition. If preferred
    split points are provided, the closest one to the ideal point is used
    as long as it does not violate the min/max constraints.
    """

    # Total number of steps and max steps per split
    total_steps = math.ceil(abs(stop - start) / abs(step))

    # Convert preferred points to index space
    pref_indx = sorted(
        (x - start) // step
        for x in (pref_splits or [])
        if x in range(start, stop, step)
    )

    result, crnt_indx, _pi = [], 0, 0
    while crnt_indx + max_len < total_steps:
        # Compute ideal next index for equal partitioning of the remaining steps
        steps_remained = total_steps - crnt_indx
        num_splits = max(1, math.ceil(steps_remained / max_len))
        ideal_split_size = math.ceil(steps_remained / num_splits)
        next_indx = crnt_indx + ideal_split_size

        # Check if there are preferred points between the current index and the max allowed
        if pref_indx:
            pref_best = None
            while _pi < len(pref_indx) and pref_indx[_pi] <= crnt_indx + max_len:
                if pref_indx[_pi] > crnt_indx:
                    pref_best = pref_best or pref_indx[_pi]
                    if abs(pref_indx[_pi] - next_indx) < abs(pref_best - next_indx):
                        pref_best = pref_indx[_pi]
                _pi += 1
            next_indx = pref_best or next_indx

        result.append((start + crnt_indx * step, start + next_indx * step))
        crnt_indx = next_indx

    result.append((start + crnt_indx * step, stop))

    return result


def split_slice(
    arr_slice: "NDSlice", max_size: int, pref_splits: Optional[list[list[int]]] = None
) -> dict[tuple[int, ...], "NDSlice"]:
    """Split an N-dimensional slice into smaller slices that do not exceed max_size

    Splits are chosen by iteratively subslicing along the most chunked or longest
    dimension until the resulting slices are all under the max_size limit.

    Parameters
    ----------
    arr_slice : NDSlice
        The N-dimensional slice to split. The slice must be expanded to account for
        the specific shape (i.e. not contain any ":" or "..."). Integer dimensions
        are allowed.
    max_size : int
        The maximum allowed size (number of elements) for each resulting slice.
    pref_splits : list of list of int, optional
        Preferred split points for each dimension.

    Returns
    -------
    dict[tuple[int, ...], NDSlice]
        A dictionary mapping from index tuples to the corresponding NDSlice objects.
    """

    # Remove singleton dimensions and replace with slices for simplicity. Revert later
    is_int_dim = [isinstance(s, int) for s in arr_slice]
    arr_slice = arr_slice.unsqueeze()
    ndim = len(arr_slice)

    # Make sure preferred split points align with the step grid and within the bounds
    if pref_splits is not None:
        pref_splits = [
            [
                x
                for x in bnd
                if x in range(slc.start, slc.stop, slc.step or 1) and x != slc.start
            ]
            for bnd, slc in zip(pref_splits, arr_slice)
        ]

    # Start with the most chunked or longest dimension and subslice it
    sorting_order = (
        [len(ps) for ps in pref_splits]
        if pref_splits is not None
        else [len(range(s.start, s.stop, s.step or 1)) for s in arr_slice]
    )
    result = [[s] for s in arr_slice]
    for d in sorted(range(ndim), key=lambda i: sorting_order[i], reverse=True):
        # Find the size of largest block along all other dimensions, excluding d
        max_other = math.prod(
            [
                max(len(range(s.start, s.stop, s.step or 1)) for s in result[_d])
                for _d in range(ndim)
                if _d != d
            ]
        )
        slc = result[d].pop()

        # Use maximum length along this dimension that keeps the slice under the limit
        splits = split_1d(
            slc.start,
            slc.stop,
            slc.step or 1,
            max_len=max(1, int(max_size / max_other)),
            pref_splits=pref_splits[d] if pref_splits is not None else None,
        )
        result[d].extend([builtins.slice(a, b, slc.step) for a, b in splits])

        # Check if we need further subslicing along other dimensions
        max_crnt = max(len(range(s.start, s.stop, s.step or 1)) for s in result[d])
        if max_crnt * max_other <= max_size:
            break

    # Replace (squeeze) any singleton slices if they were integers originally
    result = [[res[0].start] if flag else res for res, flag in zip(result, is_int_dim)]

    # Form the dict of Cartesian products
    keys = itertools.product(
        *(range(len(x)) for x in result if not isinstance(x[0], int))
    )
    vals = itertools.product(*result)

    return {k: NDSlice(v) for k, v in zip(keys, vals)}


def _slc_with_int(slc: builtins.slice, idx: int) -> int:
    "Compose a (1D) slice with an integer index"

    if slc.start is None or slc.stop is None:
        raise ValueError("Composition with relative indexing is not supported.")

    start, stop, step = slc.start, slc.stop, slc.step or 1

    # Estimate maximum length of the resulting shape after slice
    if step > 0:
        length = max(0, (stop - start + step - 1) // step)
    else:
        length = max(0, (start - stop - step - 1) // (-step))

    # Normalize negative index
    if idx < 0:
        idx += length

    if idx < 0 or idx >= length:
        raise ValueError("Composition with out-of-bounds index")

    return start + step * idx


def _slc_with_slc(slc1: builtins.slice, slc2: builtins.slice) -> builtins.slice:
    "Compose a (1D) slice with another slice"

    # Normalize first slice
    if slc1.start is None or slc1.stop is None:
        raise ValueError("Composition with relative indexing is not supported.")
    start1, stop1, step1 = slc1.start, slc1.stop, slc1.step or 1

    # Estimate maximum length of the resulting shape after first slice
    if step1 > 0:
        length = max(0, (stop1 - start1 + step1 - 1) // step1)
    else:
        length = max(0, (start1 - stop1 - step1 - 1) // (-step1))

    # Normalize second slice relative to the length of the first slice
    start2, stop2, step2 = slc2.indices(length)

    # Compose the two normalized slices
    new_start = start1 + step1 * start2
    new_step = step1 * step2
    new_stop = start1 + step1 * stop2

    return builtins.slice(new_start, new_stop, new_step)


def _is_scalar_slice(
    slc: Union[int, builtins.slice], length: Optional[int] = None
) -> bool:
    "Best effort to check if a slice selects a single element (scalar) along one dimension"
    if length is not None and isinstance(slc, builtins.slice):
        start, stop, step = slc.indices(length)
        return len(range(start, stop, step)) == 1

    return isinstance(slc, int) or (
        isinstance(slc, slice)
        and slc.start is not None
        and slc.stop is not None
        and slc.stop == slc.start + 1
    )


def compose_slices(slc1: "NDSlice", slc2: "NDSlice") -> "NDSlice":
    """Compose two NDSlices

    Creates a single NDSlice that is the composition of the input slices, i.e. equivalent
    to applying them sequentially. For example, if arr is an array, then
    arr[slc1][slc2] is equivalent to arr[compose_slices(slc1, slc2)].

    Note that Ellipsis in any of the slices may break the composition rule for
    arrays of certain shapes, so it is recommended to expand the slices for the specific
    shape before composing them.
    """

    if (not slc1) or (not slc2):
        # If either slice is empty, the result is the other slice (which may also be empty)
        return slc1 or slc2

    if not slc1.is_expanded():
        # The left slice contains Ellipsis or the resulting shape is ambiguous
        raise ValueError(
            "Composition of slices with undetermined shape is not supported."
        )

    res_rgt, i1l = [], len(slc1)
    res_lft, i2r = [], 0
    if any(is_ellipsis(s) for s in slc2) and not is_ellipsis(slc2[-1]):
        # The right slice contains Ellipsis in other than the last position
        dim1 = sum(1 for s in slc1 if not isinstance(s, int))
        if dim1 < len(slc2) - 1:
            raise ValueError(
                "Composition with Ellipsis in the right slice is only supported if it is "
                "the last element, or the left slice has enough dimensions to cover all "
                "non-Ellipsis elements of the right slice."
            )

        # Start assembling the result from the right
        for s2 in reversed(slc2):
            if is_ellipsis(s2):
                break

            # Chose the next right-most (non-integer) item from the left slice
            i1l -= 1
            s1 = slc1[i1l]
            while isinstance(s1, int) and i1l > 0:
                # Repeat: add the item, move the left counter until we find a non-integer item
                res_rgt.append(s1)
                i1l -= 1
                s1 = slc1[i1l]

            # Now compose s1 and s2 and add to the result from the right
            if isinstance(s2, int):
                res_rgt.append(_slc_with_int(s1, s2))
            elif isinstance(s2, builtins.slice):
                res_rgt.append(_slc_with_slc(s1, s2))

    # All the right slice until Ellipsis is processed; and the remaining items from the left
    for s1 in slc1[:i1l]:
        # Left item is an integer, add it to the result and move on
        if isinstance(s1, int) or i2r >= len(slc2):
            res_lft.append(s1)
            continue

        # Left item is a slice, compose it with the next right item if there are any remaining
        s2 = slc2[i2r]
        if isinstance(s2, int):
            res_lft.append(_slc_with_int(s1, s2))
        elif isinstance(s2, builtins.slice):
            res_lft.append(_slc_with_slc(s1, s2))
        elif is_ellipsis(s2) or (len(slc2) == i2r + 1):
            res_lft.append(s1)
            continue

        i2r += 1

    _cls = (
        NDBlock if isinstance(slc1, NDBlock) and isinstance(slc2, NDBlock) else NDSlice
    )

    return _cls(*res_lft, *reversed(res_rgt))


class NDSlice(tuple):
    """A representation of a slice for N-dimensional arrays.

    This class is a tuple of integers, slices, and Ellipsis objects. It is used to
    represent a slice of an N-dimensional array, similar to how numpy uses slices.

    It also provides methods to convert to and from JSON and numpy-style string
    representations of slices. To encode Ellipsis in JSON, it is represented as
    {"step": 0}, which is not a valid builtin.slice.
    """

    def __new__(
        cls, *args: Union[int, builtins.slice, EllipsisType, Iterable, None]
    ) -> "NDSlice":
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
        return bool(super()) and not all(
            is_ellipsis(s) or s in full_slices for s in self
        )

    def __getitem__(self, key):
        "An element of this NDSlice or the composition with another NDSlice"

        if is_ellipsis(key):
            return self

        if isinstance(key, NDSlice):
            # Composition of slices: arr[slc1][slc2] is equivalent to arr[slc1[slc2]]
            return compose_slices(self, key)

        # Just return the element if it's not a slice composition
        return super().__getitem__(key)

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

    def is_expanded(self) -> bool:
        """Check the _necessary_ conditions to determine the resulting shape from the slice alone

        If True, the resulting shape can be determined without knowing the shape of the array
        this slice is applied to, i.e. it does not contain any ':', '...', or relative indexing.

        NOTE: This is a necessary but not sufficient condition; e.g. `slice(0, 5)` applied to an
        array of shape (3,) will still result in a shape of (3,), not (5,).
        """

        def _is_absolute(s: builtins.slice) -> bool:
            return (s.start or 0) >= 0 and (s.stop is not None and s.stop >= 0)

        return all(
            (not is_ellipsis(s))
            and (_is_absolute(s) if isinstance(s, builtins.slice) else True)
            for s in self
        )

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

    def is_valid_for_shape(self, shape: tuple[int, ...]) -> bool:
        "Check if this NDSlice is valid for an array of the given shape"
        return (not self) or ndindex(self).isvalid(shape)

    def expand_for_shape(self, shape: tuple[int, ...]) -> "NDSlice":
        "Expand this NDSlice (remove ':' or '...') for an array of the given shape"
        return self.__class__(*ndindex(self).expand(shape).raw)

    def shape_after_slice(self, shape: tuple[int, ...]) -> tuple[int, ...]:
        "Calculate the shape after applying NDSlice to an array of the given shape"
        return ndindex(self).newshape(shape) if self else shape

    def unsqueeze(self) -> "NDSlice":
        "Convert all integer dims to slices of length 1 to preserve the dimensionality"
        return self.__class__(
            *(builtins.slice(s, s + 1, 1) if isinstance(s, int) else s for s in self)
        )

    def is_scalar(self, shape: tuple[int, ...]) -> bool:
        "Check if this NDSlice selects a single element (scalar) from an array of the given shape"
        return (len(self) == len(shape)) and all(
            _is_scalar_slice(s, length=l) for s, l in zip(self, shape)
        )


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

    def chunk_indices(self, chunks: Chunks) -> list[tuple[int, ...]]:
        """Return a list of all n-dimensional chunk indices in this block.

        Expands any slice dimensions into individual indices while keeping
        integer dimensions as-is. The result is a list of tuples where each
        tuple represents the chunk index for one block.

        Parameters
        ----------
        chunks : Chunks
            The chunking scheme to determine the valid range of chunk indices.

        Returns
        -------
        list[tuple[int, ...]]
            List of n-dimensional chunk index tuples.
        """
        # Expand to get actual indices for each dimension
        expanded = self.expand_for_shape(tuple(len(ch) for ch in chunks))

        # Convert each dimension to a list of indices
        dim_indices = []
        for item in expanded:
            if isinstance(item, int):
                dim_indices.append([item])
            elif isinstance(item, builtins.slice):
                # Extract start, stop from the slice (step is guaranteed to be 1 or None)
                start = item.start if item.start is not None else 0
                stop = item.stop
                dim_indices.append(list(range(start, stop)))

        # Generate all combinations (Cartesian product)
        return sorted(itertools.product(*dim_indices))


def block_for_slice(
    chunks: Chunks, slice: Optional[Union[tuple, "NDSlice"]] = None
) -> tuple["NDBlock", "NDSlice"]:
    """Convert a slice on a chunked array into block indices and a slice within the block

    Given an array's chunking scheme and a slice to apply, this function determines:
    1. Which blocks (chunks) are touched by the slice (returned as NDBlock)
    2. What slice to apply within the concatenated blocks to get the final result

    This is useful for reading data using block-based access (e.g., read_block()) instead
    of full array slicing, which can be more efficient for large datasets by avoiding
    expensive path resolution operations.

    Parameters
    ----------
    chunks : Chunks
        The chunking scheme as a tuple of tuples, where chunks[i] gives the sizes
        of chunks along dimension i.
    slice : tuple or NDSlice, optional
        The slice to apply to the full array.

    Returns
    -------
    block : NDBlock
        The contiguous block indices touched by the slice.
    slice_within_blocks : NDSlice
        The slice to apply to the array formed by concatenating the selected blocks
        to produce the final result.
    """

    # If empty slice, return all blocks and identity slice
    slice = NDSlice(slice or ())
    if not slice:
        return (
            NDBlock(*[builtins.slice(0, len(dim_chunks)) for dim_chunks in chunks]),
            NDSlice(),
        )

    # Expand the slice for the specific shape (handles Ellipsis, etc.)
    shape = tuple(sum(dim_chunks) for dim_chunks in chunks)
    slice = slice.expand_for_shape(shape)

    # For each dimension, determine which chunks are touched and how to slice concatenated blocks
    block_slices, adjusted_slices = [], []
    for dim_idx, (slc, dim_chunks) in enumerate(zip(slice, chunks)):
        # Compute chunk boundaries: [0, chunks[0], chunks[0]+chunks[1], ...]
        chunk_bounds = [0, *itertools.accumulate(dim_chunks)]

        if isinstance(slc, int):
            # Single index - find which chunk it falls in using binary search
            chunk_idx = bisect.bisect_right(chunk_bounds, slc) - 1
            if chunk_idx < 0 or chunk_idx >= len(dim_chunks):
                raise IndexError(
                    f"Index {slc} out of bounds for dimension {dim_idx} with shape {shape[dim_idx]}"
                )
            block_slices.append(chunk_idx)
            adjusted_slices.append(slc - chunk_bounds[chunk_idx])

        elif isinstance(slc, builtins.slice):
            # Normalize slice indices for this dimension
            start, stop, step = slc.indices(shape[dim_idx])

            if step == 0:
                raise ValueError("slice step cannot be zero")

            # Find first and last chunks touched by this slice
            if step > 0:
                if start >= stop:
                    block_slices.append(builtins.slice(0, 0))
                    adjusted_slices.append(builtins.slice(0, 0))
                    continue  # Empty slice
                first_chunk = bisect.bisect_right(chunk_bounds, start) - 1
                last_chunk = bisect.bisect_right(chunk_bounds, stop - 1) - 1
            else:  # step < 0
                if start <= stop:
                    block_slices.append(builtins.slice(0, 0))
                    adjusted_slices.append(builtins.slice(0, 0))
                    continue  # Empty slice
                # Negative steps traverse from start down to stop (exclusive)
                # so elements go from start to stop+1
                first_chunk = bisect.bisect_right(chunk_bounds, start) - 1
                last_chunk = bisect.bisect_right(chunk_bounds, stop + 1) - 1
                # Ensure first_chunk <= last_chunk for contiguous block range
                first_chunk, last_chunk = min(first_chunk, last_chunk), max(
                    first_chunk, last_chunk
                )

            # Block is the contiguous range of chunks
            if first_chunk == last_chunk:
                block_slices.append(first_chunk)
            else:
                block_slices.append(builtins.slice(first_chunk, last_chunk + 1))

            # Compute adjusted slice within concatenated blocks
            first_chunk_start = chunk_bounds[first_chunk]
            adjusted_start = start - first_chunk_start
            adjusted_stop = stop - first_chunk_start

            # Special case: for negative steps, if stop would go before the start of
            # the first chunk, set it to None to indicate "go to the beginning"
            if step < 0 and stop < first_chunk_start:
                adjusted_stop = None

            # Normalize step=1 to step=None for consistency
            normalized_step = None if step == 1 else step
            adjusted_slices.append(
                builtins.slice(adjusted_start, adjusted_stop, normalized_step)
            )

    return NDBlock(*block_slices), NDSlice(*adjusted_slices)


def build_nested_grid(indices, callable=None, dim=0):
    """Build a nested list structure from flat N-dimensional indices.

    This function organizes a flat list of N-dimensional indices into a nested
    list structure suitable for np.block(). The nesting depth matches the
    dimensionality of the indices, with each level corresponding to one dimension.

    This is particularly useful when you have a collection of array chunks indexed
    by multi-dimensional coordinates and want to concatenate them efficiently using
    np.block().

    Parameters
    ----------
    indices : list[tuple[int, ...]]
        A list of N-dimensional index tuples. The indices should be sorted in
        lexicographic order (though groupby will work with any order that groups
        consecutive identical prefixes together).
    callable : callable, optional
        Optional function to apply to each index tuple at the leaf level.
        If None, the index tuple itself is returned. This is useful for
        retrieving actual data arrays corresponding to each index.
        For example: lambda idx: array_dict[idx]
    dim : int, optional
        Internal parameter used during recursion to track current dimension.
        Should not be set by users (defaults to 0).

    Returns
    -------
    nested_list : list or object
        A nested list structure where the nesting depth equals len(indices[0]).
        At the leaf level, either the index tuples themselves or the result of
        callable(index) is stored.
    """
    if dim == len(indices[0]):
        return callable(indices[0]) if callable else indices[0]

    groups = [
        build_nested_grid(list(g), callable, dim + 1)
        for _, g in itertools.groupby(indices, key=lambda p: p[dim])
    ]

    return groups
