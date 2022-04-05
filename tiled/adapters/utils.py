import operator


class IndexCallable:
    """Provide getitem syntax for functions

    >>> def inc(x):
    ...     return x + 1

    >>> I = IndexCallable(inc)
    >>> I[3]
    4

    Vendored from dask
    """

    __slots__ = ("fn",)

    def __init__(self, fn):
        self.fn = fn

    def __getitem__(self, key):
        return self.fn(key)


class IndexersMixin:
    """
    Provides slicable attributes keys_indexes, items_indexer, values_indexer.

    Must be mixed in with a class that defines methods:

    * ``_item_by_index``
    * ``_keys_slice``
    * ``_items_slice``
    """

    __slots__ = ("keys_indexer", "items_indexer", "values_indexer")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.keys_indexer = IndexCallable(self._keys_indexer)
        self.items_indexer = IndexCallable(self._items_indexer)
        self.values_indexer = IndexCallable(self._values_indexer)

    # There is some code reptition here, but let's live with it rather than add
    # yet more depth to the call stack....

    def _keys_indexer(self, index_or_slice):
        if isinstance(index_or_slice, int):
            if index_or_slice < 0:
                index_or_slice = -1 - index_or_slice
                direction = -1
            else:
                direction = 1
            key, _value = self._item_by_index(index_or_slice, direction)
            return key
        elif isinstance(index_or_slice, slice):
            start, stop, direction = slice_to_interval(index_or_slice)
            return list(self._keys_slice(start, stop, direction))
        else:
            raise TypeError(
                f"{index_or_slice} must be an int or slice, not {type(index_or_slice)}"
            )

    def _items_indexer(self, index_or_slice):
        if isinstance(index_or_slice, int):
            if index_or_slice < 0:
                index_or_slice = -1 - index_or_slice
                direction = -1
            else:
                direction = 1
            return self._item_by_index(index_or_slice, direction)
        elif isinstance(index_or_slice, slice):
            start, stop, direction = slice_to_interval(index_or_slice)
            return list(self._items_slice(start, stop, direction))
        else:
            raise TypeError(
                f"{index_or_slice} must be an int or slice, not {type(index_or_slice)}"
            )

    def _values_indexer(self, index_or_slice):
        if isinstance(index_or_slice, int):
            if index_or_slice < 0:
                index_or_slice = -1 - index_or_slice
                direction = -1
            else:
                direction = 1
            _key, value = self._item_by_index(index_or_slice, direction)
            return value
        elif isinstance(index_or_slice, slice):
            start, stop, direction = slice_to_interval(index_or_slice)
            return [value for _key, value in self._items_slice(start, stop, direction)]
        else:
            raise TypeError(
                f"{index_or_slice} must be an int or slice, not {type(index_or_slice)}"
            )


def slice_to_interval(slice_):
    """
    Convert slice object to (start, stop, direction).
    """
    step = slice_.step if slice_.step is not None else 1
    if step == 1:
        start = slice_.start if slice_.start is not None else 0
        if start < 0:
            raise ValueError(
                "Tree sequence slices with start < 0 must have step=-1. "
                f"Use for example [{slice_.start}:{slice_.stop}:-1]"
                "(This is a limitation of slicing on Tree sequences "
                "that does not apply to Python sequences in general.)"
            )
        if (slice_.stop is not None) and (slice_.stop < start):
            raise ValueError(
                "Tree sequence slices with step=1 must have stop >= start. "
                "(This is a limitation of slicing on Tree sequences "
                "that does not apply to Python sequences in general.)"
            )
        start_ = start
        stop_ = slice_.stop
        direction = 1
    elif step == -1:
        start = slice_.start if slice_.start is not None else -1
        if start >= 0:
            raise ValueError(
                "Tree sequence slices with start >= 0 must have step=1. "
                "(This is a limitation of slicing on Tree sequences "
                "that does not apply to Python sequences in general.)"
            )
        if slice_.stop is not None:
            if slice_.stop > start:
                raise ValueError(
                    "Tree sequence slices with step=-1 must have stop <= start."
                )
            stop_ = -(slice_.stop + 1)
        else:
            stop_ = slice_.stop
        start_ = -(start + 1)
        direction = -1
    else:
        raise ValueError(
            "Only step of 1 or -1 is supported in a Tree sequence slice. "
            f"Step {slice_.step} is disallowed."
        )
    assert start_ >= 0
    assert (stop_ is None) or (stop_ >= start_)
    return start_, stop_, direction


def tree_repr(tree, sample):
    sample_reprs = list(map(repr, sample))
    out = f"<{type(tree).__name__} {{"
    # Always show at least one.
    if sample_reprs:
        out += sample_reprs[0]
    # And then show as many more as we can fit on one line.
    counter = 1
    for sample_repr in sample_reprs[1:]:
        if len(out) + len(sample_repr) > 60:  # character count
            break
        out += ", " + sample_repr
        counter += 1
    approx_len = operator.length_hint(tree)  # cheaper to compute than len(tree)
    # Are there more in the tree that what we displayed above?
    if approx_len > counter:
        out += f", ...}} ~{approx_len} entries>"
    else:
        out += "}>"
    return out
