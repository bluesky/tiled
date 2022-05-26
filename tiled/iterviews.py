"""
Iterables for KeysView, ValuesView, ItemsView that are sliceable.
"""


class IterViewBase:

    __slots__ = ("_get_length",)

    def __init__(self, get_length):
        self._get_length = get_length

    def __repr__(self):
        return f"<{type(self).__name__}>"

    # Convenience aliases

    def first(self):
        return self[0]

    def last(self):
        return self[-1]

    def head(self, n=5):
        return self[:n]

    def tail(self, n=5):
        return list(reversed(self[-1 : -(n + 1) : -1]))  # noqa: E203

    def __init_subclass__(cls, *args, **kwargs):
        cls.first.__doc__ = f"Get the first {cls._name}."
        cls.last.__doc__ = f"Get the last {cls._name}."
        cls.head.__doc__ = f"Get the first N {cls._name}s."
        cls.tail.__doc__ = f"Get the last N {cls._name}s."

    def __len__(self):
        return self._get_length()


class KeysView(IterViewBase):
    """
    A sliceable, iterable view of keys.
    """

    __slots__ = ("_keys_slice",)
    _name = "key"

    def __init__(self, get_length, keys_slice):
        self._keys_slice = keys_slice
        super().__init__(get_length)

    def __getitem__(self, index_or_slice):
        if isinstance(index_or_slice, int):
            if index_or_slice < 0:
                index_or_slice = -1 - index_or_slice
                direction = -1
            else:
                direction = 1
            keys = list(self._keys_slice(index_or_slice, 1 + index_or_slice, direction))
            try:
                (key,) = keys
            except ValueError:
                raise IndexError("Index out of range")
            return key
        elif isinstance(index_or_slice, slice):
            start, stop, direction = slice_to_interval(index_or_slice)
            return list(self._keys_slice(start, stop, direction))
        else:
            raise TypeError(
                f"{index_or_slice} must be an int or slice, not {type(index_or_slice)}"
            )

    def __iter__(self):
        yield from self._keys_slice(0, None, 1)


class ItemsView(IterViewBase):
    """
    A sliceable, iterable view of (key, value) pairs.
    """

    __slots__ = ("_items_slice",)
    _name = "item"

    def __init__(self, get_length, items_slice):
        self._items_slice = items_slice
        super().__init__(get_length)

    def __getitem__(self, index_or_slice):
        if isinstance(index_or_slice, int):
            if index_or_slice < 0:
                index_or_slice = -1 - index_or_slice
                direction = -1
            else:
                direction = 1
            items = list(
                self._items_slice(index_or_slice, 1 + index_or_slice, direction)
            )
            try:
                (item,) = items
            except ValueError:
                raise IndexError("Index out of range")
            return item
        elif isinstance(index_or_slice, slice):
            start, stop, direction = slice_to_interval(index_or_slice)
            return list(self._items_slice(start, stop, direction))
        else:
            raise TypeError(
                f"{index_or_slice} must be an int or slice, not {type(index_or_slice)}"
            )

    def __iter__(self):
        yield from self._items_slice(0, None, 1)


class ValuesView(IterViewBase):
    """
    A sliceable, iterable view of values.
    """

    __slots__ = ("_items_slice",)
    _name = "value"

    def __init__(self, get_length, items_slice):
        self._items_slice = items_slice
        super().__init__(get_length)

    def __getitem__(self, index_or_slice):
        if isinstance(index_or_slice, int):
            if index_or_slice < 0:
                index_or_slice = -1 - index_or_slice
                direction = -1
            else:
                direction = 1
            items = list(
                self._items_slice(index_or_slice, 1 + index_or_slice, direction)
            )
            try:
                (item,) = items
            except ValueError:
                raise IndexError("Index out of range")
            _key, value = item
            return value
        elif isinstance(index_or_slice, slice):
            start, stop, direction = slice_to_interval(index_or_slice)
            return [value for _key, value in self._items_slice(start, stop, direction)]
        else:
            raise TypeError(
                f"{index_or_slice} must be an int or slice, not {type(index_or_slice)}"
            )

    def __iter__(self):
        for key, value in self._items_slice(0, None, 1):
            yield value


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
