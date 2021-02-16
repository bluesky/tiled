"""
This contains ideas that will probably be abandoned but I'd like to keep handy
until the prototype solidifies a bit more.

Currently it contains code more making slicing recursively lazy.
"""
import collections.abc

from .in_memory_catalog import slice_to_interval


def _compose_intervals(a, b):
    a_start, a_stop = a
    b_start, b_stop = b
    if a_start is None:
        if b_start is None:
            start = 0
        else:
            start = b_start
    else:
        if b_start is None:
            start = a_start
        else:
            start = a_start + b_start
    if a_stop is None:
        if b_stop is None:
            stop = None
        else:
            stop = b_stop + a_start
    else:
        if b_stop is None:
            stop = a_stop
        else:
            stop = min(a_stop, b_stop + a_start)
    return start, stop


class CatalogBaseSequence(collections.abc.Sequence):
    "Base class for Keys, Values, Items Sequences."

    def __init__(self, ancestor, start=0, stop=None):
        self._ancestor = ancestor
        self._start = int(start or 0)
        if stop is not None:
            stop = int(stop)
        self._stop = stop

    def __repr__(self):
        return f"<{type(self).__name__}({list(self)!r})>"

    def __len__(self):
        len_ = len(self._ancestor) - self._start
        if self._stop is not None and (len_ > (self._stop - self._start)):
            return self._stop - self._start
        else:
            return len_

    def __getitem__(self, index):
        "Subclasses handle the case of an integer index."
        if isinstance(index, slice):
            start, stop = slice_to_interval(index)
            # Return another instance of type(self), progpagating forward a
            # reference to self and the sub-slicing specified by index.
            return type(self)(self, start, stop)
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")

    def _item_by_index(self, index):
        # Recurse
        return self._ancestor._item_by_index(index + self._start)

    def _items_slice(self, start, stop):
        # Recurse
        agg_start, agg_stop = _compose_intervals(
            (self._start, self._stop), (start, stop)
        )
        return self._ancestor._items_slice(agg_start, agg_stop)

    def _keys_slice(self, start, stop):
        # Recurse
        agg_start, agg_stop = _compose_intervals(
            (self._start, self._stop), (start, stop)
        )
        return self._ancestor._keys_slice(agg_start, agg_stop)


class CatalogKeysSequence(CatalogBaseSequence):
    def __iter__(self):
        return self._ancestor._keys_slice(self._start, self._stop)

    def __getitem__(self, index):
        if isinstance(index, int):
            key, _value = self._item_by_index(index)
            return key
        return super().__getitem__(index)


class CatalogItemsSequence(CatalogBaseSequence):
    def __iter__(self):
        return self._ancestor._items_slice(self._start, self._stop)

    def __getitem__(self, index):
        if isinstance(index, int):
            return self._item_by_index(index)
        return super().__getitem__(index)


class CatalogValuesSequence(CatalogBaseSequence):
    def __iter__(self):
        # Extract just the value for the iterable of (key, value) items.
        return (
            value
            for _key, value in self._ancestor._items_slice(self._start, self._stop)
        )

    def __getitem__(self, index):
        if isinstance(index, int):
            # Extract just the value from the item.
            _key, value = self._item_by_index(index)
            return value
        return super().__getitem__(index)
