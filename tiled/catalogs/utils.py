import operator

from ..utils import Sentinel


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

    __slots__ = (
        "keys_indexer",
        "items_indexer",
        "values_indexer",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.keys_indexer = IndexCallable(self._keys_indexer)
        self.items_indexer = IndexCallable(self._items_indexer)
        self.values_indexer = IndexCallable(self._values_indexer)

    # There is some code reptition here, but let's live with it rather than add
    # yet more depth to the call stack....

    def _keys_indexer(self, index):
        if isinstance(index, int):
            key, _value = self._item_by_index(index)
            return key
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return list(self._keys_slice(start, stop))
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")

    def _items_indexer(self, index):
        if isinstance(index, int):
            return self._item_by_index(index)
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return list(self._items_slice(start, stop))
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")

    def _values_indexer(self, index):
        if isinstance(index, int):
            _key, value = self._item_by_index(index)
            return value
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return [value for _key, value in self._items_slice(start, stop)]
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")


def slice_to_interval(index):
    "Check that slice is supported; then return (start, stop)."
    if index.start is None:
        start = 0
    elif index.start < 0:
        raise NotImplementedError
    else:
        start = index.start
    if index.stop is not None:
        if index.stop < 0:
            raise NotImplementedError
    stop = index.stop
    return start, stop


UNCHANGED = Sentinel("UNCHANGED")


def catalog_repr(catalog, sample):
    sample_reprs = list(map(repr, sample))
    out = f"<{type(catalog).__name__} {{"
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
    approx_len = operator.length_hint(catalog)  # cheaper to compute than len(catalog)
    # Are there more in the catalog that what we displayed above?
    if approx_len > counter:
        out += f", ...}} ~{approx_len} entries>"
    else:
        out += "}>"
    return out
