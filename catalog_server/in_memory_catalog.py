import collections.abc
import itertools

from .query_registration import DictView, QueryTranslationRegistry
from .queries import FullText, KeyLookup


class Catalog(collections.abc.Mapping):

    __slots__ = (
        "_mapping",
        "_metadata",
        "keys_indexer",
        "items_indexer",
        "values_indexer",
    )
    # Define classmethods for managing what queries this Catalog knows.
    __query_registry = QueryTranslationRegistry()
    register_query = __query_registry.register
    register_query_lazy = __query_registry.register_lazy

    def __init__(self, mapping, metadata=None):
        self._mapping = mapping
        self._metadata = metadata or {}
        self.keys_indexer = IndexCallable(self._keys_indexer)
        self.items_indexer = IndexCallable(self._items_indexer)
        self.values_indexer = IndexCallable(self._values_indexer)

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def __repr__(self):
        return f"<{type(self).__name__}({set(self)!r})>"

    def __getitem__(self, key):
        return self._mapping[key]

    def __iter__(self):
        yield from self._mapping

    def __len__(self):
        return len(self._mapping)

    def search(self, query):
        """
        Return a Catalog with a subset of the mapping.
        """
        return self.__query_registry(query, self)

    def _keys_slice(self, start, stop):
        yield from itertools.islice(
            self._mapping.keys(),
            start,
            stop,
        )

    def _items_slice(self, start, stop):
        # A goal of this implementation is to avoid iterating over
        # self._mapping.values() because self._mapping may be a LazyMap which
        # only constructs its values at access time. With this in mind, we
        # identify the key(s) of interest and then only access those values.
        yield from ((key, self._mapping[key]) for key in self._keys_slice(start, stop))

    def _item_by_index(self, index):
        if index >= len(self):
            raise IndexError(f"index {index} out of range for length {len(self)}")
        key = next(itertools.islice(self._mapping.keys(), index, 1 + index))
        return (key, self._mapping[key])

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


class IndexCallable:
    """Provide getitem syntax for functions

    >>> def inc(x):
    ...     return x + 1

    >>> I = IndexCallable(inc)
    >>> I[3]
    4
    """

    __slots__ = ("fn",)

    def __init__(self, fn):
        self.fn = fn

    def __getitem__(self, key):
        return self.fn(key)


def walk_string_values(tree, node=None):
    """
    >>> list(
    ...     walk_string_values(
    ...         {'a': {'b': {'c': 'apple', 'd': 'banana'},
    ...          'e': ['cat', 'dog']}, 'f': 'elephant'}
    ...     )
    ... )
    ['apple', 'banana', 'cat', 'dog', 'elephant']
    """
    if node is None:
        for node in tree:
            yield from walk_string_values(tree, node)
    else:
        value = tree[node]
        if isinstance(value, str):
            yield value
        elif hasattr(value, "items"):
            for k, v in value.items():
                yield from walk_string_values(value, k)
        elif isinstance(value, collections.abc.Iterable):
            for item in value:
                if isinstance(item, str):
                    yield item


def full_text_search(query, catalog):
    matches = {}
    query_words = set(query.text.lower().split())
    for key, value in catalog.items():
        words = set(
            word
            for s in walk_string_values(value.metadata)
            for word in s.lower().split()
        )
        # Note that `not set.isdisjoint` is faster than `set.intersection`. At
        # the C level, `isdisjoint` loops over the set until it finds one match,
        # and then bails, whereas `intersection` proceeds to find all matches.
        if not words.isdisjoint(query_words):
            matches[key] = value
    return type(catalog)(matches)


def key_lookup(query, catalog):
    try:
        matches = {query.key: catalog[query.key]}
    except KeyError:
        matches = {}
    return type(catalog)(matches)


Catalog.register_query(FullText, full_text_search)
Catalog.register_query(KeyLookup, key_lookup)
