import collections.abc
import itertools

from queries import DictView, QueryTranslationRegistry, Text


class Catalog(collections.abc.Mapping):

    __slots__ = ("_mapping", "_metadata")

    # Define classmethods for managing what queries this Catalog knows.
    __query_registry = QueryTranslationRegistry()
    register_query = __query_registry.register
    register_query_lazy = __query_registry.register_lazy

    def __init__(self, mapping, metadata=None):
        self._mapping = mapping
        self._metadata = metadata or {}

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def __repr__(self):
        return f"<{type(self).__name__}" "({...})>"

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
        yield from (
            (key, self._mapping[key]) for key in self._keys_in_interval(start, stop)
        )

    def _item_by_index(self, index):
        if index > len(self):
            raise IndexError(f"index {index} out of range for length {len(self)}")
        key = next(itertools.islice(self._mapping.keys(), index, 1 + index))
        return (key, self._mapping[key])

    @property
    def keys_indexer(self):
        return CatalogKeysSequence(self)


class CatalogKeysSequence(collections.abc.Sequence):
    def __init__(self, ancestor, start=0, stop=None):
        self._ancestor = ancestor
        self._start = start
        self._stop = stop

    def __repr__(self):
        return f"<{type(self).__name__}([...])>"

    def __len__(self):
        ...

    def __getitem__(self, index):
        if isinstance(index, int):
            key, value = self._ancestor._item_by_index(index + self._start)
            return key
        elif isinstance(index, slice):
            return type(self)(self, index.start + self._start, index.stop + self._start)
        else:
            raise TypeError(f"{type(self).__name__} index must be integer or slice.")

    def _item_by_index(self, index):
        # Recurse
        return self._ancestor._item_by_index(index + self._start)

    def _keys_slice(self, start, stop):
        # Recurse
        return self._ancestor._keys_slice(start + self._start, stop + self._start)

    def __iter__(self):
        return self._ancestor._keys_slice(self._start, self._stop)


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
        # Note that `not set.disjoint` is faster than `set.intersection`. At
        # the C level, `disjoint` loops over the set until it finds one match,
        # and then bails, whereas `intersection` proceeds to find all matches.
        if not words.disjoint(query_words):
            matches[key] = value
    return type(catalog)(matches)


Catalog.register_query(Text, full_text_search)
