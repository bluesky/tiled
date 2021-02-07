import collections.abc
import itertools

from queries import DictView, QueryTranslationRegistry, Text


class Catalog(collections.abc.Mapping):

    __slots__ = (
        "items_indexer",
        "keys_indexer",
        "_mapping",
        "_metadata",
        "values_indexer",
    )

    # Define classmethods for managing what queries this Catalog knows.
    __query_registry = QueryTranslationRegistry()
    register_query = __query_registry.register
    register_query_lazy = __query_registry.register_lazy

    def __init__(self, mapping, metadata=None):
        self._mapping = mapping
        self.items_indexer = ItemsIndexer(mapping)
        self.keys_indexer = KeysIndexer(mapping)
        self.values_indexer = ValuesIndexer(mapping)
        self._metadata = metadata or {}

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def __repr__(self):
        return f"{type(self).__name__}" "({...})"

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

    @property
    def index(self):
        return self._index_accessor


class KeysIndexer:
    def __init__(self, mapping):
        self._mapping = mapping

    def __getitem__(self, i):
        if isinstance(i, int):
            if i > len(self):
                raise IndexError(f"index {i} out of range for length {len(self)}")
            key = next(itertools.islice(self._mapping.keys(), i))
            return self._make_result(key)
        elif isinstance(i, slice):
            slice_of_keys = itertools.islice(
                self._mapping.keys(), i.start, i.stop, i.step
            )
            return [self._make_result(key) for key in slice_of_keys]
        else:
            raise TypeError(f"{type(self).__name__} index must be integer or slice.")

    def _make_result(self, key):
        return key


class ValuesIndexer(KeysIndexer):

    # A goal of this implementation is to avoid iterating over
    # self._mapping.values() because self._mapping may be a LazyMap which only
    # constructs its values at access time. With this in mind, w e identify the
    # key(s) of interest and then only access those values.

    def _make_result(self, key):
        return self._mapping[key]


class ItemsIndexer(KeysIndexer):

    # See coment in ValuesIndexer above. The same motivation applies here.

    def _make_result(self, key):
        return (key, self._mapping[key])


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
