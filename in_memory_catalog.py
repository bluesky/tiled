import collections.abc
import itertools

from queries import DictView, QueryTranslationRegistry, Text


class Catalog(collections.abc.Mapping):

    # Define classmethods for managing what queries this Catalog knows.
    __query_registry = QueryTranslationRegistry()
    register_query = __query_registry.register
    register_query_lazy = __query_registry.register_lazy

    def __init__(self, entries, metadata=None):
        self._entries = entries
        self._index_accessor = _IndexAccessor(entries, type(self))
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
        return self._entries[key]

    def __iter__(self):
        yield from self._entries

    def __len__(self):
        return len(self._entries)

    def search(self, query):
        """
        Return a Catalog with a subset of the entries.
        """
        return self.__query_registry(query, self)

    @property
    def index(self):
        return self._index_accessor


class _IndexAccessor:
    "Internal object used by Catalog."

    def __init__(self, entries, out_type):
        self._entries = entries
        self._out_type = out_type

    def __getitem__(self, i):
        if isinstance(i, int):
            if i >= len(self._entries):
                raise IndexError("Catalog index out of range.")
            out = next(itertools.islice(self._entries.values(), i, 1 + i))
        elif isinstance(i, slice):
            out = self._out_type(
                dict(itertools.islice(self._entries.items(), i.start, i.stop, i.step))
            )
        else:
            raise TypeError("Catalog index must be integer or slice.")
        return out


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
        if words.intersection(query_words):
            matches[key] = value
    return type(catalog)(matches)


Catalog.register_query(Text, full_text_search)
