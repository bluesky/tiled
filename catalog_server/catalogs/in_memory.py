import collections.abc
import itertools

from ..query_registration import QueryTranslationRegistry
from ..queries import FullText, KeyLookup
from ..utils import (
    authenticated,
    DictView,
    IndexCallable,
    slice_to_interval,
    SpecialUsers,
)


class Catalog(collections.abc.Mapping):

    __slots__ = (
        "_access_policy",
        "_authenticated_identity",
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

    def __init__(
        self, mapping, metadata=None, access_policy=None, authenticated_identity=None
    ):
        self._mapping = mapping
        self._metadata = metadata or {}
        if (access_policy is not None) and (
            not access_policy.check_compatibility(self)
        ):
            raise ValueError(
                f"Access policy {access_policy} is not compatible with this Catalog."
            )
        self._access_policy = access_policy
        self._authenticated_identity = authenticated_identity
        self.keys_indexer = authenticated(IndexCallable(self._keys_indexer))
        self.items_indexer = authenticated(IndexCallable(self._items_indexer))
        self.values_indexer = authenticated(IndexCallable(self._values_indexer))

    @property
    def access_policy(self):
        return self._access_policy

    @property
    def authenticated_identity(self):
        return self._authenticated_identity

    @property
    def metadata(self):
        "Metadata about this Catalog."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def __repr__(self):
        return f"<{type(self).__name__}({set(self._mapping)!r})>"

    @authenticated
    def __getitem__(self, key):
        return self._mapping[key]

    @authenticated
    def __iter__(self):
        yield from self._mapping

    @authenticated
    def __len__(self):
        return len(self._mapping)

    def authenticated_as(self, identity):
        if self._authenticated_identity is not None:
            raise RuntimeError(
                f"Already authenticated as {self.authenticated_identity}"
            )
        if self._access_policy is not None:
            catalog = self._access_policy.filter_results(
                self,
                identity,
            )
        else:
            catalog = type(self)(
                self._mapping,
                metadata=self.metadata,
                access_policy=self.access_policy,
                authenticated_identity=identity,
            )
        return catalog

    @authenticated
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
            return value
        elif isinstance(index, slice):
            start, stop = slice_to_interval(index)
            return [value for _key, value in self._items_slice(start, stop)]
        else:
            raise TypeError(f"{index} must be an int or slice, not {type(index)}")


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
    return type(catalog)(
        matches,
        metadata=catalog.metadata,
        access_policy=catalog.access_policy,
        authenticated_identity=catalog.authenticated_identity,
    )


def key_lookup(query, catalog):
    try:
        matches = {query.key: catalog[query.key]}
    except KeyError:
        matches = {}
    return type(catalog)(
        matches,
        metadata=catalog.metadata,
        access_policy=catalog.access_policy,
        authenticated_identity=catalog.authenticated_identity,
    )


Catalog.register_query(FullText, full_text_search)
Catalog.register_query(KeyLookup, key_lookup)


class DummyAccessPolicy:
    "Impose no access restrictions."

    def check_compatibility(self, catalog):
        # This only works on in-memory Catalog or subclases.
        return isinstance(catalog, Catalog)

    def modify_queries(self, queries, authenticated_identity):
        return queries

    def filter_results(self, catalog, authenticated_identity):
        return type(catalog)(
            mapping=self._mapping,
            metadata=catalog.metadata,
            access_policy=catalog.access_policy,
            authenticated_identity=authenticated_identity,
        )


class SimpleAccessPolicy:
    """
    Refer to a mapping of user names to lists of entries they can access.

    >>> SimpleAccessPolicy({"alice": ["A", "B"], "bob": ["B"]})
    """

    ALL = object()  # sentinel

    def __init__(self, access_lists):
        self.access_lists = access_lists

    def check_compatibility(self, catalog):
        # This only works on in-memory Catalog or subclases.
        return isinstance(catalog, Catalog)

    def modify_queries(self, queries, authenticated_identity):
        return queries

    def filter_results(self, catalog, authenticated_identity):
        allowed = self.access_lists.get(authenticated_identity, [])
        if (authenticated_identity is SpecialUsers.admin) or (allowed is self.ALL):
            mapping = catalog._mapping
        else:
            mapping = {k: v for k, v in catalog._mapping.items() if k in allowed}
        return type(catalog)(
            mapping=mapping,
            metadata=catalog.metadata,
            access_policy=catalog.access_policy,
            authenticated_identity=authenticated_identity,
        )
