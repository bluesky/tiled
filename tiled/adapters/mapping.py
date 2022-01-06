import collections.abc
import itertools
from datetime import datetime

from ..queries import FullText
from ..query_registration import QueryTranslationRegistry
from ..utils import UNCHANGED, DictView, SpecialUsers, import_object
from .utils import IndexersMixin


class MapAdapter(collections.abc.Mapping, IndexersMixin):
    """
    Adapt any mapping (dictionary-like object) to Tiled.
    """

    __slots__ = (
        "_access_policy",
        "_authenticated_identity",
        "_mapping",
        "_metadata",
        "_must_revalidate",
        "background_tasks",
        "entries_stale_after",
        "include_routers",
        "metadata_stale_after",
    )

    structure_family = "node"

    # Define classmethods for managing what queries this Adapter knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    def __init__(
        self,
        mapping,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
        entries_stale_after=None,
        metadata_stale_after=None,
        must_revalidate=True,
    ):
        """
        Create a simple Adapter from any mapping (e.g. dict, OneShotCachedMap).

        Parameters
        ----------
        mapping : dict-like
        metadata : dict, optional
        access_policy : AccessPolicy, optional
        authenticated_identity : str, optional
        entries_stale_after: timedelta
            This server uses this to communite to the client how long
            it should rely on a local cache before checking back for changes.
        metadata_stale_after: timedelta
            This server uses this to communite to the client how long
            it should rely on a local cache before checking back for changes.
        must_revalidate : bool
            Whether the client should strictly refresh stale cache items.
        """
        self._mapping = mapping
        self._metadata = metadata or {}
        if (access_policy is not None) and (
            not access_policy.check_compatibility(self)
        ):
            raise ValueError(
                f"Access policy {access_policy} is not compatible with this Adapter."
            )
        self._access_policy = access_policy
        self._authenticated_identity = authenticated_identity
        self._must_revalidate = must_revalidate
        self.include_routers = []
        self.background_tasks = []
        self.entries_stale_after = entries_stale_after
        self.metadata_stale_after = metadata_stale_after
        super().__init__()

    @property
    def must_revalidate(self):
        return self._must_revalidate

    @must_revalidate.setter
    def must_revalidate(self, value):
        self._must_revalidate = value

    @property
    def access_policy(self):
        return self._access_policy

    @access_policy.setter
    def access_policy(self, value):
        self._access_policy = value

    @property
    def authenticated_identity(self):
        return self._authenticated_identity

    @property
    def metadata(self):
        "Metadata about this Adapter."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    def __repr__(self):
        return f"<{type(self).__name__}({set(self._mapping)!r})>"

    def __getitem__(self, key):
        return self._mapping[key]

    def __iter__(self):
        yield from self._mapping

    def __len__(self):
        return len(self._mapping)

    @property
    def metadata_stale_at(self):
        if self.metadata_stale_after is None:
            return
        return self.metadata_stale_after + datetime.utcnow()

    @property
    def entries_stale_at(self):
        if self.entries_stale_after is None:
            return
        return self.entries_stale_after + datetime.utcnow()

    def authenticated_as(self, identity):
        if self._authenticated_identity is not None:
            raise RuntimeError(
                f"Already authenticated as {self.authenticated_identity}"
            )
        if self._access_policy is not None:
            tree = self._access_policy.filter_results(self, identity)
        else:
            tree = self.new_variation(authenticated_identity=identity)
        return tree

    def new_variation(
        self,
        *args,
        mapping=UNCHANGED,
        metadata=UNCHANGED,
        authenticated_identity=UNCHANGED,
        must_revalidate=UNCHANGED,
        **kwargs,
    ):
        if mapping is UNCHANGED:
            mapping = self._mapping
        if metadata is UNCHANGED:
            metadata = self._metadata
        if authenticated_identity is UNCHANGED:
            authenticated_identity = self._authenticated_identity
        if must_revalidate is UNCHANGED:
            must_revalidate = self.must_revalidate
        return type(self)(
            *args,
            mapping=mapping,
            metadata=self._metadata,
            access_policy=self.access_policy,
            authenticated_identity=self.authenticated_identity,
            entries_stale_after=self.entries_stale_after,
            metadata_stale_after=self.entries_stale_after,
            must_revalidate=must_revalidate,
            **kwargs,
        )

    def read(self, fields=None):
        if fields is not None:
            raise NotImplementedError
        return self

    def search(self, query):
        """
        Return a Adapter with a subset of the mapping.
        """
        return self.query_registry(query, self)

    # The following three methods are used by IndexersMixin
    # to define keys_indexer, items_indexer, and values_indexer.

    def _keys_slice(self, start, stop, direction):
        if direction > 0:
            yield from itertools.islice(self._mapping.keys(), start, stop)
        else:
            keys_to_slice = reversed(
                list(
                    itertools.islice(
                        self._mapping.keys(), 0, len(self._mapping) - start
                    )
                )
            )
            keys = keys_to_slice[start:stop]
            return keys

    def _items_slice(self, start, stop, direction):
        # A goal of this implementation is to avoid iterating over
        # self._mapping.values() because self._mapping may be a OneShotCachedMap which
        # only constructs its values at access time. With this in mind, we
        # identify the key(s) of interest and then only access those values.
        yield from (
            (key, self._mapping[key])
            for key in self._keys_slice(start, stop, direction)
        )

    def _item_by_index(self, index, direction):
        if direction > 0:
            key = next(itertools.islice(self._mapping.keys(), index, 1 + index))
        else:
            key = itertools.islice(self._mapping.keys(), len(self._mapping) - index)
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


def full_text_search(query, tree):
    matches = {}
    text = query.text
    if query.case_sensitive:

        def maybe_lower(s):
            # no-op
            return s

    else:
        maybe_lower = str.lower
    query_words = set(text.split())
    for key, value in tree.items():
        words = set(
            word
            for s in walk_string_values(value.metadata)
            for word in maybe_lower(s).split()
        )
        # Note that `not set.isdisjoint` is faster than `set.intersection`. At
        # the C level, `isdisjoint` loops over the set until it finds one match,
        # and then bails, whereas `intersection` proceeds to find all matches.
        if not words.isdisjoint(query_words):
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(FullText, full_text_search)


class DummyAccessPolicy:
    "Impose no access restrictions."

    def check_compatibility(self, tree):
        # This only works on in-memory Adapter or subclases.
        return isinstance(tree, MapAdapter)

    def modify_queries(self, queries, authenticated_identity):
        return queries

    def filter_results(self, tree, authenticated_identity):
        return type(tree)(
            mapping=self._mapping,
            metadata=tree.metadata,
            access_policy=tree.access_policy,
            authenticated_identity=authenticated_identity,
        )


class SimpleAccessPolicy:
    """
    A mapping of user names to lists of entries they can access.

    >>> SimpleAccessPolicy({"alice": ["A", "B"], "bob": ["B"]})
    """

    ALL = object()  # sentinel

    def __init__(self, access_lists, public=None):
        self.access_lists = {}
        self.public = set(public or [])
        for key, value in access_lists.items():
            if isinstance(value, str):
                value = import_object(value)
            self.access_lists[key] = value

    def check_compatibility(self, tree):
        # This only works on MapAdapter or subclases.
        return isinstance(tree, MapAdapter)

    def modify_queries(self, queries, authenticated_identity):
        return queries

    def filter_results(self, tree, authenticated_identity):
        # either list of paths or ALL
        access_list = self.access_lists.get(authenticated_identity, [])

        if (authenticated_identity is SpecialUsers.admin) or (access_list is self.ALL):
            mapping = tree._mapping
        else:
            allowed = set(access_list or []) | self.public
            mapping = {k: v for k, v in tree._mapping.items() if k in allowed}
        return type(tree)(
            mapping=mapping,
            metadata=tree.metadata,
            access_policy=tree.access_policy,
            authenticated_identity=authenticated_identity,
        )
