import collections.abc
import itertools

from ..query_registration import QueryTranslationRegistry
from ..queries import FullText
from ..utils import (
    DictView,
    import_object,
    SpecialUsers,
)
from .utils import IndexersMixin, UNCHANGED


class Tree(collections.abc.Mapping, IndexersMixin):
    """
    A Tree backed by a mapping.
    """

    __slots__ = (
        "_access_policy",
        "_authenticated_identity",
        "include_routers",
        "background_tasks",
        "_mapping",
        "_metadata",
    )
    # Define classmethods for managing what queries this Tree knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    def __init__(
        self,
        mapping,
        metadata=None,
        access_policy=None,
        authenticated_identity=None,
    ):
        """
        Create a simple Tree from any mapping (e.g. dict, OneShotCachedMap).

        Parameters
        ----------
        mapping : dict-like
        metadata : dict, optional
        access_policy : AccessPolicy, optional
        authenticated_identity : str, optional
        """
        self._mapping = mapping
        self._metadata = metadata or {}
        if (access_policy is not None) and (
            not access_policy.check_compatibility(self)
        ):
            raise ValueError(
                f"Access policy {access_policy} is not compatible with this Tree."
            )
        self._access_policy = access_policy
        self._authenticated_identity = authenticated_identity
        self.include_routers = []
        self.background_tasks = []
        super().__init__()

    @property
    def access_policy(self):
        return self._access_policy

    @property
    def authenticated_identity(self):
        return self._authenticated_identity

    @property
    def metadata(self):
        "Metadata about this Tree."
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

    def authenticated_as(self, identity):
        if self._authenticated_identity is not None:
            raise RuntimeError(
                f"Already authenticated as {self.authenticated_identity}"
            )
        if self._access_policy is not None:
            tree = self._access_policy.filter_results(
                self,
                identity,
            )
        else:
            tree = self.new_variation(authenticated_identity=identity)
        return tree

    def new_variation(
        self,
        *args,
        mapping=UNCHANGED,
        metadata=UNCHANGED,
        authenticated_identity=UNCHANGED,
        **kwargs,
    ):
        if mapping is UNCHANGED:
            mapping = self._mapping
        if metadata is UNCHANGED:
            metadata = self._metadata
        if authenticated_identity is UNCHANGED:
            authenticated_identity = self._authenticated_identity
        return type(self)(
            *args,
            mapping=mapping,
            metadata=self._metadata,
            access_policy=self.access_policy,
            authenticated_identity=self.authenticated_identity,
            **kwargs,
        )

    def search(self, query):
        """
        Return a Tree with a subset of the mapping.
        """
        return self.query_registry(query, self)

    # The following three methods are used by IndexersMixin
    # to define keys_indexer, items_indexer, and values_indexer.

    def _keys_slice(self, start, stop, direction):
        if direction > 0:
            yield from itertools.islice(
                self._mapping.keys(),
                start,
                stop,
            )
        else:
            keys_to_slice = reversed(
                list(
                    itertools.islice(
                        self._mapping.keys(),
                        0,
                        len(self._mapping) - start,
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


Tree.register_query(FullText, full_text_search)


class DummyAccessPolicy:
    "Impose no access restrictions."

    def check_compatibility(self, tree):
        # This only works on in-memory Tree or subclases.
        return isinstance(tree, Tree)

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
    Refer to a mapping of user names to lists of entries they can access.

    >>> SimpleAccessPolicy({"alice": ["A", "B"], "bob": ["B"]})
    """

    ALL = object()  # sentinel

    def __init__(self, access_lists):
        self.access_lists = {}
        for key, value in access_lists.items():
            if isinstance(value, str):
                value = import_object(value)
            self.access_lists[key] = value

    def check_compatibility(self, tree):
        # This only works on in-memory Tree or subclases.
        return isinstance(tree, Tree)

    def modify_queries(self, queries, authenticated_identity):
        return queries

    def filter_results(self, tree, authenticated_identity):
        allowed = self.access_lists.get(authenticated_identity, [])
        if (authenticated_identity is SpecialUsers.admin) or (allowed is self.ALL):
            mapping = tree._mapping
        else:
            mapping = {k: v for k, v in tree._mapping.items() if k in allowed}
        return type(tree)(
            mapping=mapping,
            metadata=tree.metadata,
            access_policy=tree.access_policy,
            authenticated_identity=authenticated_identity,
        )
