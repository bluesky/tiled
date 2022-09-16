import collections.abc
import copy
import itertools
import operator
from collections import Counter
from datetime import datetime

from ..iterviews import ItemsView, KeysView, ValuesView
from ..queries import (
    Comparison,
    Contains,
    Eq,
    FullText,
    In,
    KeysFilter,
    NotEq,
    NotIn,
    Regex,
    Specs,
    StructureFamily,
)
from ..query_registration import QueryTranslationRegistry
from ..utils import UNCHANGED, DictView
from .utils import IndexersMixin


class MapAdapter(collections.abc.Mapping, IndexersMixin):
    """
    Adapt any mapping (dictionary-like object) to Tiled.
    """

    __slots__ = (
        "_access_policy",
        "_mapping",
        "_metadata",
        "_sorting",
        "_must_revalidate",
        "background_tasks",
        "entries_stale_after",
        "include_routers",
        "metadata_stale_after",
        "specs",
        "references",
    )

    structure_family = "node"

    # Define classmethods for managing what queries this Adapter knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    def __init__(
        self,
        mapping,
        *,
        metadata=None,
        sorting=None,
        specs=None,
        references=None,
        access_policy=None,
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
        specs : List[str], optional
        sorting : List[Tuple[str, int]], optional
        specs : List[str], optional
        references : Dict[str, URL], optional
        access_policy : AccessPolicy, optional
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
        if sorting is None:
            # This is a special case that means, "the given ordering".
            # By giving that a name ("_") we enable requests to asking for the
            # last N by requesting the sorting ("_", -1).
            sorting = [("_", 1)]
        self._sorting = sorting
        self._metadata = metadata or {}
        self.specs = specs or []
        self._access_policy = access_policy
        self._must_revalidate = must_revalidate
        self.include_routers = []
        self.background_tasks = []
        self.entries_stale_after = entries_stale_after
        self.metadata_stale_after = metadata_stale_after
        self.specs = specs or []
        self.references = references or []
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
    def metadata(self):
        "Metadata about this Adapter."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return DictView(self._metadata)

    @property
    def sorting(self):
        return list(self._sorting)

    def __repr__(self):
        return (
            f"<{type(self).__name__}({{{', '.join(repr(k) for k in self._mapping)}}})>"
        )

    def __getitem__(self, key):
        return self._mapping[key]

    def __iter__(self):
        yield from self._mapping

    def __len__(self):
        return len(self._mapping)

    def keys(self):
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self):
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self):
        return ItemsView(lambda: len(self), self._items_slice)

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

    def new_variation(
        self,
        *args,
        mapping=UNCHANGED,
        metadata=UNCHANGED,
        sorting=UNCHANGED,
        must_revalidate=UNCHANGED,
        **kwargs,
    ):
        if mapping is UNCHANGED:
            mapping = self._mapping
        if metadata is UNCHANGED:
            metadata = self._metadata
        if sorting is UNCHANGED:
            sorting = self._sorting
        if must_revalidate is UNCHANGED:
            must_revalidate = self.must_revalidate
        return type(self)(
            *args,
            mapping=mapping,
            sorting=sorting,
            metadata=self._metadata,
            specs=self.specs,
            references=self.references,
            access_policy=self.access_policy,
            entries_stale_after=self.entries_stale_after,
            metadata_stale_after=self.entries_stale_after,
            must_revalidate=must_revalidate,
            **kwargs,
        )

    def read(self, fields=None):
        if fields is not None:
            new_mapping = {}
            for field in fields:
                new_mapping[field] = self._mapping[field]
            return self.new_variation(mapping=new_mapping)
        return self

    def search(self, query):
        """
        Return a Adapter with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def get_distinct(self, metadata, structure_families, specs, counts):
        data = {}

        if metadata:
            data["metadata"] = {}

            for metadata_key in metadata:
                counter = Counter(
                    term for key, value, term in iter_child_metadata(metadata_key, self)
                )
                data["metadata"][metadata_key] = counter_to_dict(counter, counts)

        if structure_families:
            counter = Counter(value.structure_family for key, value in self.items())
            data["structure_families"] = counter_to_dict(counter, counts)

        if specs:
            counter = Counter(tuple(value.specs) for key, value in self.items())
            data["specs"] = counter_to_dict(counter, counts)

        return data

    def sort(self, sorting):
        mapping = copy.copy(self._mapping)
        for key, direction in reversed(sorting):
            if key == "_":
                # Special case to enable reversing the given/default ordering.
                # Leave mapping as is, and possibly reserve it below.
                pass
            else:
                mapping = dict(
                    sorted(
                        mapping.items(),
                        key=lambda item: item[1].metadata.get(key, _HIGH_SORTER),
                    )
                )
            if direction < 0:
                # TODO In Python 3.8 dict items should be reservible
                # but I have seen errors in the wild that I could not
                # quickly resolve so for now we convert to list in the middle.
                to_reverse = list(mapping.items())
                mapping = dict(reversed(to_reverse))

        return self.new_variation(mapping=mapping, sorting=sorting)

    # The following two methods are used by keys(), values(), items().

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


def counter_to_dict(counter, counts):
    if counts:
        data = [{"value": k, "count": v} for k, v in counter.items() if k is not None]
    else:
        data = [{"value": k} for k in counter if k is not None]

    return data


def iter_child_metadata(query_key, tree):
    for key, value in tree.items():
        term = value.metadata
        for subkey in query_key.split("."):
            if subkey not in term:
                term = None
                break
            term = term[subkey]
        else:
            yield key, value, term


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


def regex(query, tree):
    import re

    matches = {}
    flags = 0 if query.case_sensitive else re.IGNORECASE
    pattern = re.compile(query.pattern, flags=flags)

    for key, value, term in iter_child_metadata(query.key, tree):
        if isinstance(term, str) and pattern.search(term):
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(Regex, regex)


def eq(query, tree):
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if term == query.value:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(Eq, eq)


def noteq(query, tree):
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if term != query.value:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(NotEq, noteq)


def contains(query, tree):
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if (
            isinstance(term, collections.abc.Iterable)
            and (not isinstance(term, str))
            and (query.value in term)
        ):
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(Contains, contains)


def comparison(query, tree):
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if query.operator not in {"le", "lt", "ge", "gt"}:
            raise ValueError(f"Unexpected operator {query.operator}.")
        comparison_func = getattr(operator, query.operator)
        if comparison_func(term, query.value):
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(Comparison, comparison)


def _in(query, tree):
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if term in query.value:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(In, _in)


def notin(query, tree):
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if term not in query.value:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(NotIn, notin)


def specs(query, tree):
    matches = {}
    include = set(query.include)
    exclude = set(query.exclude)

    for key, value in tree.items():
        specs = set(value.specs)
        if include.issubset(specs) and exclude.isdisjoint(specs):
            matches[key] = value

    return tree.new_variation(mapping=matches)


MapAdapter.register_query(Specs, specs)


def structure_family(query, tree):
    matches = {}
    for key, value in tree.items():
        if value.structure_family == query.value:
            matches[key] = value

    return tree.new_variation(mapping=matches)


MapAdapter.register_query(StructureFamily, structure_family)


def keys_filter(query, tree):
    matches = {}
    for key, value in tree.items():
        if key in query.keys:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(KeysFilter, keys_filter)


class _HIGH_SORTER_CLASS:
    """
    Enables sort to work when metadata is sparse
    """

    def __lt__(self, other):
        return False

    def __gt__(self, other):
        return True


_HIGH_SORTER = _HIGH_SORTER_CLASS()
