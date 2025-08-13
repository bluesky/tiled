import copy
import itertools
import operator
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

if TYPE_CHECKING:
    from fastapi import APIRouter

from collections.abc import Iterable, Mapping

from ..iterviews import ItemsView, KeysView, ValuesView
from ..queries import (
    Comparison,
    Contains,
    Eq,
    FullText,
    In,
    KeyPresent,
    KeysFilter,
    NotEq,
    NotIn,
    Regex,
    SpecsQuery,
    StructureFamilyQuery,
)
from ..query_registration import QueryTranslationRegistry
from ..server.schemas import SortingItem
from ..storage import Storage
from ..structures.core import Spec, StructureFamily
from ..structures.table import TableStructure
from ..type_aliases import JSON
from ..utils import UNCHANGED, Sentinel
from .protocols import AnyAdapter
from .utils import IndexersMixin


class MapAdapter(Mapping[str, AnyAdapter], IndexersMixin):
    """
    Adapt any mapping (dictionary-like object) to Tiled.
    """

    __slots__ = (
        "_mapping",
        "_metadata",
        "_sorting",
        "_must_revalidate",
        "background_tasks",
        "entries_stale_after",
        "include_routers",
        "metadata_stale_after",
        "specs",
    )

    structure_family = StructureFamily.container
    supported_storage: Set[type[Storage]] = set()

    # Define classmethods for managing what queries this Adapter knows.
    query_registry = QueryTranslationRegistry()
    register_query = query_registry.register
    register_query_lazy = query_registry.register_lazy

    def __init__(
        self,
        mapping: Dict[str, Any],
        *,
        structure: Optional[TableStructure] = None,
        metadata: Optional[JSON] = None,
        sorting: Optional[List[SortingItem]] = None,
        specs: Optional[List[Spec]] = None,
        entries_stale_after: Optional[timedelta] = None,
        metadata_stale_after: Optional[timedelta] = None,
        must_revalidate: bool = True,
    ) -> None:
        """
        Create a simple Adapter from any mapping (e.g. dict, OneShotCachedMap).

        Parameters
        ----------
        mapping : dict-like
        metadata : dict, optional
        specs : List[str], optional
        sorting : List[Tuple[str, int]], optional
        specs : List[str], optional
        entries_stale_after: timedelta
            This server uses this to communicate to the client how long
            it should rely on a local cache before checking back for changes.
        metadata_stale_after: timedelta
            This server uses this to communicate to the client how long
            it should rely on a local cache before checking back for changes.
        must_revalidate : bool
            Whether the client should strictly refresh stale cache items.
        """
        if structure is not None:
            raise ValueError(
                f"structure is expected to be None for containers, not {structure}"
            )
        self._mapping = mapping
        if sorting is None:
            # This is a special case that means, "the given ordering".
            # By giving that a name ("_") we enable requests to asking for the
            # last N by requesting the sorting ("_", -1).
            sorting = [SortingItem(key="_", direction=1)]
        self._sorting = sorting
        self._metadata = metadata or {}
        self.specs = specs or []
        self._must_revalidate = must_revalidate
        self.include_routers: List[APIRouter] = []
        self.background_tasks: List[Any] = []
        self.entries_stale_after = entries_stale_after
        self.metadata_stale_after = metadata_stale_after
        self.specs = specs or []
        super().__init__()

    @property
    def must_revalidate(self) -> bool:
        """

        Returns
        -------

        """
        return self._must_revalidate

    @must_revalidate.setter
    def must_revalidate(self, value: bool) -> None:
        """

        Parameters
        ----------
        value :

        Returns
        -------

        """
        self._must_revalidate = value

    def metadata(self) -> JSON:
        "Metadata about this Adapter."
        # Ensure this is immutable (at the top level) to help the user avoid
        # getting the wrong impression that editing this would update anything
        # persistent.
        return self._metadata

    @property
    def sorting(self) -> List[SortingItem]:
        """

        Returns
        -------

        """
        return list(self._sorting)

    def __repr__(self) -> str:
        """

        Returns
        -------

        """
        return (
            f"<{type(self).__name__}({{{', '.join(repr(k) for k in self._mapping)}}})>"
        )

    def __getitem__(self, key: str) -> Any:
        """

        Parameters
        ----------
        key :

        Returns
        -------

        """
        return self._mapping[key]

    def __iter__(self) -> Iterator[str]:
        """

        Returns
        -------

        """
        yield from self._mapping

    def __len__(self) -> int:
        """

        Returns
        -------

        """
        return len(self._mapping)

    def keys(self) -> KeysView:  # type: ignore
        """

        Returns
        -------

        """
        return KeysView(lambda: len(self), self._keys_slice)

    def values(self) -> ValuesView:  # type: ignore
        """

        Returns
        -------

        """
        return ValuesView(lambda: len(self), self._items_slice)

    def items(self) -> ItemsView:  # type: ignore
        """

        Returns
        -------

        """
        return ItemsView(lambda: len(self), self._items_slice)

    def structure(self) -> None:
        """

        Returns
        -------

        """
        return None

    @property
    def metadata_stale_at(self) -> Optional[datetime]:
        """

        Returns
        -------

        """
        if self.metadata_stale_after is None:
            return None
        return self.metadata_stale_after + datetime.now(timezone.utc)

    @property
    def entries_stale_at(self) -> Optional[datetime]:
        """

        Returns
        -------

        """
        if self.entries_stale_after is None:
            return None
        return self.entries_stale_after + datetime.now(timezone.utc)

    def new_variation(
        self,
        *args: Any,
        mapping: Union[Sentinel, Dict[str, Any]] = UNCHANGED,
        metadata: Union[Sentinel, JSON] = UNCHANGED,
        sorting: Union[Sentinel, List[SortingItem]] = UNCHANGED,
        must_revalidate: Union[Sentinel, bool] = UNCHANGED,
        **kwargs: Any,
    ) -> "MapAdapter":
        """

        Parameters
        ----------
        args :
        mapping :
        metadata :
        sorting :
        must_revalidate :
        kwargs :

        Returns
        -------

        """
        if mapping is UNCHANGED:
            mapping = self._mapping
        if metadata is UNCHANGED:
            metadata = self._metadata
        if sorting is UNCHANGED:
            sorting = self._sorting
        if must_revalidate is UNCHANGED:
            must_revalidate = self.must_revalidate
        return type(self)(
            # *args,
            mapping=cast(Dict[str, Any], mapping),
            sorting=cast(List[SortingItem], sorting),
            metadata=cast(JSON, self._metadata),
            specs=self.specs,
            entries_stale_after=self.entries_stale_after,
            metadata_stale_after=self.entries_stale_after,
            must_revalidate=cast(bool, must_revalidate),
            **kwargs,
        )

    def read(self, fields: Optional[str] = None) -> "MapAdapter":
        """

        Parameters
        ----------
        fields :

        Returns
        -------

        """
        if fields is not None:
            new_mapping = {}
            for field in fields:
                new_mapping[field] = self._mapping[field]
            return self.new_variation(mapping=new_mapping)
        return self

    def search(self, query: Any) -> Any:
        """

        Parameters
        ----------
        query :

        Returns
        -------
                Return a Adapter with a subset of the mapping.
        """
        return self.query_registry(query, self)

    def get_distinct(
        self,
        metadata: JSON,
        structure_families: StructureFamily,
        specs: List[Spec],
        counts: int,
    ) -> Dict[str, Any]:
        """

        Parameters
        ----------
        metadata :
        structure_families :
        specs :
        counts :

        Returns
        -------

        """
        data: Dict[str, Any] = {}
        # data: dict[str, list[dict[str, Any]]] = {}

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

    def sort(self, sorting: SortingItem) -> "MapAdapter":
        """

        Parameters
        ----------
        sorting :

        Returns
        -------

        """
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
                        key=lambda item: item[1].metadata().get(key, _HIGH_SORTER),  # type: ignore
                    )
                )

            if direction < 0:
                # TODO In Python 3.8 dict items should be reversible
                # but I have seen errors in the wild that I could not
                # quickly resolve so for now we convert to list in the middle.
                to_reverse = list(mapping.items())
                mapping = dict(reversed(to_reverse))

        return self.new_variation(mapping=mapping, sorting=sorting)

    # The following two methods are used by keys(), values(), items().

    def _keys_slice(
        self, start: int, stop: int, direction: int, page_size: Optional[int] = None
    ) -> Union[Iterator[str], List[str]]:
        """

        Parameters
        ----------
        start :
        stop :
        direction :

        Returns
        -------

        """
        if direction > 0:
            yield from itertools.islice(self._mapping.keys(), start, stop)
        else:
            keys_to_slice = list(
                reversed(
                    list(
                        itertools.islice(
                            self._mapping.keys(), 0, len(self._mapping) - start
                        )
                    )
                )
            )
            keys = keys_to_slice[start:stop]
            return keys

    def _items_slice(
        self, start: int, stop: int, direction: int, page_size: Optional[int] = None
    ) -> Iterator[Tuple[str, Any]]:
        """

        Parameters
        ----------
        start :
        stop :
        direction :

        Returns
        -------

        """
        # A goal of this implementation is to avoid iterating over
        # self._mapping.values() because self._mapping may be a OneShotCachedMap which
        # only constructs its values at access time. With this in mind, we
        # identify the key(s) of interest and then only access those values.
        yield from (
            (key, self._mapping[key])
            for key in self._keys_slice(start, stop, direction)
        )


def walk_string_values(tree: MapAdapter, node: Optional[Any] = None) -> Iterator[str]:
    """
    >>> list(
    ...     walk_string_values(
    ...         {'a': {'b': {'c': 'apple', 'd': 'banana'},
    ...          'e': ['cat', 'dog']}, 'f': 'elephant'}
    ...     )
    ... )
    ['apple', 'banana', 'cat', 'dog', 'elephant']

    Parameters
    ----------
    tree :
    node :

    Returns
    -------
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
        elif isinstance(value, Iterable):
            for item in value:
                if isinstance(item, str):
                    yield item


def counter_to_dict(counter: Dict[str, Any], counts: Any) -> List[Dict[str, Any]]:
    """

    Parameters
    ----------
    counter :
    counts :

    Returns
    -------

    """
    if counts:
        data = [{"value": k, "count": v} for k, v in counter.items() if k is not None]
    else:
        data = [{"value": k} for k in counter if k is not None]

    return data


def iter_child_metadata(
    query_key: Any, tree: MapAdapter
) -> Iterator[Tuple[str, Any, Any]]:
    """

    Parameters
    ----------
    query_key :
    tree :

    Returns
    -------

    """
    for key, value in tree.items():
        term = value.metadata()
        for subkey in query_key.split("."):
            if subkey not in term:
                term = None
                break
            term = term[subkey]
        else:
            yield key, value, term


def full_text_search(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    text = query.text
    query_words = set(text.split())
    for key, value in tree.items():
        words = set(
            word
            for s in walk_string_values(value.metadata())
            for word in s.lower().split()
        )
        # Note that `not set.isdisjoint` is faster than `set.intersection`. At
        # the C level, `isdisjoint` loops over the set until it finds one match,
        # and then bails, whereas `intersection` proceeds to find all matches.
        if not words.isdisjoint(query_words):
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(FullText, full_text_search)


def regex(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    import re

    matches = {}
    flags = 0 if query.case_sensitive else re.IGNORECASE
    pattern = re.compile(query.pattern, flags=flags)

    for key, value, term in iter_child_metadata(query.key, tree):
        if isinstance(term, str) and pattern.search(term):
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(Regex, regex)


def eq(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if term == query.value:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(Eq, eq)


def noteq(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if term != query.value:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(NotEq, noteq)


def contains(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if (
            isinstance(term, Iterable)
            and (not isinstance(term, str))
            and (query.value in term)
        ):
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(Contains, contains)


def comparison(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if query.operator not in {"le", "lt", "ge", "gt"}:
            raise ValueError(f"Unexpected operator {query.operator}.")
        comparison_func = getattr(operator, query.operator)
        if comparison_func(term, query.value):
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(Comparison, comparison)


def _in(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if term in query.value:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(In, _in)


def notin(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    if len(query.value) == 0:
        return tree
    for key, value, term in iter_child_metadata(query.key, tree):
        if term not in query.value:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(NotIn, notin)


def key_present(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    for key, value, term in iter_child_metadata(query.key, tree):
        if term in query.key:
            matches[key] = value
    return tree.new_variation(mapping=matches)


MapAdapter.register_query(KeyPresent, key_present)


def specs(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    include = set(query.include)
    exclude = set(query.exclude)

    for key, value in tree.items():
        specs = set(value.specs)
        if include.issubset(specs) and exclude.isdisjoint(specs):
            matches[key] = value

    return tree.new_variation(mapping=matches)


MapAdapter.register_query(SpecsQuery, specs)


def structure_family(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
    matches = {}
    for key, value in tree.items():
        if value.structure_family == query.value:
            matches[key] = value

    return tree.new_variation(mapping=matches)


MapAdapter.register_query(StructureFamilyQuery, structure_family)


def keys_filter(query: Any, tree: MapAdapter) -> MapAdapter:
    """

    Parameters
    ----------
    query :
    tree :

    Returns
    -------

    """
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

    def __lt__(self, other: "_HIGH_SORTER_CLASS") -> bool:
        """

        Parameters
        ----------
        other :

        Returns
        -------

        """
        return False

    def __gt__(self, other: "_HIGH_SORTER_CLASS") -> bool:
        """

        Parameters
        ----------
        other :

        Returns
        -------

        """
        return True


_HIGH_SORTER = _HIGH_SORTER_CLASS()
