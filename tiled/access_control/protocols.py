from abc import ABC, abstractmethod
from typing import Iterable, Optional, Tuple

from ..adapters.protocols import BaseAdapter
from ..server.schemas import Principal
from ..type_aliases import AccessTags, Filters, Scopes

# Prefixes of principal tags ('user:<id>', 'service:<uuid>'), which mark
# nodes as owned by a single principal.
PRINCIPAL_TAG_PREFIXES = ("user:", "service:")

# Generation counter for the compiled-tag state. Bumped after every in-process
# change to the compiled tags (a compiler run, or on-the-fly registration of a
# principal tag row) so that caches of compiled-tag data (the policy's filters
# cache, the catalog context's resolved tag-id cache) are invalidated
# immediately within this process. Changes made by *other* processes are
# covered by those caches' TTLs instead. This lives here (rather than in
# access_tags) because it is needed by modules that cannot import access_tags
# without a circular import.
_TAGS_GENERATION = 0


def tags_generation():
    "Current compiled-tags generation; changes after each in-process compile."
    return _TAGS_GENERATION


def bump_tags_generation():
    global _TAGS_GENERATION
    _TAGS_GENERATION += 1


def normalize_access_tags(tags: Iterable[str] = ()) -> AccessTags:
    """
    Validate tag names and normalize them to a frozenset.

    A bare string is rejected rather than accepted: frozenset("abc") would
    silently produce {'a', 'b', 'c'} instead of {'abc'}.
    """
    if isinstance(tags, str):
        raise TypeError(
            "access tags must be an iterable of strings, not a single string."
        )
    return frozenset(tags)


class AccessPolicy(ABC):
    @abstractmethod
    async def init_node(
        self,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_tags: Optional[AccessTags] = None,
    ) -> Tuple[bool, AccessTags]:
        pass

    async def modify_node(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_tags: Optional[AccessTags],
    ) -> Tuple[bool, AccessTags]:
        return (False, access_tags or normalize_access_tags())

    @abstractmethod
    async def allowed_scopes(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
    ) -> Scopes:
        pass

    @abstractmethod
    async def filters(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        scopes: Scopes,
    ) -> Filters:
        pass
