from abc import ABC, abstractmethod
from typing import Iterable, Optional, Tuple

from ..adapters.protocols import BaseAdapter
from ..server.schemas import Principal
from ..type_aliases import AccessTags, Filters, Scopes

# Prefixes of principal tags ('user:<id>', 'service:<uuid>'), which mark
# nodes as owned by a single principal.
PRINCIPAL_TAG_PREFIXES = ("user:", "service:")


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
