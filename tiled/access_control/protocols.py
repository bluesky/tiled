from abc import ABC, abstractmethod
from typing import Iterable, Optional, Tuple

from typing_extensions import Self

from ..adapters.protocols import BaseAdapter
from ..server.schemas import Principal
from ..type_aliases import Filters, Scopes

# Prefixes of principal tags ('user:<id>', 'service:<uuid>'), which mark
# nodes as owned by a single principal.
PRINCIPAL_TAG_PREFIXES = ("user:", "service:")


class AccessTags(frozenset[str]):
    def __new__(cls, tags: Iterable[str] = ()) -> Self:
        if isinstance(tags, str):
            raise TypeError(
                "AccessTags expects an iterable of strings, not a single string."
            )
        return super().__new__(cls, tags)


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
        return (False, access_tags or AccessTags())

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
