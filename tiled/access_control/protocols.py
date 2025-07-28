from abc import ABC, abstractmethod
from typing import Any, Optional, Set

from ..adapters.protocols import BaseAdapter
from ..server.schemas import Principal
from ..type_aliases import Filters, Scopes


class AccessPolicy(ABC):
    @abstractmethod
    async def allowed_scopes(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[Set[str]],
        authn_scopes: Scopes,
    ) -> Scopes:
        pass

    @abstractmethod
    async def filters(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[Set[str]],
        authn_scopes: Scopes,
        scopes: Scopes,
    ) -> Filters:
        pass

    async def modify_node(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_scopes: Scopes,
        access_blob: Optional[dict[str, Any]],
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        return (False, access_blob)
