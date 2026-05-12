import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Generic, Optional, Tuple, TypeVar

import casbin
import casbin_sqlalchemy_adapter
from pydantic import BaseModel, HttpUrl, TypeAdapter, ValidationError

from ..adapters.protocols import BaseAdapter
from ..queries import AccessBlobFilter
from ..server.schemas import Principal
from ..type_aliases import AccessBlob, AccessTags, Filters, Scopes
from ..utils import Sentinel, import_object
from .protocols import AccessPolicy
from .scopes import ALL_SCOPES, NO_SCOPES, PUBLIC_SCOPES

logger = logging.getLogger(__name__)

ALL_ACCESS = []
NO_ACCESS = Sentinel("NO_ACCESS")

"""
Uses casbin's features to setup access policies that map to tags in Tiled.

Casbin allows you to define the access policy. In this case, we're using
configure casbin with casbin_mode.conf explicitly as an RBAC policy. This maps
to: 
- users being in groups by proposal, ESAF, letting them enjoy read raw data and write processed data
- beamline staff are in group by beamline, meaning they enjoy reading all data for their beamine
- admin staff enjoy most privilages
- If a node does not have a tag, the tags of the first node in the hierarchy apply

A policy stored in that database might look like:

```
p user, raw, read:metadata
p user, raw, read:data
p user, processed, write:metadata
p user, processed, read:data

p bl_staff, *, read:metadata
p bl_staff, *, read:metadata

p admin, *, read:metadata
p admin, *, read:data
p admin, *, write:metadata
p admin, *, read:data

# proposal role
g, alice, user, ALS_0042 
g, bob, user, ALS_0042 

# esaf role
g, alice, user, ALS_0042-001 
g, bob, user, ALS_0042-001  


# beamline role
g, slartibartfast, bl_staff, 42.1.1
g, a_mouse, bl_staff, 42.1.1
```


|_ bl4211            access_tags=42.1.1
    |_ raw           access_tags=42.1.1, raw
    |_ processed     access_tags=4.2.1.1, processed

The policy be constantly based on calls to user office, which stores membership
in prooposals, esafs and beamline staff.

What I don't know right now is whetehr I can really do this in casbin. The examples
I see without internet on a plane make me wonder. I think this is right?

- Resources in casbin are tags in tiled
- Does the wildcarding I'm doing in the policy work? I think casbin lets me write functions
  which maybe I'll need to do the wildcars?

"""



class CasbinRBACAccessPolicy(AccessPolicy, ABC):
    def __init__(
        self,
        enforcer: casbin.Enforcer,
        provider: str,
        policy_ttl: float = 60.0,
    ):
        self._enforcer = enforcer
        self._provider = provider
        self._policy_ttl = policy_ttl
        self._policy_loaded_at: float = time.monotonic()

    @classmethod
    def from_config(cls, policy_db_path: str, model_config_path: str, provider_id: str, **kwargs):
        adapter = casbin_sqlalchemy_adapter.Adapter(f"sqlite:///{policy_db_path}")
        enforcer = casbin.Enforcer(model_config_path, adapter)
        return cls(enforcer=enforcer, provider=provider_id, **kwargs)

    def _maybe_reload(self):
        now = time.monotonic()
        if now - self._policy_loaded_at >= self._policy_ttl:
            self._enforcer.load_policy()
            self._policy_loaded_at = now

    def _enforce(self, *args) -> bool:
        self._maybe_reload()
        return self._enforcer.enforce(*args)

    def _get_principal_id(self, principal: Principal) -> Optional[str]:
        if principal is None:
            return None
        if principal.type == "service":
            return str(principal.uuid)
        for identity in principal.identities:
            if identity.provider == self._provider:
                return identity.id
        raise ValueError(
            f"Principal {principal} has no identity from provider {self._provider!r}. "
            f"Identities: {principal.identities}"
        )

    async def init_node(
        self,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_blob: Optional[AccessBlob] = None,
    ) -> Tuple[bool, Optional[AccessBlob]]:
        user = self._get_principal_id(principal)
        if not self._enforce(user, "*", "write:data"):
            raise ValueError("Permission denied: cannot create node")
        return True, access_blob

    async def modify_node(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_blob: Optional[AccessBlob],
    ) -> Tuple[bool, Optional[AccessBlob]]:
        if access_blob == node.access_blob:
            logger.info(
                f"Node access_blob not modified; access_blob is identical: {access_blob}"
            )
            return False, node.access_blob
        user = self._get_principal_id(principal)
        resource = str(getattr(node, "access_blob", "*"))
        if not self._enforce(user, resource, "write:metadata"):
            raise ValueError("Permission denied: cannot modify node")
        return True, access_blob

    async def allowed_scopes(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
    ) -> Scopes:
        key = str(getattr(node, "key", ''))
        user = self._get_principal_id(principal)
        resource = str(getattr(node, "access_blob", ""))
        if "admin" in principal.roles:
            return ALL_SCOPES
        self._maybe_reload()
        user_scopes = {scope for scope in ALL_SCOPES if self._enforcer.enforce(user, resource, scope)}
        if not key and len(user_scopes) == 0:
            # all users should read root node????
            return ["read:data", "read:metadata"]

    async def filters(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        scopes: Scopes,
    ) -> Filters:
        user = self._get_principal_id(principal)
        self._maybe_reload()
        # get all of a users tags based on their group membership
        # each role will correspond to a tiled tag
        tags = self._enforcer.get_implicit_roles_for_user(user)
        # permissions = self._enforcer.get_implicit_permissions_for_user(user)
        # allowed_resources = {
        #     resource for _sub, resource, action in permissions if action in scopes
        # }

        # return ALL_ACCESS
        if "*" in allowed_resources:
            return ALL_ACCESS
        if not allowed_resources:
            return NO_ACCESS
        return [AccessBlobFilter(tags=list(allowed_resources), user_id=user)]
        