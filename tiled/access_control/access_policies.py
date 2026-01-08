import logging
import os
from abc import ABC, abstractmethod
from typing import Generic, Optional, Tuple, TypeVar

import httpx
from pydantic import BaseModel, HttpUrl, TypeAdapter, ValidationError

from ..adapters.protocols import BaseAdapter
from ..queries import AccessBlobFilter
from ..server.schemas import Principal
from ..type_aliases import AccessBlob, AccessTags, Filters, Scopes
from ..utils import Sentinel, import_object
from .protocols import AccessPolicy
from .scopes import ALL_SCOPES, NO_SCOPES, PUBLIC_SCOPES

ALL_ACCESS = Sentinel("ALL_ACCESS")
NO_ACCESS = Sentinel("NO_ACCESS")


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setLevel("DEBUG")
handler.setFormatter(logging.Formatter("TILED ACCESS POLICY: %(message)s"))
logger.addHandler(handler)
log_level = os.getenv("TILED_ACCESS_POLICY_LOG_LEVEL")
if log_level:
    logger.setLevel(log_level.upper())


class DummyAccessPolicy(AccessPolicy):
    "Impose no access restrictions."

    async def init_node(
        self,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_blob: Optional[AccessBlob] = None,
    ) -> Tuple[bool, AccessBlob]:
        "Do nothing; there is no persistent state to initialize."
        return (False, access_blob)

    async def allowed_scopes(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
    ) -> Scopes:
        "Always allow all scopes."
        return ALL_SCOPES

    async def filters(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        scopes: Scopes,
    ) -> Filters:
        "Always impose no filtering on results."
        return []


class TagBasedAccessPolicy(AccessPolicy):
    def __init__(
        self,
        *,
        provider,
        tags_db,
        access_tags_parser,
        scopes=None,
    ):
        self.provider = provider
        self.scopes = scopes if (scopes is not None) else ALL_SCOPES

        access_tags_parser = import_object(access_tags_parser)
        self.access_tags_parser = access_tags_parser.from_uri(tags_db["uri"])
        self.is_tag_defined = self.access_tags_parser.is_tag_defined
        self.get_public_tags = self.access_tags_parser.get_public_tags
        self.get_scopes_from_tag = self.access_tags_parser.get_scopes_from_tag
        self.is_tag_owner = self.access_tags_parser.is_tag_owner
        self.is_tag_public = self.access_tags_parser.is_tag_public
        self.get_tags_from_scope = self.access_tags_parser.get_tags_from_scope

        self.read_scopes = PUBLIC_SCOPES
        self.unremovable_scopes = ["read:metadata", "write:metadata"]
        self.admin_scopes = ["admin:apikeys"]
        self.public_tag = "public".casefold()
        self.invalid_tag_names = [name.casefold() for name in []]

    def _get_id(self, principal):
        for identity in principal.identities:
            if identity.provider == self.provider:
                return identity.id
        else:
            raise ValueError(
                f"Principal {principal} has no identity from provider {self.provider}."
                f"The Principal's identities are: {principal.identities}"
            )

    def _is_admin(self, authn_scopes):
        if all(s in authn_scopes for s in self.admin_scopes):
            return True
        return False

    async def init_node(
        self,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_blob: Optional[AccessBlob] = None,
    ) -> Tuple[bool, AccessBlob]:
        if principal.type == "service":
            identifier = str(principal.uuid)
        else:
            identifier = self._get_id(principal)

        if access_blob:
            if len(access_blob) != 1 or "tags" not in access_blob:
                raise ValueError(
                    f"""access_blob must be in the form '{{"tags": ["tag1", "tag2", ...]}}'\n"""
                    f"""Received {access_blob=}"""
                )
            if not access_blob["tags"]:
                if not self._is_admin(authn_scopes):
                    raise ValueError(
                        "Cannot apply empty tag list to node: only Tiled admins can apply an empty tag list."
                    )
            access_tags = set(access_blob["tags"])
            include_public_tag = False
            for tag in access_tags:
                if authn_access_tags is not None:
                    if tag not in authn_access_tags:
                        raise ValueError(
                            f"Cannot apply tag to node: API key is restricted to access tags: {authn_access_tags}."
                        )
                if tag.casefold() == self.public_tag:
                    include_public_tag = True
                    if not self._is_admin(authn_scopes):
                        raise ValueError(
                            "Cannot apply 'public' tag to node: only Tiled admins can apply the 'public' tag."
                        )
                elif not await self.is_tag_defined(tag):
                    raise ValueError(f"Cannot apply tag to node: {tag=} is not defined")
                elif not await self.is_tag_owner(tag, identifier):
                    # admins can ignore the tag ownership check
                    if not self._is_admin(authn_scopes):
                        raise ValueError(
                            f"Cannot apply tag to node: user='{identifier}' is not an owner of {tag=}"
                        )
                elif tag.casefold() in self.invalid_tag_names:
                    raise ValueError(
                        f"Cannot apply tag to node: '{tag}' is not a valid tag name."
                    )

            access_tags_from_policy = {
                tag for tag in access_tags if tag.casefold() != self.public_tag
            }
            if include_public_tag:
                access_tags_from_policy.add(self.public_tag)

            access_blob_from_policy = {"tags": list(access_tags_from_policy)}
            access_blob_modified = access_tags != access_tags_from_policy

            # admin principals are not subject to scope reduction restriction
            if not self._is_admin(authn_scopes):
                # check that the access_blob would not result in invalid scopes for user.
                new_scopes = set()
                for tag in access_tags_from_policy:
                    new_scopes.update(await self.get_scopes_from_tag(tag, identifier))
                if not all(scope in new_scopes for scope in self.unremovable_scopes):
                    raise ValueError(
                        f"Cannot init node with tags: operation does not grant necessary scopes.\n"
                        f"The resulting access_blob would be: {access_blob_from_policy}\n"
                        f"This access_blob does not confer the minimum scopes: {self.unremovable_scopes}"
                    )
        else:
            if authn_access_tags is not None:
                raise ValueError(
                    f"Cannot init node as user-owned node.\n"
                    f"Current API key does not permit action on user-owned nodes.\n"
                    f"Please provide a tag allowed by this API key: {authn_access_tags}"
                )
            access_blob_from_policy = {"user": identifier}
            access_blob_modified = True

        logger.info(
            f"Node to be initialized with access_blob: {access_blob_from_policy}"
        )
        # modified means the blob to-be-used was changed in comparison to the user input
        return access_blob_modified, access_blob_from_policy

    async def modify_node(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_blob: Optional[AccessBlob],
    ) -> Tuple[bool, AccessBlob]:
        if principal.type == "service":
            identifier = str(principal.uuid)
        else:
            identifier = self._get_id(principal)

        if access_blob == node.access_blob:
            logger.info(
                f"Node access_blob not modified; access_blob is identical: {access_blob}"
            )
            return False, node.access_blob

        if len(access_blob) != 1 or "tags" not in access_blob:
            raise ValueError(
                f"""access_blob must be in the form '{{"tags": ["tag1", "tag2", ...]}}'\n"""
                f"""Received {access_blob=}\n"""
                f"""If this was a merge patch on a user-owned node, use a replace op instead."""
            )
        if not access_blob["tags"]:
            if not self._is_admin(authn_scopes):
                raise ValueError(
                    "Cannot apply empty tag list to node: only Tiled admins can apply an empty tag list."
                )
        access_tags = set(access_blob["tags"])
        include_public_tag = False
        # check for tags that need to be added
        for tag in access_tags:
            if authn_access_tags is not None:
                if tag not in authn_access_tags:
                    raise ValueError(
                        f"Cannot apply tag to node: API key is restricted to access tags: {authn_access_tags}."
                    )
            if tag in node.access_blob.get("tags", []):
                # node already has this tag - no action.
                # or: access_blob does not have "tags" key,
                # so it must have a "user" key currently
                include_public_tag = include_public_tag or (
                    tag.casefold() == self.public_tag
                )
                continue
            elif tag.casefold() == self.public_tag:
                include_public_tag = True
                if not self._is_admin(authn_scopes):
                    raise ValueError(
                        "Cannot apply 'public' tag to node: only Tiled admins can apply the 'public' tag."
                    )
            elif not await self.is_tag_defined(tag):
                raise ValueError(f"Cannot apply tag to node: {tag=} is not defined")
            elif not await self.is_tag_owner(tag, identifier):
                # admins can ignore the tag ownership check
                if not self._is_admin(authn_scopes):
                    raise ValueError(
                        f"Cannot apply tag to node: user='{identifier}' is not an owner of {tag=}"
                    )
            elif tag.casefold() in self.invalid_tag_names:
                raise ValueError(
                    f"Cannot apply tag to node: '{tag}' is not a valid tag name."
                )

        access_tags_from_policy = {
            tag for tag in access_tags if tag.casefold() != self.public_tag
        }
        if include_public_tag:
            access_tags_from_policy.add(self.public_tag)

        # check for tags that need to be removed
        if "tags" in node.access_blob:
            for tag in set(node.access_blob["tags"]).difference(
                access_tags_from_policy
            ):
                if authn_access_tags is not None:
                    if tag not in authn_access_tags:
                        raise ValueError(
                            f"Cannot remove tag from node: "
                            f"API key is restricted to access tags: {authn_access_tags}."
                        )
                if tag == self.public_tag:
                    if not self._is_admin(authn_scopes):
                        raise ValueError(
                            "Cannot remove 'public' tag from node: only Tiled admins can remove the 'public' tag."
                        )
                elif not await self.is_tag_defined(tag):
                    raise ValueError(
                        f"Cannot remove tag from node: {tag=} is not defined"
                    )
                elif not await self.is_tag_owner(tag, identifier):
                    # admins can ignore the tag ownership check
                    if not self._is_admin(authn_scopes):
                        raise ValueError(
                            f"Cannot remove tag from node: user='{identifier}' is not an owner of {tag=}"
                        )
                elif tag.casefold() in self.invalid_tag_names:
                    raise ValueError(
                        f"Cannot remove tag from node: '{tag}' is not a valid tag name."
                    )

        access_blob_from_policy = {"tags": list(access_tags_from_policy)}
        access_blob_modified = access_tags != access_tags_from_policy

        # admin principals are not subject to scope reduction restriction
        if not self._is_admin(authn_scopes):
            # check that the access_blob change would not result in invalid scopes for user.
            # this applies when removing tags, but also must be done when
            # converting from user-owned node to shared (tagged) node
            new_scopes = set()
            for tag in access_tags_from_policy:
                new_scopes.update(await self.get_scopes_from_tag(tag, identifier))
            if not all(scope in new_scopes for scope in self.unremovable_scopes):
                raise ValueError(
                    f"Cannot modify tags on node: operation removes unremovable scopes.\n"
                    f"The current access_blob is: {node.access_blob}\n"
                    f"The new access_blob would be: {access_blob_from_policy}\n"
                    f"These scopes cannot be self-removed: {self.unremovable_scopes}"
                )

        logger.info(
            f"Node to be modified with new access_blob: {access_blob_from_policy}"
        )
        # modified means the blob to-be-used was changed in comparison to the user input
        return access_blob_modified, access_blob_from_policy

    async def allowed_scopes(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
    ) -> Scopes:
        # If this is being called, filter_for_access has let us get this far.
        # However, filters and allowed_scopes should always be implemented to
        # give answers consistent with each other.
        if not hasattr(node, "access_blob"):
            allowed = self.scopes
        elif self._is_admin(authn_scopes):
            allowed = self.scopes
        else:
            if principal is None:
                identifier = None
            elif principal.type == "service":
                identifier = str(principal.uuid)
            else:
                identifier = self._get_id(principal)

            allowed = set()
            if "user" in node.access_blob:
                if authn_access_tags is None and identifier == node.access_blob["user"]:
                    allowed = self.scopes
            elif "tags" in node.access_blob:
                for tag in node.access_blob["tags"]:
                    if authn_access_tags is not None:
                        if tag not in authn_access_tags:
                            continue
                    if await self.is_tag_public(tag):
                        allowed.update(self.read_scopes)
                        if tag == self.public_tag:
                            continue
                    elif not await self.is_tag_defined(tag):
                        continue
                    if identifier is not None:
                        tag_scopes = await self.get_scopes_from_tag(tag, identifier)
                        allowed.update(
                            tag_scopes if tag_scopes.issubset(self.scopes) else set()
                        )

        return allowed

    async def filters(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        scopes: Scopes,
    ) -> Filters:
        queries = []
        query_filter = AccessBlobFilter

        if not hasattr(node, "access_blob"):
            return queries
        if not scopes.issubset(self.scopes):
            return NO_ACCESS

        tag_list = set()
        if principal is None:
            identifier = None
        else:
            if principal.type == "service":
                identifier = str(principal.uuid)
            elif self._is_admin(authn_scopes):
                return queries
            else:
                identifier = self._get_id(principal)
            tag_list.update(
                set.intersection(
                    *[
                        await self.get_tags_from_scope(scope, identifier)
                        for scope in scopes
                    ]
                )
            )

        tag_list.update(
            set.intersection(
                *[
                    await self.get_public_tags() if scope in self.read_scopes else set()
                    for scope in scopes
                ]
            )
        )

        if authn_access_tags is not None:
            identifier = None
            tag_list.intersection_update(authn_access_tags)

        queries.append(query_filter(identifier, tag_list))
        return queries


T = TypeVar("T")


class ResultHolder(BaseModel, Generic[T]):
    result: T


class ExternalPolicyDecisionPoint(AccessPolicy, ABC):
    def __init__(
        self,
        authorization_provider: HttpUrl,
        create_node_endpoint: str,
        allowed_tags_endpoint: str,
        scopes_endpoint: str,
        modify_node_endpoint: Optional[str] = None,
        provider: Optional[str] = None,
        empty_access_blob_public: Optional[bool] = None,
    ):
        """
        Initialize an access policy configuration.

        Parameters
        ----------
        authorization_provider : HttpUrl
            The base URL of the authorization provider.
        create_node_endpoint : str
            An endpoint that returns a boolean decision on whether a use may create a node
        allowed_tags_endpoint : str
            An endpoint that returns a list[str] of tags a user may view on a node
        scopes_endpoint : str
            An endpoint that returns a set[str] of scopes a user has on a node
        modify_node_endpoint : str, optional
            An endpoint that returns a boolean decision on whether a use may modify a node
            Defaults to create_node_endpoint if not set
        provider : Optional[str], optional
            The name of the authorization provider, by default None.
        empty_access_blob_public: bool, optional
            Should a node (e.g. the root node) with no access_blob be treated as public,
            read/writable by any request with correct scopes? Default None, which does not
            short circuit the logic and lets the remote provider decide.
        empty_tag_list_include_all: bool, optional, default False
            Should an empty list of filters the unfiltered list of child nodes, rather
            than filtering out all nodes with any tags? Default False
        """
        self._create_node = str(authorization_provider) + create_node_endpoint
        self._modify_node = str(authorization_provider) + (
            modify_node_endpoint or create_node_endpoint
        )
        self._user_tags = str(authorization_provider) + allowed_tags_endpoint
        self._node_scopes = str(authorization_provider) + scopes_endpoint
        self._empty_access_blob_public = empty_access_blob_public
        self._provider = provider

    @abstractmethod
    def build_input(
        self,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_blob: Optional[AccessBlob] = None,
    ) -> str:
        ...

    async def _get_external_decision(
        self,
        decision_endpoint: str,
        input: str,
        decision_type: type[T],
    ) -> Optional[T]:
        logger.debug(f"Requesting auth {decision_endpoint=} for {input=}")
        async with httpx.AsyncClient() as client:
            response = await client.post(decision_endpoint, content=input)
        response.raise_for_status()
        try:
            logger.debug(f"Deserializing auth {response.text=} as {decision_type=}")
            return TypeAdapter(decision_type).validate_json(response.text)
        except ValidationError:
            return None

    async def init_node(
        self,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        access_blob: Optional[AccessBlob] = None,
    ) -> Tuple[bool, Optional[AccessBlob]]:
        if access_blob is None and self._empty_access_blob_public is not None:
            return self._empty_access_blob_public, access_blob
        decision = await self._get_external_decision(
            self._create_node,
            self.build_input(principal, authn_access_tags, authn_scopes, access_blob),
            ResultHolder[bool],
        )
        if decision:
            return (decision.result, access_blob)
        raise ValueError("Permission denied not able to add the node")

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
            return (False, node.access_blob)
        decision = await self._get_external_decision(
            self._modify_node,
            self.build_input(principal, authn_access_tags, authn_scopes, access_blob),
            ResultHolder[bool],
        )
        if decision:
            return (decision.result, access_blob)
        raise ValueError("Permission denied not able to add the node")

    async def filters(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
        scopes: Scopes,
    ) -> Filters:
        tags = await self._get_external_decision(
            self._user_tags,
            self.build_input(principal, authn_access_tags, authn_scopes),
            ResultHolder[list[str]],
        )
        if tags is not None:
            return [AccessBlobFilter(tags=tags.result, user_id=None)]
        else:
            return NO_ACCESS

    async def allowed_scopes(
        self,
        node: BaseAdapter,
        principal: Principal,
        authn_access_tags: Optional[AccessTags],
        authn_scopes: Scopes,
    ) -> Scopes:
        scopes = await self._get_external_decision(
            self._node_scopes,
            self.build_input(
                principal,
                authn_access_tags,
                authn_scopes,
                getattr(node, "access_blob", None),
            ),
            ResultHolder[set[str]],
        )
        if scopes:
            return scopes.result
        return NO_SCOPES
