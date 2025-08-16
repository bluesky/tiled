import logging
import os

from ..queries import AccessBlobFilter
from ..utils import Sentinel, import_object
from .scopes import ALL_SCOPES, PUBLIC_SCOPES

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


class DummyAccessPolicy:
    "Impose no access restrictions."

    async def allowed_scopes(self, node, principal, authn_access_tags, authn_scopes):
        return ALL_SCOPES

    async def filters(self, node, principal, authn_access_tags, authn_scopes, scopes):
        return []


class TagBasedAccessPolicy:
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
        self, principal, authn_access_tags, authn_scopes, access_blob=None
    ):
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
        self, node, principal, authn_access_tags, authn_scopes, access_blob
    ):
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

    async def allowed_scopes(self, node, principal, authn_access_tags, authn_scopes):
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

    async def filters(self, node, principal, authn_access_tags, authn_scopes, scopes):
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
