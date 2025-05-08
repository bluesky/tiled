import logging
import os
import warnings
from copy import deepcopy
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from sys import intern

import yaml

from .queries import AccessBlobFilter, In, KeysFilter
from .scopes import ALL_SCOPES, PUBLIC_SCOPES
from .utils import Sentinel, SpecialUsers, import_object

ALL_ACCESS = Sentinel("ALL_ACCESS")
NO_ACCESS = Sentinel("NO_ACCESS")

_MAX_TAG_NESTING = 5


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

    async def allowed_scopes(self, node, principal, authn_scopes):
        return ALL_SCOPES

    async def filters(self, node, principal, authn_scopes, scopes):
        return []


class SimpleAccessPolicy:
    """
    A mapping of user names to lists of entries they have access to.

    This simple policy does not provide fine-grained control of scopes.
    Any restriction on scopes is applied the same to all users, except
    for an optional list of 'admins'.

    This is used in the test suite; it may be suitable for very simple
    deployments.

    >>> SimpleAccessPolicy({"alice": ["A", "B"], "bob": ["B"]}, provider="toy")
    """

    ALL = ALL_ACCESS

    def __init__(
        self, access_lists, *, provider, key=None, scopes=None, public=None, admins=None
    ):
        self.access_lists = {}
        self.provider = provider
        self.key = key
        self.scopes = scopes if (scopes is not None) else ALL_SCOPES
        self.public = set(public or [])
        self.admins = set(admins or [])
        for key, value in access_lists.items():
            if isinstance(value, str):
                value = import_object(value)
            self.access_lists[key] = value

    def _get_id(self, principal):
        # Get the id (i.e. username) of this Principal for the
        # associated authentication provider.
        for identity in principal.identities:
            if identity.provider == self.provider:
                id = identity.id
                break
        else:
            raise ValueError(
                f"Principcal {principal} has no identity from provider {self.provider}. "
                f"Its identities are: {principal.identities}"
            )
        return id

    async def allowed_scopes(self, node, principal, authn_scopes):
        # If this is being called, filter_access has let us get this far.
        if principal is SpecialUsers.public:
            allowed = PUBLIC_SCOPES
        elif principal.type == "service":
            allowed = self.scopes
        elif self._get_id(principal) in self.admins:
            allowed = ALL_SCOPES
        # The simple policy does not provide for different Principals to
        # have different scopes on different Nodes. If the Principal has access,
        # they have the same hard-coded access everywhere.
        else:
            allowed = self.scopes
        return allowed

    async def filters(self, node, principal, authn_scopes, scopes):
        queries = []
        query_filter = KeysFilter if not self.key else partial(In, self.key)
        if principal is SpecialUsers.public:
            queries.append(query_filter(self.public))
        else:
            # Services have no identities; just use the uuid.
            if principal.type == "service":
                id = str(principal.uuid)
            else:
                id = self._get_id(principal)
            if id in self.admins:
                return queries
            if not scopes.issubset(self.scopes):
                return NO_ACCESS
            access_list = self.access_lists.get(id, [])
            if not ((principal is SpecialUsers.admin) or (access_list == self.ALL)):
                try:
                    allowed = set(access_list or [])
                except TypeError:
                    # Provide rich debugging info because we have encountered a confusing
                    # bug here in a previous implementation.
                    raise TypeError(
                        f"Unexpected access_list {access_list} of type {type(access_list)}. "
                        f"Expected iterable or {self.ALL}, instance of {type(self.ALL)}."
                    )
                queries.append(query_filter(allowed))
        return queries


def get_group_users(groupname):
    """
    A default method to retrieve group information
    when compiling ACLs from tags. This should be
    replaced by site-specific requirements.
    """
    import grp

    return grp.getgrnam(groupname).gr_mem


class InterningLoader(yaml.loader.BaseLoader):
    pass


def interning_constructor(loader, node):
    value = loader.construct_scalar(node)
    return intern(value)


InterningLoader.add_constructor("tag:yaml.org,2002:str", interning_constructor)


@dataclass(frozen=True)
class LoadedTags:
    tags: dict = field(default_factory=dict)
    public: set = field(default_factory=set)
    scopes: dict = field(default_factory=dict)
    owners: dict = field(default_factory=dict)


class TagBasedAccessPolicy:
    def __init__(
        self,
        *,
        provider,
        tag_config,
        group_parser,
        scopes=None,
    ):
        self.provider = provider
        self.tag_config_path = Path(tag_config)
        self.scopes = scopes if (scopes is not None) else ALL_SCOPES
        self.read_scopes = [intern("read:metadata"), intern("read:data")]
        self.reverse_lookup_scopes = [intern("read:metadata"), intern("read:data")]
        self.unremovable_scopes = [intern("read:metadata"), intern("write:metadata")]
        self.admin_scopes = [intern("admin:apikeys")]
        self.public_tag = intern("public".casefold())
        self.max_tag_nesting = max(_MAX_TAG_NESTING, 0)
        self.group_parser = import_object(group_parser)

        self.roles = {}
        self.tags = {}
        self.tag_owners = {}
        self.compiled_tags = {}
        self.compiled_public = set({self.public_tag})
        self.compiled_scopes = {}
        self.compiled_tag_owners = {}
        self.loaded_tags = LoadedTags()

        self.load_tag_config()
        self.compile()
        self.load_compiled_tags()

    def load_tag_config(self):
        try:
            with open(self.tag_config_path) as tag_config:
                tag_definitions = yaml.load(tag_config, Loader=InterningLoader)
                self.roles.update(tag_definitions.get("roles", {}))
                self.tags.update(tag_definitions["tags"])
                self.tag_owners.update(tag_definitions.get("tag_owners", {}))
        except FileNotFoundError as e:
            raise ValueError(
                f"The tag config path {self.tag_config_path!s} doesn't exist."
            ) from e

    def _dfs(self, current_tag, tags, seen_tags, nested_level=0):
        if current_tag in self.compiled_tags:
            return self.compiled_tags[current_tag], current_tag in self.compiled_public
        if current_tag in seen_tags:
            return {}, False
        if nested_level > self.max_tag_nesting:
            raise RecursionError(
                f"Exceeded maximum tag nesting of {self.max_tag_nesting} levels"
            )

        public_auto_tag = False
        seen_tags.add(current_tag)
        users = {}
        for tag in tags[current_tag]:
            if tag.casefold() == self.public_tag:
                public_auto_tag = True
                continue
            try:
                child_users, child_public = self._dfs(
                    tag, tags, seen_tags, nested_level + 1
                )
                public_auto_tag = public_auto_tag or child_public
                users.update(child_users)
            except (RecursionError, ValueError) as e:
                raise RuntimeError(
                    f"Tag compilation failed at tag: {current_tag}"
                ) from e

        if public_auto_tag:
            self.compiled_public.add(current_tag)

        if "users" in self.tags[current_tag]:
            for user in self.tags[current_tag]["users"]:
                username = user["name"]
                if all(k in user for k in ("scopes", "role")):
                    raise ValueError(
                        f"Cannot define both 'scopes' and 'role' for a user. {username=}"
                    )
                elif not any(k in user for k in ("scopes", "role")):
                    raise ValueError(
                        f"Must define either 'scopes' or 'role' for a user. {username=}"
                    )

                user_scopes = set(
                    self.roles[user["role"]]["scopes"]
                    if ("role" in user) and (user["role"] in self.roles)
                    else user.get("scopes", [])
                )
                if not user_scopes:
                    raise ValueError(f"Scopes must not be empty. {username=}")
                if not user_scopes.issubset(self.scopes):
                    raise ValueError(
                        f"Scopes for {username=} are not in the valid set of scopes. The invalid scopes are:"
                        f"{user_scopes.difference(self.scopes)}"
                    )
                users.setdefault(username, set())
                users[username].update(user_scopes)

        if "groups" in self.tags[current_tag]:
            for group in self.tags[current_tag]["groups"]:
                groupname = group["name"]
                if all(k in group for k in ("scopes", "role")):
                    raise ValueError(
                        f"Cannot define both 'scopes' and 'role' for a group. {groupname=}"
                    )
                elif not any(k in group for k in ("scopes", "role")):
                    raise ValueError(
                        f"Must define either 'scopes' or 'role' for a group. {groupname=}"
                    )

                group_scopes = set(
                    self.roles[group["role"]]["scopes"]
                    if ("role" in group) and (group["role"] in self.roles)
                    else group.get("scopes", [])
                )
                if not group_scopes:
                    raise ValueError(f"Scopes must not be empty. {groupname=}")
                if not group_scopes.issubset(self.scopes):
                    raise ValueError(
                        f"Scopes for {groupname=} are not in the valid set of scopes. The invalid scopes are:"
                        f"{group_scopes.difference(self.scopes)}"
                    )

                try:
                    usernames = self.group_parser(groupname)
                except KeyError:
                    warnings.warn(
                        f"Group with {groupname=} does not exist - skipping",
                        UserWarning,
                    )
                    continue
                else:
                    for username in usernames:
                        username = intern(username)
                        users.setdefault(username, set())
                        users[username].update(group_scopes)

        self.compiled_tags[current_tag] = users
        return users, public_auto_tag

    def compile(self):
        for role in self.roles.values():
            if "scopes" not in role:
                raise ValueError(f"Scopes must be defined for a role. {role=}")
            if not role["scopes"]:
                raise ValueError(f"Scopes must not be empty. {role=}")
            if not set(role["scopes"]).issubset(self.scopes):
                raise ValueError(
                    f"Scopes for {role=} are not in the valid set of scopes. The invalid scopes are:"
                    f'{set(role["scopes"]).difference(self.scopes)}'
                )

        adjacent_tags = {}
        for tag, members in self.tags.items():
            if tag.casefold() == self.public_tag:
                raise ValueError(
                    f"'Public' tag '{self.public_tag}' cannot be redefined."
                )
            adjacent_tags[tag] = set()
            if "auto_tags" in members:
                for auto_tag in members["auto_tags"]:
                    if (
                        auto_tag["name"] not in self.tags
                        and auto_tag["name"].casefold() != self.public_tag
                    ):
                        raise KeyError(
                            f"Tag '{tag}' has nested tag '{auto_tag}' which does not have a definition."
                        )
                    adjacent_tags[tag].add(auto_tag["name"])

        for tag in adjacent_tags:
            try:
                self._dfs(tag, adjacent_tags, set())
            except (RecursionError, ValueError) as e:
                raise RuntimeError(f"Tag compilation failed at tag: {tag}") from e

        for scope in self.reverse_lookup_scopes:
            self.compiled_scopes.setdefault(scope, {})
        for tag, users in self.compiled_tags.items():
            for user, scopes in users.items():
                for scope in self.reverse_lookup_scopes:
                    if scope in scopes:
                        self.compiled_scopes[scope].setdefault(user, set())
                        self.compiled_scopes[scope][user].add(tag)

        for tag in self.tag_owners:
            self.compiled_tag_owners.setdefault(tag, set())
            if "users" in self.tag_owners[tag]:
                for user in self.tag_owners[tag]["users"]:
                    username = user["name"]
                    self.compiled_tag_owners[tag].add(username)
            if "groups" in self.tag_owners[tag]:
                for group in self.tag_owners[tag]["groups"]:
                    groupname = group["name"]
                    try:
                        usernames = self.group_parser(groupname)
                    except KeyError:
                        warnings.warn(
                            f"Group with {groupname=} does not exist - skipping",
                            UserWarning,
                        )
                        continue
                    else:
                        for username in usernames:
                            username = intern(username)
                            self.compiled_tag_owners[tag].add(username)

    def clear_raw_tags(self):
        self.roles = {}
        self.tags = {}
        self.tag_owners = {}

    def recompile(self):
        self.compiled_tags = {}
        self.compiled_public = set({self.public_tag})
        self.compiled_scopes = {}
        self.compiled_tag_owners = {}
        self.compile()

    def load_compiled_tags(self):
        self.loaded_tags = LoadedTags(
            self.compiled_tags.copy(),
            self.compiled_public.copy(),
            deepcopy(self.compiled_scopes),
            deepcopy(self.compiled_tag_owners),
        )

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

    async def init_node(self, principal, authn_scopes, access_blob=None):
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
                if tag.casefold() == self.public_tag:
                    include_public_tag = True
                    if not self._is_admin(authn_scopes):
                        raise ValueError(
                            "Cannot apply 'public' tag to node: only Tiled admins can apply the 'public' tag."
                        )
                elif tag not in self.loaded_tags.tags:
                    raise ValueError(f"Cannot apply tag to node: {tag=} is not defined")
                elif identifier not in self.loaded_tags.owners.get(tag, set()):
                    # admins can ignore the tag ownership check
                    if not self._is_admin(authn_scopes):
                        raise ValueError(
                            f"Cannot apply tag to node: user='{identifier}' is not an owner of {tag=}"
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
                    new_scopes.update(self.loaded_tags.tags[tag].get(identifier, set()))
                if not all(scope in new_scopes for scope in self.unremovable_scopes):
                    raise ValueError(
                        f"Cannot init node with tags: operation does not grant necessary scopes.\n"
                        f"The resulting access_blob would be: {access_blob_from_policy}\n"
                        f"This access_blob does not confer the minimum scopes: {self.unremovable_scopes}"
                    )
        else:
            access_blob_from_policy = {"user": identifier}
            access_blob_modified = True

        logger.info(
            f"Node to be initialized with access_blob: {access_blob_from_policy}"
        )
        # modified means the blob to-be-used was changed in comparison to the user input
        return access_blob_modified, access_blob_from_policy

    async def modify_node(self, node, principal, authn_scopes, access_blob):
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
            elif tag not in self.loaded_tags.tags:
                raise ValueError(f"Cannot apply tag to node: {tag=} is not defined")
            elif identifier not in self.loaded_tags.owners.get(tag, set()):
                # admins can ignore the tag ownership check
                if not self._is_admin(authn_scopes):
                    raise ValueError(
                        f"Cannot apply tag to node: user='{identifier}' is not an owner of {tag=}"
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
                if tag == self.public_tag:
                    if not self._is_admin(authn_scopes):
                        raise ValueError(
                            "Cannot remove 'public' tag from node: only Tiled admins can remove the 'public' tag."
                        )
                elif tag not in self.loaded_tags.tags:
                    raise ValueError(
                        f"Cannot remove tag from node: {tag=} is not defined"
                    )
                elif identifier not in self.loaded_tags.owners.get(tag, set()):
                    # admins can ignore the tag ownership check
                    if not self._is_admin(authn_scopes):
                        raise ValueError(
                            f"Cannot remove tag from node: user='{identifier}' is not an owner of {tag=}"
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
                new_scopes.update(self.loaded_tags.tags[tag].get(identifier, set()))
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

    async def allowed_scopes(self, node, principal, authn_scopes):
        # If this is being called, filter_for_access has let us get this far.
        # However, filters and allowed_scopes should always be implmented to
        # give answers consistent with each other.
        if not hasattr(node, "access_blob"):
            allowed = self.scopes
        elif self._is_admin(authn_scopes):
            allowed = self.scopes
        else:
            if principal.type == "service":
                identifier = str(principal.uuid)
            else:
                identifier = self._get_id(principal)

            allowed = set()
            if "user" in node.access_blob:
                if identifier == node.access_blob["user"]:
                    allowed = self.scopes
            elif "tags" in node.access_blob:
                for tag in node.access_blob["tags"]:
                    if tag in self.loaded_tags.public:
                        allowed.update(self.read_scopes)
                        if tag == self.public_tag:
                            continue
                    elif tag not in self.loaded_tags.tags:
                        continue
                    tag_scopes = self.loaded_tags.tags[tag].get(identifier, set())
                    allowed.update(
                        tag_scopes if tag_scopes.issubset(self.scopes) else set()
                    )

        return allowed

    async def filters(self, node, principal, authn_scopes, scopes):
        queries = []
        query_filter = AccessBlobFilter

        if not hasattr(node, "access_blob"):
            return queries
        if not scopes.issubset(self.scopes):
            return NO_ACCESS
        if not scopes.issubset(self.reverse_lookup_scopes):
            return NO_ACCESS

        if principal.type == "service":
            identifier = str(principal.uuid)
        elif self._is_admin(authn_scopes):
            return queries
        else:
            identifier = self._get_id(principal)

        tag_list = set.intersection(
            *[self.loaded_tags.scopes[scope].get(identifier, set()) for scope in scopes]
        )
        tag_list.update(
            set.intersection(
                *[
                    self.loaded_tags.public if scope in self.read_scopes else set()
                    for scope in scopes
                ]
            )
        )

        queries.append(query_filter(identifier, tag_list))
        return queries
