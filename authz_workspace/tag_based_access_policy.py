import grp
import warnings
from pathlib import Path
from sys import intern

import cachetools
import httpx
import yaml
from access_blob_queries import AccessBlobFilter

from tiled.access_policies import NO_ACCESS
from tiled.scopes import ALL_SCOPES

_MAX_TAG_NESTING = 5

WRITING_SERVICE_ACCOUNT_UUIDS = {}

group_record_cache = cachetools.TTLCache(maxsize=50_000, ttl=14_400)


def get_group_users(groupname):
    try:
        usernames = group_record_cache[groupname]
    except KeyError:
        # logger.debug("%s: Cache miss", username)
        usernames = grp.getgrnam(groupname).gr_mem
        group_record_cache[groupname] = usernames
    else:
        # logger.debug("%s: Cache hit", username)
        pass

    return usernames


class InterningLoader(yaml.loader.BaseLoader):
    pass


def interning_constructor(loader, node):
    value = loader.construct_scalar(node)
    return intern(value)


InterningLoader.add_constructor("tag:yaml.org,2002:str", interning_constructor)


class TagBasedAccessPolicy:
    def __init__(self, *, provider, tag_config, url, scopes=None):
        self.provider = provider
        self.tag_config_path = Path(tag_config)
        self.client = httpx.Client(base_url=url)
        self.scopes = scopes if (scopes is not None) else ALL_SCOPES
        self.read_scopes = [intern("read:metadata"), intern("read:data")]
        self.reverse_lookup_scopes = [intern("read:metadata"), intern("read:data")]
        self.public_tag = intern("public").casefold()
        self.max_tag_nesting = max(_MAX_TAG_NESTING, 0)

        self.roles = {}
        self.tags = {}
        self.tag_owners = {}
        self.compiled_tags = {}
        self.compiled_scopes = {}
        self.compiled_tag_owners = {}
        self.loaded_tags = {}
        self.loaded_scopes = {}
        self.loaded_tag_owners = {}

        self.load_tag_config()
        self.create_tags_root_node()
        self.compile()
        self.load_compiled_tags()

    def load_tag_config(self):
        try:
            with open(self.tag_config_path) as tag_config:
                tag_definitions = yaml.load(tag_config, Loader=InterningLoader)
                self.roles = tag_definitions.get("roles", {})
                self.tags = tag_definitions["tags"]
                self.tag_owners = tag_definitions.get("tag_owners", {})
        except FileNotFoundError as e:
            raise ValueError(
                f"The tag config path {self.tag_config_path!s} doesn't exist."
            ) from e

    @property
    def all_facilities(self):
        return {
            "nsls2": {
                "beamlines": [
                    "six",
                    "hxn",
                    "xfm",
                    "isr",
                    "srx",
                    "bmm",
                    "qas",
                    "sst1",
                    "sst2",
                    "tes",
                    "iss",
                    "ixs",
                    "cms",
                    "chx",
                    "smi",
                    "lix",
                    "xfp",
                    "amx",
                    "fmx",
                    "fxi",
                    "nyx",
                    "esm",
                    "fis",
                    "met",
                    "csx",
                    "ios",
                    "hex",
                    "pdf",
                    "xpd",
                ],
            },
            "lbms": {
                "beamlines": [
                    "krios1",
                    "talos1",
                    "jeol1",
                ],
            },
        }

    async def _get_current_cycle(self, facility):
        cycle_response = self.client.get(f"/v1/facility/{facility}/cycles/current")
        cycle_response.raise_for_status()
        cycle = cycle_response.json()["cycle"]
        return cycle

    async def _load_facility_api(self, facility, cycles=[]):
        if facility not in self.all_facilities:
            raise ValueError(f"Invalid {facility=} is not a known facility.")
        valid_cycles_response = self.client.get(f"/v1/facility/{facility}/cycles")
        valid_cycles_response.raise_for_status()
        valid_cycles = valid_cycles_response.json()["cycles"]
        if not cycles:
            cycles_response = self.client.get(f"/v1/facility/{facility}/cycles")
            cycles_response.raise_for_status()
            cycles = cycles_response.json()["cycles"]
        elif not all(cycle in valid_cycles for cycle in cycles):
            raise ValueError(
                f"Invalid cycles provided for {facility=}. Invalid cycles: {set(cycles).difference(valid_cycles)}"
            )

        proposals_from_api = {}
        for beamline in self.all_facilities[facility]["beamlines"]:
            proposals_from_api.setdefault(beamline, [])
            for cycle in cycles:
                page, page_size = 1, 100
                count = page_size
                while count == page_size:
                    proposals_response = self.client.get(
                        f"/v1/proposals/?beamline={beamline.upper()}&cycle={cycle}&facility={facility}&page_size={page_size}&page={page}&include_directories=false"
                    )
                    proposals_response.raise_for_status()
                    proposals_response_json = proposals_response.json()
                    for proposal in proposals_response_json["proposals"]:
                        proposals_from_api[beamline].append(
                            intern(proposal["data_session"])
                        )
                    count = proposals_response_json["count"]
                    page = page + 1

            proposals_response = self.client.get(
                f"/v1/proposals/commissioning?beamline={beamline}&facility={facility}"
            )
            proposals_response.raise_for_status()
            proposals = proposals_response.json()["commissioning_proposals"]
            proposals_from_api[beamline].extend(
                [intern("pass-" + proposal_id) for proposal_id in proposals]
            )

        return proposals_from_api

    def _generate_tags_from_api(self, proposal_info):
        proposal_role = intern("facility_user")
        for beamline, proposal_list in proposal_info.items():
            beamline_tag = f"{beamline.lower()}_beamline"
            if beamline in ("sst1", "sst2"):
                beamline_tag = "sst_beamline"
            beamline_tag = intern(beamline_tag)

            for proposal in proposal_list:
                if proposal in self.tags:
                    self.tags[proposal].setdefault("groups", [])
                    self.tags[proposal].setdefault("auto_tags", [])
                    # if group is already on tag then the existing role takes precedence,
                    # so that the tag definitions file has priority
                    if not any(
                        (group["name"] == proposal)
                        for group in self.tags[proposal]["groups"]
                    ):
                        self.tags[proposal]["groups"].append(
                            {"name": proposal, "roles": proposal_role}
                        )
                    self.tags[proposal]["auto_tags"].append({"name": beamline_tag})
                else:
                    self.tags[proposal] = {
                        "groups": [{"name": proposal, "role": proposal_role}],
                        "auto_tags": [{"name": beamline_tag}],
                    }

                beamline_tag_owner = (
                    intern(WRITING_SERVICE_ACCOUNT_UUIDS[beamline])
                    if beamline in WRITING_SERVICE_ACCOUNT_UUIDS
                    else None
                )
                if beamline_tag_owner is not None:
                    if proposal in self.tag_owners:
                        self.tag_owners[proposal].setdefault("users", [])
                        self.tag_owners[proposal]["users"].append(
                            {"name": beamline_tag_owner}
                        )
                    else:
                        self.tag_owners[proposal] = {
                            "users": [{"name": beamline_tag_owner}]
                        }

    async def load_proposals_all_cycles(self):
        all_proposal_info = {}
        for facility in self.all_facilities:
            all_proposal_info.update(await self._load_facility_api(facility))
        self._generate_tags_from_api(all_proposal_info)
        return all_proposal_info

    async def load_proposals_current_cycle(self):
        current_proposal_info = {}
        for facility in self.all_facilities:
            current_cycle = await self._get_current_cycle(facility)
            current_proposal_info.update(
                await self._load_facility_api(facility, [current_cycle])
            )
        self._generate_tags_from_api(current_proposal_info)
        return current_proposal_info

    async def update_tags_all_cycles(self):
        """
        Fetch any newly added proposals and load their tags,
        without changing the already-compiled tags
        """
        await self.load_proposals_all_cycles()
        self.create_tags_root_node()
        self.compile()
        self.load_compiled_tags()

    async def reload_tags_all_cycles(self, clear_grp_cache=False):
        """
        Fetch all proposals and reload all tags. This is the same as fresh restart.
        Optionally, clear the group_record_cache to also force group membership
        to be refreshed.
        """
        self.load_tag_config()
        await self.load_proposals_all_cycles()
        self.create_tags_root_node()
        self.recompile()
        self.load_compiled_tags()

    async def update_tags_current_cycle(self):
        """
        Fetch any newly added proposals and load their tags,
        without changing the already-compiled tags
        """
        if clear_grp_cache:
            group_record_cache.clear()
        await self.load_proposals_current_cycle()
        self.create_tags_root_node()
        self.compile()
        self.load_compiled_tags()

    async def reload_tags_current_cycle(self, clear_grp_cache=False):
        """
        Fetch all proposals and reload all tags.This is the same as fresh restart.
        Optionally, clear the group_record_cache to also force group membership
        to be refreshed.
        """
        if clear_grp_cache:
            group_record_cache.clear()
        self.load_tag_config()
        await self.load_proposals_current_cycle()
        self.create_tags_root_node()
        self.recompile()
        self.load_compiled_tags()

    def create_tags_root_node(self):
        for facility in self.all_facilities:
            for beamline in self.all_facilities[facility]["beamlines"]:
                beamline_tag = f"{beamline.lower()}_beamline"
                beamline_root_tag = f"_ROOT_NODE_{beamline.upper()}"
                if beamline in ("sst1", "sst2"):
                    beamline_tag = "sst_beamline"
                    beamline_root_tag = "_ROOT_NODE_SST"
                beamline_tag = intern(beamline_tag)
                beamline_root_tag = intern(beamline_root_tag)
                if beamline_tag in self.tags:
                    # clear out to ensure RootNode tags are not self-included
                    self.tags[beamline_root_tag] = {}
                    self.tags[beamline_root_tag] = {
                        "auto_tags": [
                            {"name": tag}
                            for tag, members in self.tags.items()
                            if any(
                                auto_tag["name"] == beamline_tag
                                for auto_tag in members.get("auto_tags", [])
                            )
                        ]
                    }
                    self.tags[beamline_root_tag]["auto_tags"].append(
                        {"name": beamline_tag}
                    )

    def _dfs(self, current_tag, tags, seen_tags, nested_level=0):
        if current_tag in self.compiled_tags:
            return self.compiled_tags[current_tag]
        if current_tag in seen_tags:
            return {}
        if nested_level > self.max_tag_nesting:
            raise RecursionError(
                f"Exceeded maximum tag nesting of {max_nesting} levels"
            )

        seen_tags.add(current_tag)
        users = {}
        for tag in tags[current_tag]:
            try:
                users.update(self._dfs(tag, tags, seen_tags, nested_level + 1))
            except (RecursionError, ValueError) as e:
                raise RuntimeError(
                    f"Tag compilation failed at tag: {current_tag}"
                ) from e

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
                    user["scopes"]
                    if "scopes" in user
                    else self.roles[user["role"]]["scopes"]
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
                    group["scopes"]
                    if "scopes" in group
                    else self.roles[group["role"]]["scopes"]
                )
                if not group_scopes:
                    raise ValueError(f"Scopes must not be empty. {groupname=}")
                if not group_scopes.issubset(self.scopes):
                    raise ValueError(
                        f"Scopes for {groupname=} are not in the valid set of scopes. The invalid scopes are:"
                        f"{group_scopes.difference(self.scopes)}"
                    )

                try:
                    usernames = get_group_users(groupname)
                except KeyError:
                    warnings.warn(
                        f"Group with {groupname=} does not exist on the system - skipping",
                        UserWarning,
                    )
                    continue
                else:
                    for username in usernames:
                        username = intern(username)
                        users.setdefault(username, set())
                        users[username].update(group_scopes)

        self.compiled_tags[current_tag] = users
        return users

    def compile(self):
        for role in self.roles.values():
            if "scopes" not in role:
                raise ValueError(f"Scopes must be defined for a role. {role=}")
            if not role["scopes"]:
                raise ValueError(f"Scopes must not be empty. {role=}")
            if not set(role["scopes"]).issubset(self.scopes):
                raise ValueError(
                    f"Scopes for {role=} are not in the valid set of scopes. The invalid scopes are:"
                    f'{role["scopes"].difference(self.scopes)}'
                )

        adjacent_tags = {}
        for tag, members in self.tags.items():
            adjacent_tags[tag] = set()
            if "auto_tags" in members:
                for auto_tag in members["auto_tags"]:
                    if auto_tag["name"] not in self.tags:
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
                        usernames = get_group_users(groupname)
                    except KeyError:
                        warnings.warn(
                            f"Group with {groupname=} does not exist on the system - skipping",
                            UserWarning,
                        )
                        continue
                    else:
                        for username in usernames:
                            username = intern(username)
                            self.compiled_tag_owners[tag].add(username)

    def recompile(self):
        self.compiled_tags = {}
        self.compiled_scopes = {}
        self.compiled_tag_owners = {}
        self.compile()

    def load_compiled_tags(self):
        self.loaded_tags = self.compiled_tags.copy()
        self.loaded_scopes = self.compiled_scopes.copy()
        self.loaded_tag_owners = self.compiled_tag_owners.copy()

    def _get_id(self, principal):
        for identity in principal.identities:
            if identity.provider == self.provider:
                return identity.id
        else:
            raise ValueError(
                f"Principal {principal} has no identity from provider {self.provider}."
                f"The Principal's identities are: {principal.identities}"
            )

    def _is_admin(self, principal):
        for role in principal.roles:
            if role.name == "admin":
                return True
        return False

    async def init_node(self, principal, access_blob=None):
        if principal.type == "service":
            identifier = str(principal.uuid)
        else:
            identifier = self._get_id(principal)

        if access_blob:
            if not "tags" in access_blob:
                raise ValueError(
                    f"""access_blob must be in the form '{"tags": ["tag1", "tag2", ...]}'"""
                    f"""Received {access_blob=}"""
                )
            access_tags = set(access_blob["tags"])
            include_public_tag = False
            for tag in access_tags:
                if tag.casefold() == self.public_tag:
                    include_public_tag = True
                    if not self._is_admin(principal):
                        raise ValueError(
                            f"Cannot apply 'public' tag to dataset: only Tiled admins can apply the 'public' tag."
                        )
                elif tag not in self.loaded_tags:
                    raise ValueError(
                        f"Cannot apply tag to dataset: {tag=} is not defined"
                    )
                elif identifier not in self.loaded_tag_owners.get(tag, set()):
                    raise ValueError(
                        f"Cannot apply tag to dataset: user='{identifier}' is not an owner of {tag=}"
                    )
            access_blob_from_policy["tags"] = {
                tag for tag in access_tags if tag.casefold() != self.public_tag
            }
            if include_public_tag:
                access_blob_from_policy["tags"].add(self.public_tag)
        else:
            access_blob_from_policy["user"] = identifier

        return access_blob_from_policy

    async def allowed_scopes(self, node, principal, path_parts):
        # If this is being called, filter_for_access has let us get this far.
        # However, filters and allowed_scopes should always be implmented to
        # give answers consistent with each other.
        if not hasattr(node, "access_blob"):
            allowed = self.scopes
        elif self._is_admin(principal):
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
                    if tag == self.public_tag:
                        allowed.update(self.read_scopes)
                        continue
                    elif tag not in self.loaded_tags:
                        continue
                    tag_scopes = self.loaded_tags[tag].get(identifier, set())
                    allowed.update(
                        tag_scopes if tag_scopes.issubset(self.scopes) else set()
                    )
        return allowed

    async def filters(self, node, principal, scopes, path_parts):
        queries = []
        query_filter = lambda value_id, value_tags: AccessBlobFilter(
            "user", value_id, "tags", value_tags
        )
        if not hasattr(node, "access_blob"):
            return queries
        if not scopes.issubset(self.scopes):
            return NO_ACCESS
        if not scopes.issubset(self.reverse_lookup_scopes):
            return NO_ACCESS

        if principal.type == "service":
            identifier = str(principal.uuid)
        elif self._is_admin(principal):
            return queries
        else:
            identifier = self._get_id(principal)

        tag_list = set.intersection(
            *[self.loaded_scopes[scope].get(identifier, set()) for scope in scopes],
            *[
                self.public_tag if scope in self.read_scopes else set()
                for scope in scopes
            ],
        )
        queries.append(query_filter(identifier, tag_list))
        return queries
