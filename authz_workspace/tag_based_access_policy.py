import grp
import warnings
from pathlib import Path
from sys import intern

import httpx
import yaml
from access_blob_queries import AccessBlobFilter

from tiled.access_policies import NO_ACCESS
from tiled.scopes import ALL_SCOPES

_MAX_TAG_NESTING = 5

WRITING_SERVICE_ACCOUNT_UUIDS = {}


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
        self.read_metadata_scope = intern("read:metadata")

        self.max_tag_nesting = max(_MAX_TAG_NESTING, 0)
        self.roles = {}
        self.tags = {}
        self.tag_owners = {}
        self.compiled_tags = {}
        self.compiled_users = {}
        self.compiled_tag_owners = {}

        self.load_tag_config()
        self.create_root_tags()
        self.compile()

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

    async def get_current_cycle(self, facility):
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
            intern(beamline_tag)

            for proposal in proposal_list:
                if proposal in self.tags:
                    self.tags[proposal].setdefault("groups", [])
                    self.tags[proposal].setdefault("tags", [])
                    # if group is already on tag then the existing role takes precedence,
                    # so that the tag definitions file has priority
                    if not any(
                        (group["name"] == proposal)
                        for group in self.tags[proposal]["groups"]
                    ):
                        self.tags[proposal]["groups"].append(
                            {"name": proposal, "roles": proposal_role}
                        )
                    self.tags[proposal]["tags"].append({"name": beamline_tag})
                else:
                    self.tags[proposal] = {
                        "groups": [{"name": proposal, "role": proposal_role}],
                        "tags": [{"name": beamline_tag}],
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
            current_cycle = await self.get_current_cycle(facility)
            current_proposal_info.update(
                await self._load_facility_api(facility, [current_cycle])
            )
        self._generate_tags_from_api(current_proposal_info)
        return current_proposal_info

    async def reload_tags_all_cycles(self):
        self.load_tag_config()
        await self.load_proposals_all_cycles()
        self.create_root_tags()

    async def reload_tags_current_cycle(self):
        self.load_tag_config()
        await self.load_proposals_current_cycle()
        self.create_root_tags()

    def create_root_tags(self):
        for facility in self.all_facilities:
            for beamline in self.all_facilities[facility]["beamlines"]:
                beamline_tag = f"{beamline.lower()}_beamline"
                beamline_root_tag = f"_ROOT_NODE_{beamline.upper()}"
                if beamline in ("sst1", "sst2"):
                    beamline_tag = "sst_beamline"
                    beamline_root_tag = "_ROOT_NODE_SST"
                intern(beamline_tag)
                intern(beamline_root_tag)
                if beamline_tag in self.tags:
                    self.tags[beamline_root_tag] = (
                        {}
                    )  # clear out to ensure root tag is not self-included
                    self.tags[beamline_root_tag] = {
                        "tags": [
                            {"name": tag}
                            for tag, members in self.tags.items()
                            if any(
                                member_tag["name"] == beamline_tag
                                for member_tag in members.get("tags", [])
                            )
                        ]
                    }
                    self.tags[beamline_root_tag]["tags"].append({"name": beamline_tag})

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
                    usernames = grp.getgrnam(groupname).gr_mem
                except KeyError:
                    warnings.warn(
                        f"Group with {groupname=} does not exist on the system - skipping",
                        UserWarning,
                    )
                    continue
                else:
                    for username in usernames:
                        intern(username)
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
            if "tags" in members:
                for member_tag in members["tags"]:
                    if member_tag["name"] not in self.tags:
                        raise KeyError(
                            f"Tag '{tag}' has nested tag '{member_tag}' which does not have a definition."
                        )
                    adjacent_tags[tag].add(member_tag["name"])

        for tag in adjacent_tags:
            try:
                self._dfs(tag, adjacent_tags, set())
            except (RecursionError, ValueError) as e:
                raise RuntimeError(f"Tag compilation failed at tag: {tag}") from e

        for tag, users in self.compiled_tags.items():
            for user, scopes in users.items():
                if self.read_metadata_scope in scopes:
                    self.compiled_users.setdefault(user, set())
                    self.compiled_users[user].add(tag)

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
                        usernames = grp.getgrnam(groupname).gr_mem
                    except KeyError:
                        warnings.warn(
                            f"Group with {groupname=} does not exist on the system - skipping",
                            UserWarning,
                        )
                        continue
                    else:
                        for username in usernames:
                            intern(username)
                            self.compiled_tag_owners[tag].add(username)

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

    async def init_node(self, principal, access_tags=None):
        if principal.type == "service":
            identifier = str(principal.uuid)
        else:
            identifier = self._get_id(principal)

        if access_tags:
            access_tags = set(access_tags)
            for tag in access_tags:
                if tag not in self.compiled_tags:
                    raise ValueError(
                        f"Cannot apply tag to dataset: {tag=} is not defined"
                    )
                if identifier not in self.compiled_tag_owners.get(tag, set()):
                    raise ValueError(
                        f"Cannot apply tag to dataset: you are not an owner of {tag=}"
                    )
            access_blob = {"tags": list(access_tags)}
        else:
            access_blob = {"user": identifier}

        return access_blob

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

            if "user" in node.access_blob:
                if identifier == node.access_blob["user"]:
                    allowed = self.scopes
            elif "tags" in node.access_blob:
                allowed = set()
                for tag in node.access_blob["tags"]:
                    if tag not in self.compiled_tags:
                        continue
                    tag_scopes = self.compiled_tags[tag].get(identifier, set())
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

        if principal.type == "service":
            identifier = str(principal.uuid)
        elif self._is_admin(principal):
            return queries
        else:
            identifier = self._get_id(principal)

        tag_list = self.compiled_users.get(identifier, set())
        queries.append(query_filter(identifier, tag_list))
        return queries
