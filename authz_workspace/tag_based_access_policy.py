import asyncio
import grp
import threading
import warnings
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from sys import intern
from typing import NamedTuple

import cachetools
import httpx
import yaml
from access_blob_queries import AccessBlobFilter

from tiled.access_policies import NO_ACCESS
from tiled.scopes import ALL_SCOPES


TILED_TBAP_GROUP_CACHE_MAXSIZE = 55_000
TILED_TBAP_GROUP_CACHE_TTL = 3600  # seconds

_TAG_SYNC_TICK_RATE = 15  # seconds
_MAX_TAG_NESTING = 5

WRITING_SERVICE_ACCOUNT_UUIDS = {}

group_record_cache = cachetools.TTLCache(
    maxsize=TILED_TBAP_GROUP_CACHE_MAXSIZE, ttl=TILED_TBAP_GROUP_CACHE_TTL
)

TAG_SYNC_LOCK = asyncio.Lock()
sync_tags_tasks = []


if __debug__:
    import logging
    import os

    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setLevel("DEBUG")
    handler.setFormatter(logging.Formatter("TAG BASED ACCESS POLICY: %(message)s"))
    logger.addHandler(handler)

    log_level = os.getenv("TAG_BASED_ACCESS_POLICY_LOG_LEVEL")
    if log_level:
        logger.setLevel(log_level.upper())


def calculate_next_cycle(now: datetime, ref: datetime, period):
    # calculate next cycle using a reference point
    minutes_since_ref = (now - ref).total_seconds() // 60
    minutes_until_cycle = period - (minutes_since_ref % period)
    next_cycle_at = now + timedelta(minutes=minutes_until_cycle)
    return next_cycle_at


def create_sync_tags_tasks(coro, *args, period=1, **kwargs):
    now = datetime.now().replace(second=0, microsecond=0)
    # using midnight as starting reference
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    next_cycle_at = calculate_next_cycle(now, midnight, period)
    sync_tags_tasks.append(
        {
            "coro": (coro, args, kwargs),
            "period": period,
            "next_run": next_cycle_at,
            "last_run": None,
        }
    )


def _sync_tags_run_tasks(loop: asyncio.AbstractEventLoop, now: datetime):
    now = now.replace(second=0, microsecond=0)
    for task in sync_tags_tasks:
        if task["last_run"] == now:
            continue

        coro, args, kwargs = task["coro"]
        period = task["period"]
        if task["last_run"] is None:
            loop.create_task(coro(*args, **kwargs))
            task["last_run"] = now
            logger.debug(f"Initial run of coroutine '{coro.__name__}' at {now}")
        elif task["last_run"] >= task["next_run"]:
            # fell behind, skip ahead to next scheduled cycle
            task["next_run"] = calculate_next_cycle(now, task["next_run"], period)
            logger.debug(
                "Task '{coro.__name__}' fell behind, skipping ahead to next scheduled cycle"
            )
        elif now >= task["next_run"]:
            loop.create_task(coro(*args, **kwargs))
            task["next_run"] += timedelta(minutes=period)
            task["last_run"] = now
        logger.debug(f"""Last run of '{coro.__name__}': {task["last_run"]}""")
        logger.debug(f"""Next run of '{coro.__name__}': {task["next_run"]}""")


async def _sync_tags_scheduler():
    loop = asyncio.get_running_loop()

    # this logic normalizes update ticks to even wall-clock times.
    # this way, the schedule is predictable regardless of when
    # the application starts.
    now = datetime.now()
    tick_rate = _TAG_SYNC_TICK_RATE
    tick_rate = min(3600, tick_rate)  # this logic works up to 1 hour tick
    seconds_since_hour = (now.minute * 60) + now.second
    cycles_since_hour = seconds_since_hour // tick_rate  # floor div
    last_cycle_seconds = cycles_since_hour * tick_rate
    next_cycle_seconds = last_cycle_seconds + tick_rate
    next_cycle_at = now.replace(minute=0, second=0, microsecond=0) + timedelta(
        seconds=next_cycle_seconds
    )
    next_tick = loop.time() + (next_cycle_at - now).total_seconds()

    while True:
        await asyncio.sleep(max(0, next_tick - loop.time()))
        now = datetime.now()
        logger.debug(f"Tag Sync Event Loop Tick {now}")
        _sync_tags_run_tasks(loop, now)
        next_tick += tick_rate


def _sync_tags_start_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.create_task(_sync_tags_scheduler())
    loop.run_forever()


sync_tags_loop = asyncio.new_event_loop()
sync_tags_thread = threading.Thread(
    target=_sync_tags_start_loop, args=(sync_tags_loop,), daemon=True
)
sync_tags_thread.start()


def get_group_users(groupname):
    try:
        usernames = group_record_cache[groupname]
    except KeyError:
        # logger.debug("%s: Cache miss", groupname)
        usernames = grp.getgrnam(groupname).gr_mem
        group_record_cache[groupname] = usernames
    else:
        # logger.debug("%s: Cache hit", groupname)
        pass

    return usernames


class InterningLoader(yaml.loader.BaseLoader):
    pass


def interning_constructor(loader, node):
    value = loader.construct_scalar(node)
    return intern(value)


InterningLoader.add_constructor("tag:yaml.org,2002:str", interning_constructor)


class LoadedTags(NamedTuple):
    tags: dict
    public: set
    scopes: dict
    owners: dict


class TagBasedAccessPolicy:
    def __init__(self, *, provider, tag_config, url, scopes=None, sync_proposals={}):
        self.provider = provider
        self.tag_config_path = Path(tag_config)
        self.client = httpx.AsyncClient(base_url=url)
        self.scopes = scopes if (scopes is not None) else ALL_SCOPES
        self.read_scopes = [intern("read:metadata"), intern("read:data")]
        self.reverse_lookup_scopes = [intern("read:metadata"), intern("read:data")]
        self.unremovable_scopes = [intern("read:metadata"), intern("write:metadata")]
        self.public_tag = intern("public").casefold()
        self.max_tag_nesting = max(_MAX_TAG_NESTING, 0)

        self.roles = {}
        self.tags = {}
        self.tag_owners = {}
        self.compiled_tags = {}
        self.compiled_public = set({self.public_tag})
        self.compiled_scopes = {}
        self.compiled_tag_owners = {}
        self.loaded_tags = LoadedTags({}, {}, set(), {})

        self.load_tag_config()
        self.create_tags_root_node()
        self.compile()
        self.load_compiled_tags()

        if not all(rate in sync_proposals for rate in ("rate_all", "rate_current")):
            raise ValueError("Must specify rates for syncing proposals from NSLS2 API.")

        create_sync_tags_tasks(
            self.reload_tags_all_cycles, period=sync_proposals["rate_all"]
        )
        create_sync_tags_tasks(
            self.reload_tags_current_cycle, period=sync_proposals["rate_current"]
        )

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
        cycle_response = await self.client.get(
            f"/v1/facility/{facility}/cycles/current"
        )
        cycle_response.raise_for_status()
        cycle = cycle_response.json()["cycle"]
        return cycle

    async def _load_facility_api(self, facility, cycles=[]):
        if facility not in self.all_facilities:
            raise ValueError(f"Invalid {facility=} is not a known facility.")
        valid_cycles_response = await self.client.get(f"/v1/facility/{facility}/cycles")
        valid_cycles_response.raise_for_status()
        valid_cycles = valid_cycles_response.json()["cycles"]
        if not cycles:
            cycles_response = await self.client.get(f"/v1/facility/{facility}/cycles")
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
                    proposals_response = await self.client.get(
                        f"/v1/proposals/?beamline={beamline.upper()}&cycle={cycle}&facility={facility}&page_size={page_size}&page={page}&include_directories=false"
                    )
                    proposals_response.raise_for_status()
                    proposals_response_json = proposals_response.json()
                    for proposal in proposals_response_json["proposals"]:
                        proposals_from_api[beamline].append(
                            intern(proposal["data_session"])
                        )
                    count = proposals_response_json["count"]
                    page += 1

            proposals_response = await self.client.get(
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
        async with TAG_SYNC_LOCK:
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
        async with TAG_SYNC_LOCK:
            logger.debug("Updating ALL cycles")
            if clear_grp_cache:
                group_record_cache.clear()
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
        try:
            logger.debug("Acquiring lock for sync of current cycle")
            await asyncio.wait_for(TAG_SYNC_LOCK.acquire(), timeout=0.1)
        except (TimeoutError, asyncio.TimeoutError):
            logger.debug("Lock failed: skipping sync of current cycle")
            return

        try:
            logger.debug("Syncing current cycle")
            await self.load_proposals_current_cycle()
            self.create_tags_root_node()
            self.compile()
            self.load_compiled_tags()
        finally:
            TAG_SYNC_LOCK.release()
            logger.debug("Releasing lock...")

    async def reload_tags_current_cycle(self, clear_grp_cache=False):
        """
        Fetch all proposals and reload all tags.This is the same as fresh restart.
        Optionally, clear the group_record_cache to also force group membership
        to be refreshed.
        """
        try:
            logger.debug("Acquiring lock for sync of current cycle")
            await asyncio.wait_for(TAG_SYNC_LOCK.acquire(), timeout=0.1)
        except (TimeoutError, asyncio.TimeoutError):
            logger.debug("Lock failed: skipping sync of current cycle")
            return

        try:
            logger.debug("Syncing current cycle")
            if clear_grp_cache:
                group_record_cache.clear()
            self.load_tag_config()
            await self.load_proposals_current_cycle()
            self.create_tags_root_node()
            self.recompile()
            self.load_compiled_tags()
        finally:
            TAG_SYNC_LOCK.release()
            logger.debug("Releasing lock...")

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
                    self.tags[beamline_root_tag] = {
                        "auto_tags": [
                            {"name": beamline_tag},
                            {"name": self.public_tag},
                        ]
                    }

    def _dfs(self, current_tag, tags, seen_tags, nested_level=0):
        if current_tag in self.compiled_tags:
            return self.compiled_tags[current_tag], current_tag in self.compiled_public
        if current_tag in seen_tags:
            return {}, False
        if nested_level > self.max_tag_nesting:
            raise RecursionError(
                f"Exceeded maximum tag nesting of {max_nesting} levels"
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
                    f'{role["scopes"].difference(self.scopes)}'
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
                    f"""access_blob must be in the form '{{"tags": ["tag1", "tag2", ...]}}'"""
                    f"""Received {access_blob=}"""
                )
            access_tags = set(access_blob["tags"])
            include_public_tag = False
            for tag in access_tags:
                if tag.casefold() == self.public_tag:
                    include_public_tag = True
                    if not self._is_admin(principal):
                        raise ValueError(
                            f"Cannot apply 'public' tag to node: only Tiled admins can apply the 'public' tag."
                        )
                elif tag not in self.loaded_tags.tags:
                    raise ValueError(f"Cannot apply tag to node: {tag=} is not defined")
                elif identifier not in self.loaded_tags.owners.get(tag, set()):
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
        else:
            access_blob_from_policy = {"user": identifier}
            access_blob_modified = True

        # modified means the blob to-be-used was changed in comparison to the user input
        return access_blob_modified, access_blob_from_policy

    async def modify_node(self, node, principal, access_blob):
        if principal.type == "service":
            identifier = str(principal.uuid)
        else:
            identifier = self._get_id(principal)

        if not "tags" in access_blob:
            raise ValueError(
                f"""access_blob must be in the form '{{"tags": ["tag1", "tag2", ...]}}'"""
                f"""Received {access_blob=}"""
            )
        access_tags = set(access_blob["tags"])
        include_public_tag = False
        # check for tags that need to be added
        for tag in access_tags:
            if tag in node.access_blob.get("tags", []):
                # node already has this tag - no action.
                # or: access_blob does not have "tags" key,
                # so it must hvae a "user" key currently
                continue
            if tag.casefold() == self.public_tag:
                include_public_tag = True
                if not self._is_admin(principal):
                    raise ValueError(
                        f"Cannot apply 'public' tag to node: only Tiled admins can apply the 'public' tag."
                    )
            elif tag not in self.loaded_tags.tags:
                raise ValueError(f"Cannot apply tag to node: {tag=} is not defined")
            elif identifier not in self.loaded_tags.owners.get(tag, set()):
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
                    if not self._is_admin(principal):
                        raise ValueError(
                            f"Cannot remove 'public' tag from node: only Tiled admins can remove the 'public' tag."
                        )
                elif tag not in self.loaded_tags.tags:
                    raise ValueError(
                        f"Cannot remove tag from node: {tag=} is not defined"
                    )
                elif identifier not in self.loaded_tags.owners.get(tag, set()):
                    raise ValueError(
                        f"Cannot remove tag from node: user='{identifier}' is not an owner of {tag=}"
                    )

        access_blob_from_policy = {"tags": list(access_tags_from_policy)}
        access_blob_modified = access_tags != access_tags_from_policy

        # check that the access_blob change would not result in invalid scopes for user.
        # this applies when removing tags, but also must be done when
        # switching from user-owned node to shared (tagged) node
        new_scopes = set()
        for tag in access_tags_from_policy:
            new_scopes.update(self.loaded_tags.tags[tag][identifier])
        if not all(scope in new_scopes for scope in self.unremovable_scopes):
            raise ValueError(
                f"Cannot modify tags on node: operation removes unremovable scopes."
                f"The current access_blob is: {node.access_blob}"
                f"The new access_blob would be: {access_blob_from_policy}"
                f"These scopes cannot be self-removed: {self.unremovable_scopes}"
            )

        # modified means the blob to-be-used was changed in comparison to the user input
        return access_blob_modified, access_blob_from_policy

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
