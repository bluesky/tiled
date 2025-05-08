import asyncio
import grp
import logging
import os
import pickle
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path
from sys import intern

import cachetools
import httpx

from tiled.access_policies import TagBasedAccessPolicy

TILED_TBAP_GROUP_CACHE_MAXSIZE = 55_000
TILED_TBAP_GROUP_CACHE_TTL = 3300  # seconds

_TAG_SYNC_TICK_RATE = 15  # seconds

WRITING_SERVICE_ACCOUNT_UUIDS = {}

group_record_cache = cachetools.TTLCache(
    maxsize=TILED_TBAP_GROUP_CACHE_MAXSIZE, ttl=TILED_TBAP_GROUP_CACHE_TTL
)

TAG_SYNC_LOCK = asyncio.Lock()
sync_tags_tasks = []


logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setLevel("DEBUG")
handler.setFormatter(logging.Formatter("TAG BASED PASS ACCESS POLICY: %(message)s"))
logger.addHandler(handler)

log_level = os.getenv("TAG_BASED_PASS_ACCESS_POLICY_LOG_LEVEL")
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
            logger.info(f"Initial run of coroutine '{coro.__qualname__}' at {now}")
        elif task["last_run"] >= task["next_run"]:
            # fell behind, skip ahead to next scheduled cycle
            task["next_run"] = calculate_next_cycle(now, task["next_run"], period)
            logger.error(
                f"Task '{coro.__qualname__}' fell behind, skipping ahead to next scheduled cycle"
            )
        elif now >= task["next_run"]:
            loop.create_task(coro(*args, **kwargs))
            task["next_run"] += timedelta(minutes=period)
            task["last_run"] = now
        logger.info(f"""Last run of '{coro.__qualname__}': {task["last_run"]}""")
        logger.info(f"""Next run of '{coro.__qualname__}': {task["next_run"]}""")


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


def _sync_tags_exc_handler(loop, context):
    task = context.get("task") or context.get("future")
    if task is None:
        msg = context.get("message")
        logger.debug(f"Exception handler invoked, but there is no task: {msg}")

    e = task.exception()
    e_type = e.__class__.__name__
    coro_name = task.get_coro().__qualname__
    logger.exception(f"Caught '{e_type}' exception from '{coro_name}': {e}")


sync_tags_loop = asyncio.new_event_loop()
sync_tags_loop.set_exception_handler(_sync_tags_exc_handler)
sync_tags_thread = threading.Thread(
    target=_sync_tags_start_loop, args=(sync_tags_loop,), daemon=True
)
sync_tags_thread.start()


def get_group_users(groupname):
    try:
        usernames = group_record_cache[groupname]
    except KeyError:
        logger.debug("%s: Cache miss on group record", groupname)
        usernames = grp.getgrnam(groupname).gr_mem
        group_record_cache[groupname] = usernames
    else:
        logger.debug("%s: Cache hit on group record", groupname)

    return usernames


def load_tags_file(tags_db):
    if not tags_db.is_file():
        raise FileNotFoundError(f"sqlite DB not found: {tags_db}")

    conn = sqlite3.connect(tags_db)
    cursor = conn.cursor()
    cursor.execute("SELECT obj_blob FROM store WHERE obj_name = ?", ("tags_state",))
    row = cursor.fetchone()
    loaded_tags = pickle.loads(row[0]) if row else None
    if not loaded_tags:
        raise ValueError(f"Could not load tags state from DB: {tags_db}")

    return loaded_tags


def dump_tags_file(tags_db, loaded_tags):
    conn = sqlite3.connect(tags_db)
    cursor = conn.cursor()
    cursor.execute(
        """
CREATE TABLE IF NOT EXISTS store (
    obj_name TEXT PRIMARY KEY,
    obj_blob BLOB
)
"""
    )
    loaded_tags_blob = sqlite3.Binary(
        pickle.dumps(loaded_tags, protocol=pickle.HIGHEST_PROTOCOL)
    )
    cursor.execute(
        "REPLACE INTO store (obj_name, obj_blob) VALUES (?, ?)",
        ("tags_state", loaded_tags_blob),
    )
    conn.commit()


class TagBasedPASSAccessPolicy(TagBasedAccessPolicy):
    def __init__(self, url, tags_db, sync_proposals, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tags_db = Path(tags_db)
        self.client = httpx.AsyncClient(base_url=url)
        self.sync_proposals = sync_proposals or {}

        try:
            self.loaded_tags = load_tags_file(self.tags_db)
            logger.info(f"Loaded previous tags state from file: '{self.tags_db}'")
        except (FileNotFoundError, ValueError) as e:
            logger.info(
                f"Could not load previous tags state from file: '{self.tags_db}'\n"
                f"Caught exception '{type(e).__name__}': {e}"
            )
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
                        f"/v1/proposals/?beamline={beamline.upper()}&cycle={cycle}&facility={facility}&page_size={page_size}&page={page}&include_directories=false"  # noqa: E501
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
                        group["name"] == proposal
                        for group in self.tags[proposal]["groups"]
                    ):
                        self.tags[proposal]["groups"].append(
                            {"name": proposal, "role": proposal_role}
                        )
                    if not any(
                        auto_tag["name"] == beamline_tag
                        for auto_tag in self.tags[proposal]["auto_tags"]
                    ):
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
                        if not any(
                            owner["name"] == beamline_tag_owner
                            for owner in self.tag_owners[proposal]["users"]
                        ):
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
            logger.info("Synchronizing from API for ALL cycles - update")
            await self.load_proposals_all_cycles()
            self.compile()
            self.load_compiled_tags()

    async def reload_tags_all_cycles(self, clear_grp_cache=False):
        """
        Fetch all proposals and reload all tags. This is the same as fresh restart.
        Optionally, clear the group_record_cache to also force group membership
        to be refreshed.
        """
        async with TAG_SYNC_LOCK:
            logger.info("Synchronizing from API for ALL cycles - reload")
            if clear_grp_cache:
                group_record_cache.clear()
            self.clear_raw_tags()
            self.load_tag_config()
            await self.load_proposals_all_cycles()
            self.create_tags_root_node()
            self.recompile()
            self.load_compiled_tags()
            dump_tags_file(self.tags_db, self.loaded_tags)

    async def update_tags_current_cycle(self):
        """
        Fetch any newly added proposals and load their tags,
        without changing the already-compiled tags
        """
        try:
            logger.debug("Acquiring lock for sync of current cycle - update")
            await asyncio.wait_for(TAG_SYNC_LOCK.acquire(), timeout=0.1)
        except (TimeoutError, asyncio.TimeoutError):
            logger.error("Lock failed: skipping sync of current cycle")
            return

        try:
            logger.info("Syncing current cycle")
            await self.load_proposals_current_cycle()
            self.compile()
            self.load_compiled_tags()
        finally:
            TAG_SYNC_LOCK.release()
            logger.debug("Releasing lock...")

    async def reload_tags_current_cycle(self, clear_grp_cache=False):
        """
        Fetch current proposals and reload their tags. This is a partial refresh,
        not a fresh restart. Additions and changes in the tag config will be
        pulled in, but not deletions (i.e. tags that were removed).
        Current-cycle proposals will be recompiled.
        Optionally, clear the group_record_cache to also force group membership
        to be refreshed.
        """
        try:
            logger.debug("Acquiring lock for sync of current cycle - reload")
            await asyncio.wait_for(TAG_SYNC_LOCK.acquire(), timeout=0.1)
        except (TimeoutError, asyncio.TimeoutError):
            logger.error("Lock failed: skipping sync of current cycle")
            return

        try:
            logger.info("Syncing current cycle")
            if clear_grp_cache:
                group_record_cache.clear()
            self.load_tag_config()
            current_proposal_info = await self.load_proposals_current_cycle()
            self.create_tags_root_node()
            self.recompile_current_cycle(current_proposal_info)
            self.load_compiled_tags()
            dump_tags_file(self.tags_db, self.loaded_tags)
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

    def recompile_current_cycle(self, current_proposal_info):
        for proposal_list in current_proposal_info.values():
            for proposal in proposal_list:
                self.compiled_tags.pop(proposal, None)
                self.compiled_public.discard(proposal)
                self.compiled_tag_owners.pop(proposal, None)
        # this is a many-to-one relationship for tags,
        # must recompile the whole structure
        self.compiled_scopes = {}
        self.compile()
