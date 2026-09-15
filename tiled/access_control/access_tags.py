import warnings
from pathlib import Path
from sys import intern

import yaml
from sqlalchemy import delete, func, insert, inspect, select, tuple_, update
from sqlalchemy.dialects.postgresql import insert as postgresql_upsert
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert

from ..authn_database import orm as authn_orm
from ..catalog import orm
from ..graph import orm as graph_orm
from ..server.connection_pool import get_database_engine
from ..server.schemas import PrincipalType
from ..utils import InterningLoader
from .protocols import PRINCIPAL_TAG_PREFIXES

# The access_tag_principal_scopes junction table, joined to the tables its
# three foreign keys reference so that it can be queried by name. Shared by
# the lookups in both directions: (tag, principal) -> scopes and
# (principal, scope) -> tags.
access_tag_principal_scopes_named = (
    orm.AccessTagPrincipalScope.__table__.join(
        orm.AccessTag.__table__,
        orm.AccessTag.id == orm.AccessTagPrincipalScope.tag_id,
    )
    .join(
        orm.AccessTagsPrincipal.__table__,
        orm.AccessTagsPrincipal.id == orm.AccessTagPrincipalScope.principal_id,
    )
    .join(
        orm.Scope.__table__,
        orm.Scope.id == orm.AccessTagPrincipalScope.scope_id,
    )
)


class AccessTagsParser:
    """
    Read access tag definitions from the catalog database.

    Serves the lookups that TagBasedAccessPolicy needs. Tag definitions live
    in the catalog database (written there by the access tags compiler), so
    the parser connects with the catalog's own database settings -- there is
    no separate tag database to configure. Queries are SQLAlchemy Core
    statements and run on any supported dialect (SQLite and PostgreSQL).
    """

    def __init__(self, engine=None):
        self._engine = engine

    async def connect(self, database_settings):
        if self._engine is None:
            # Reuse the pooled engine keyed by these settings; the pool is
            # shared with the catalog adapter connected to the same database.
            self._engine = get_database_engine(database_settings)

    async def is_tag_defined(self, name):
        statement = select(orm.AccessTag.id).where(orm.AccessTag.name == name)
        async with self._engine.connect() as conn:
            found_tagname = (await conn.execute(statement)).first() is not None
        return found_tagname

    async def get_public_tags(self):
        statement = select(orm.AccessTag.name).where(orm.AccessTag.is_public)
        async with self._engine.connect() as conn:
            public_tags = set((await conn.execute(statement)).scalars())
        return public_tags

    async def get_defined_scopes(self):
        statement = select(orm.Scope.name)
        async with self._engine.connect() as conn:
            defined_scopes = set((await conn.execute(statement)).scalars())
        return defined_scopes

    async def get_scopes_from_tag(self, tagname, username):
        statement = (
            select(orm.Scope.name)
            .select_from(access_tag_principal_scopes_named)
            .where(
                orm.AccessTag.name == tagname,
                orm.AccessTagsPrincipal.name == username,
            )
        )
        async with self._engine.connect() as conn:
            user_tag_scopes = set((await conn.execute(statement)).scalars())
        return user_tag_scopes

    async def is_tag_owner(self, tagname, username):
        statement = (
            select(orm.AccessTagOwner.tag_id)
            .join(orm.AccessTag, orm.AccessTag.id == orm.AccessTagOwner.tag_id)
            .join(
                orm.AccessTagsPrincipal,
                orm.AccessTagsPrincipal.id == orm.AccessTagOwner.principal_id,
            )
            .where(
                orm.AccessTag.name == tagname,
                orm.AccessTagsPrincipal.name == username,
            )
        )
        async with self._engine.connect() as conn:
            found_owner = (await conn.execute(statement)).first() is not None
        return found_owner

    async def is_tag_public(self, name):
        statement = select(orm.AccessTag.id).where(
            orm.AccessTag.name == name, orm.AccessTag.is_public
        )
        async with self._engine.connect() as conn:
            found_public = (await conn.execute(statement)).first() is not None
        return found_public

    async def get_tags_from_scope(self, scope, username):
        statement = (
            select(orm.AccessTag.name)
            .select_from(access_tag_principal_scopes_named)
            .where(
                orm.Scope.name == scope,
                orm.AccessTagsPrincipal.name == username,
            )
        )
        async with self._engine.connect() as conn:
            user_scope_tags = set((await conn.execute(statement)).scalars())
        return user_scope_tags


# The tables that the access tags compiler writes. They belong to the catalog
# schema (normally created by catalog initialization and migrations), but the
# compiler ensures they exist so that it can also run against a catalog
# database that has not been initialized yet. The set is self-contained: no
# foreign key in it references a table outside of it, and catalog
# initialization tolerates their prior existence (create_all with checkfirst).
ACCESS_TAGS_TABLES = [
    orm.AccessTag.__table__,
    orm.AccessTagsPrincipal.__table__,
    orm.Scope.__table__,
    orm.AccessTagPrincipalScope.__table__,
    orm.AccessTagOwner.__table__,
]

# Association tables recording which tags are assigned to which data. Unlike
# the grant junctions above, their contents are not derived from the compiled
# tag definitions -- deleting a tag cascades to these rows, destroying state
# that a later compile cannot restore. The compiler never writes these tables;
# it only checks them before deleting a tag.
ASSIGNMENT_TABLES = [
    orm.NodeAccessTag.__table__,
    graph_orm.entity_access_tags,
    graph_orm.link_access_tags,
]

# PostgreSQL advisory lock key serializing concurrent compiles against the
# same database (e.g. a scheduled sync overlapping a manual run). The value
# is arbitrary but must be fixed and not collide with other advisory lock
# users of the catalog database; tiled uses no other advisory locks.
# 746 spells "tag" in digits.
ACCESS_TAGS_COMPILER_LOCK_KEY = 746


def _upsert(engine):
    "Return the dialect-specific INSERT ... ON CONFLICT construct."
    if engine.dialect.name == "postgresql":
        return postgresql_upsert
    return sqlite_upsert


async def create_access_tags_tables(engine):
    "Create the access tag tables and their indexes, if they do not exist."
    async with engine.begin() as connection:
        await connection.run_sync(
            orm.Base.metadata.create_all,
            tables=ACCESS_TAGS_TABLES,
            checkfirst=True,
        )


async def update_access_tags_tables(engine, scopes, tags, owners, public_tags):
    """
    Synchronize the access tag tables with the compiled tag state.

    Names are upserted, so existing rows -- and therefore their ids, which the
    node_access_tags association table references -- are preserved across
    recompilations. Definitions absent from the compiled state are deleted;
    deleting a definition cascades to the rows that reference it, including
    node_access_tags rows for a deleted tag. The whole update is a single
    transaction.
    """
    upsert = _upsert(engine)
    tags_table = orm.AccessTag.__table__
    users_table = orm.AccessTagsPrincipal.__table__
    scopes_table = orm.Scope.__table__
    tags_users_scopes_table = orm.AccessTagPrincipalScope.__table__
    tag_owners_table = orm.AccessTagOwner.__table__

    # stage all items in memory, deduplicated
    # (a name may appear in both tags and owners)
    all_tags = {name: (name in public_tags) for name in (*tags, *owners)}
    all_users = {user for users in tags.values() for user in users}
    all_users.update(user for users in owners.values() for user in users)
    all_scopes = set(scopes)

    async with engine.begin() as connection:
        if engine.dialect.name == "postgresql":
            # Serialize concurrent compiles. Transaction-scoped: the lock is
            # released automatically on commit or rollback, including when
            # the session dies. SQLite needs no equivalent; its writers are
            # serialized by database-level locking.
            await connection.execute(
                select(func.pg_advisory_xact_lock(ACCESS_TAGS_COMPILER_LOCK_KEY))
            )

        # push item names and metadata, preserving ids of existing names
        tags_statement = upsert(tags_table).values(
            [
                {"name": name, "is_public": is_public}
                for name, is_public in all_tags.items()
            ]
        )
        await connection.execute(
            tags_statement.on_conflict_do_update(
                index_elements=["name"],
                set_={"is_public": tags_statement.excluded.is_public},
            )
        )
        if all_users:
            await connection.execute(
                upsert(users_table)
                .values([{"name": name} for name in all_users])
                .on_conflict_do_nothing(index_elements=["name"])
            )
        if all_scopes:
            await connection.execute(
                upsert(scopes_table)
                .values([{"name": name} for name in all_scopes])
                .on_conflict_do_nothing(index_elements=["name"])
            )

        # Among tags dropped from the compiled state, retain any that are
        # still assigned to data: deleting them would cascade-destroy the
        # assignments, which -- unlike the grants, which are re-derived from
        # the compiled state on every run -- could not be restored by a later
        # compile. A retained tag keeps its id and assignments but is
        # stripped of all grants (its junction rows are pruned below) and
        # made non-public, so it confers no access. If a later compile
        # restores the name, the same row is reused and its grants resume;
        # once nothing is assigned to it, the next compile deletes it.
        stale_tags = {
            name: tag_id
            for tag_id, name in await connection.execute(
                select(tags_table.c.id, tags_table.c.name).where(
                    tags_table.c.name.not_in(list(all_tags))
                )
            )
        }
        retained_tag_ids = set()
        if stale_tags:
            stale_tag_ids = list(stale_tags.values())
            # The assignment tables may not exist yet (e.g. compiling into a
            # catalog database that has not been initialized); a table that
            # does not exist holds no assignments.
            existing_table_names = await connection.run_sync(
                lambda sync_connection: set(inspect(sync_connection).get_table_names())
            )
            for assignment_table in ASSIGNMENT_TABLES:
                if assignment_table.name not in existing_table_names:
                    continue
                retained_tag_ids.update(
                    (
                        await connection.execute(
                            select(assignment_table.c.tag_id.distinct()).where(
                                assignment_table.c.tag_id.in_(stale_tag_ids)
                            )
                        )
                    ).scalars()
                )
        if retained_tag_ids:
            retained_tag_names = sorted(
                name
                for name, tag_id in stale_tags.items()
                if tag_id in retained_tag_ids
            )
            warnings.warn(
                "These tags were dropped from the compiled tag definitions but "
                "are still assigned to data, so instead of being deleted they "
                f"are retained with all grants revoked: {retained_tag_names}",
                UserWarning,
            )
            await connection.execute(
                update(tags_table)
                .where(tags_table.c.id.in_(list(retained_tag_ids)))
                .values(is_public=False)
            )

        # delete outdated items; deletes cascade to the association tables
        deleted_tag_ids = [
            tag_id for tag_id in stale_tags.values() if tag_id not in retained_tag_ids
        ]
        if deleted_tag_ids:
            await connection.execute(
                delete(tags_table).where(tags_table.c.id.in_(deleted_tag_ids))
            )
        await connection.execute(
            delete(users_table).where(users_table.c.name.not_in(list(all_users)))
        )
        await connection.execute(
            delete(scopes_table).where(scopes_table.c.name.not_in(list(all_scopes)))
        )

        # load db IDs for items into memory
        tags_to_id = {
            intern(name): tag_id
            for tag_id, name in await connection.execute(
                select(tags_table.c.id, tags_table.c.name)
            )
        }
        users_to_id = {
            intern(name): user_id
            for user_id, name in await connection.execute(
                select(users_table.c.id, users_table.c.name)
            )
        }
        scopes_to_id = {
            # name is a ScopeName enum member; key by its interned string
            # value (not str(member), which is e.g. "ScopeName.read_data")
            intern(name.value): scope_id
            for scope_id, name in await connection.execute(
                select(scopes_table.c.id, scopes_table.c.name)
            )
        }

        # flatten relationships and diff against current table contents
        tags_users_scopes = {
            (tags_to_id[tag], users_to_id[user], scopes_to_id[scope])
            for tag, users in tags.items()
            for user, user_scopes in users.items()
            for scope in user_scopes
        }
        tag_owners = {
            (tags_to_id[tag], users_to_id[user])
            for tag, users in owners.items()
            for user in users
        }
        existing_tags_users_scopes = {
            tuple(row)
            for row in await connection.execute(
                select(
                    tags_users_scopes_table.c.tag_id,
                    tags_users_scopes_table.c.principal_id,
                    tags_users_scopes_table.c.scope_id,
                )
            )
        }
        existing_tag_owners = {
            tuple(row)
            for row in await connection.execute(
                select(tag_owners_table.c.tag_id, tag_owners_table.c.principal_id)
            )
        }

        # add updated relationships and delete outdated ones
        new_tags_users_scopes = tags_users_scopes - existing_tags_users_scopes
        if new_tags_users_scopes:
            await connection.execute(
                insert(tags_users_scopes_table),
                [
                    {"tag_id": tag_id, "principal_id": user_id, "scope_id": scope_id}
                    for tag_id, user_id, scope_id in new_tags_users_scopes
                ],
            )
        stale_tags_users_scopes = existing_tags_users_scopes - tags_users_scopes
        if stale_tags_users_scopes:
            await connection.execute(
                delete(tags_users_scopes_table).where(
                    tuple_(
                        tags_users_scopes_table.c.tag_id,
                        tags_users_scopes_table.c.principal_id,
                        tags_users_scopes_table.c.scope_id,
                    ).in_(list(stale_tags_users_scopes))
                )
            )
        new_tag_owners = tag_owners - existing_tag_owners
        if new_tag_owners:
            await connection.execute(
                insert(tag_owners_table),
                [
                    {"tag_id": tag_id, "principal_id": user_id}
                    for tag_id, user_id in new_tag_owners
                ],
            )
        stale_tag_owners = existing_tag_owners - tag_owners
        if stale_tag_owners:
            await connection.execute(
                delete(tag_owners_table).where(
                    tuple_(
                        tag_owners_table.c.tag_id, tag_owners_table.c.principal_id
                    ).in_(list(stale_tag_owners))
                )
            )


class AccessTagsCompiler:
    _MAX_TAG_NESTING = 5

    def __init__(
        self,
        scopes,
        tag_config,
        database_settings,
        group_parser,
        *,
        authn_database_settings=None,
        provider=None,
    ):
        self.scopes = scopes or {}
        self.tag_config = tag_config
        # Reuse the pooled engine keyed by these settings; when the compiler
        # runs inside a tiled server, the pool is shared with the catalog
        # adapter connected to the same database.
        self._engine = get_database_engine(database_settings)
        self._tables_created = False
        self.group_parser = group_parser
        # Optionally, load_principal_tags() reads principals from the
        # authentication database and defines a principal tag ('user:...'
        # or 'service:...') for each
        if (authn_database_settings is None) != (provider is None):
            raise ValueError(
                "authn_database_settings and provider must be given together: "
                "principal tags are generated from the identities associated "
                "with the given provider."
            )
        self._authn_engine = (
            get_database_engine(authn_database_settings)
            if authn_database_settings is not None
            else None
        )
        self.provider = provider

        self.max_tag_nesting = max(self._MAX_TAG_NESTING, 0)
        self.public_tag = intern("public".casefold())
        self.invalid_tag_names = [name.casefold() for name in []]

        self.roles = {}
        self.tags = {}
        self.tag_owners = {}
        self.compiled_tags = {self.public_tag: {}}
        self.compiled_public = set({self.public_tag})
        self.compiled_tag_owners = {}

    def load_tag_config(self):
        if isinstance(self.tag_config, str) or isinstance(self.tag_config, Path):
            try:
                with open(Path(self.tag_config)) as tag_config_file:
                    tag_definitions = yaml.load(tag_config_file, Loader=InterningLoader)
                    self.roles.update(tag_definitions.get("roles", {}))
                    self.tags.update(tag_definitions["tags"])
                    self.tag_owners.update(tag_definitions.get("tag_owners", {}))
            except FileNotFoundError as e:
                raise ValueError(
                    f"The tag config file {self.tag_config!s} doesn't exist."
                ) from e
        elif isinstance(self.tag_config, dict):
            tag_definitions = self.tag_config
            self.roles.update(tag_definitions.get("roles", {}))
            self.tags.update(tag_definitions["tags"])
            self.tag_owners.update(tag_definitions.get("tag_owners", {}))

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

    async def load_principal_tags(self):
        """
        Load a principal tag definition for every principal in the
        authentication database.

        User principals are identified by their identity id from the
        configured provider (tag 'user:<id>'); service principals by their
        uuid (tags 'service:<uuid>' and 'user:<uuid>', since either literal
        may be used to refer to them).

        If the authentication database is missing, uninitialized, or
        unreachable, warn and load nothing; the compilation proceeds with
        the config-defined tags.
        """
        if self._authn_engine is None:
            raise RuntimeError(
                "Principal tags cannot be loaded: the compiler was "
                "constructed without authn_database_settings and provider."
            )

        # Connecting to a nonexistent SQLite database would create it as an
        # empty file, which the server then refuses to initialize at startup.
        # The compiler must only ever read this database, so check for the
        # file first instead of connecting.
        url = self._authn_engine.url
        if url.get_backend_name() == "sqlite":
            database = url.database
            if (
                database
                and database != ":memory:"
                and "mode=memory" not in database
                and not Path(database).exists()
            ):
                warnings.warn(
                    "The authentication database is not initialized yet; "
                    "no principal tags were generated.",
                    UserWarning,
                )
                return

        association = authn_orm.principal_role_association_table
        principals_with_roles = authn_orm.Principal.__table__.join(
            association, association.c.principal_id == authn_orm.Principal.id
        )
        try:
            async with self._authn_engine.connect() as conn:
                if not await conn.run_sync(
                    lambda sync_conn: inspect(sync_conn).has_table(
                        authn_orm.Principal.__tablename__
                    )
                ):
                    warnings.warn(
                        "The authentication database is not initialized yet; "
                        "no principal tags were generated.",
                        UserWarning,
                    )
                    return
                # Read the roles once; the scopes granted through a role are
                # the intersection of the role's scopes with the compiler's.
                granted_scopes_of_role = {
                    role_id: set(role_scopes) & set(self.scopes)
                    for role_id, role_scopes in await conn.execute(
                        select(authn_orm.Role.id, authn_orm.Role.scopes)
                    )
                }
                user_rows = list(
                    await conn.execute(
                        select(authn_orm.Identity.id, association.c.role_id)
                        .select_from(
                            principals_with_roles.join(
                                authn_orm.Identity,
                                authn_orm.Identity.principal_id
                                == authn_orm.Principal.id,
                            )
                        )
                        .where(
                            authn_orm.Principal.type == PrincipalType.user,
                            authn_orm.Identity.provider == self.provider,
                        )
                    )
                )
                service_rows = list(
                    await conn.execute(
                        select(authn_orm.Principal.uuid, association.c.role_id)
                        .select_from(principals_with_roles)
                        .where(authn_orm.Principal.type == PrincipalType.service)
                    )
                )
        except Exception as exc:
            # Not just DBAPIError: connection failures from the async drivers
            # (e.g. asyncpg) can propagate unwrapped, and no failure to read
            # the authentication database should abort the compilation.
            warnings.warn(
                f"The authentication database could not be read "
                f"({exc.__class__.__name__}: {exc}); no principal tags were "
                f"generated. Compilation proceeds with the config-defined "
                f"tags only. Previously generated principal tags will be "
                f"treated as stale until a compilation can read the "
                f"authentication database again.",
                UserWarning,
            )
            return

        # A principal may hold several roles (several rows); union the scopes
        # its roles grant. A user is named by its provider identity id, a
        # service by its uuid; a service may be referred to by either literal.
        tag_scopes = {}
        for identifier, role_id in user_rows:
            tag_scopes.setdefault(f"user:{identifier}", set()).update(
                granted_scopes_of_role[role_id]
            )
        for identifier, role_id in service_rows:
            granted = granted_scopes_of_role[role_id]
            tag_scopes.setdefault(f"service:{identifier}", set()).update(granted)
            tag_scopes.setdefault(f"user:{identifier}", set()).update(granted)

        for tag_name, granted_scopes in tag_scopes.items():
            # Merge with (do not overwrite) a definition that the tag config
            # may provide for this tag; compilation unions the grants. A
            # principal whose roles grant nothing gets a definition with no
            # 'users' entry: _dfs rejects a user entry with empty scopes, and
            # an empty definition compiles to an empty grant set.
            identifier = tag_name.partition(":")[2]
            definition = self.tags.setdefault(intern(tag_name), {})
            if granted_scopes:
                definition.setdefault("users", []).append(
                    {"name": identifier, "scopes": granted_scopes}
                )

    async def compile(self):
        if not self._tables_created:
            await create_access_tags_tables(self._engine)
            self._tables_created = True

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

        for tag in self.tag_owners:
            if tag.casefold().startswith(PRINCIPAL_TAG_PREFIXES):
                raise ValueError(
                    f"Tag '{tag}' uses a principal-tag prefix "
                    f"{PRINCIPAL_TAG_PREFIXES}.\n"
                    f"Principal tags cannot have owners: they are never "
                    f"applied to nodes manually, only by the access policy."
                )

        adjacent_tags = {}
        for tag, members in self.tags.items():
            if tag.casefold() == self.public_tag:
                raise ValueError(
                    f"'Public' tag '{self.public_tag}' cannot be redefined."
                )
            if tag.casefold() in self.invalid_tag_names:
                raise ValueError(
                    f"Tag 'tag' is an invalid tag name.\n"
                    f"The invalid tag names are: {self.invalid_tag_names}"
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

        await update_access_tags_tables(
            self._engine,
            self.scopes,
            self.compiled_tags,
            self.compiled_tag_owners,
            self.compiled_public,
        )

    def clear_raw_tags(self):
        self.roles = {}
        self.tags = {}
        self.tag_owners = {}

    async def recompile(self):
        self.compiled_tags = {self.public_tag: {}}
        self.compiled_public = set({self.public_tag})
        self.compiled_tag_owners = {}
        await self.compile()
