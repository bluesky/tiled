import sqlite3
import warnings
from contextlib import closing
from pathlib import Path
from sys import intern

import aiosqlite
import yaml

from ..utils import InterningLoader, ensure_specified_sql_driver


class AccessTagsParser:
    @classmethod
    def from_uri(cls, uri):
        if uri.startswith("file:"):
            uri = uri.split(":", 1)[1]
        uri = ensure_specified_sql_driver(uri)
        if not uri.startswith("sqlite+aiosqlite:"):
            raise ValueError(
                f"AccessTagsParser must be given a SQLite database URI, "
                f"i.e. 'sqlite:///...', 'sqlite+aiosqlite:///...'\n"
                f"Given URI results in: {uri=}"
            )
        uri_path = uri.split(":", 1)[1]
        if not uri_path.startswith("///"):
            raise ValueError(
                "Invalid URI provided, URI must contain 3 forward slashes, "
                "e.g. 'sqlite:///...'."
            )
        uri = f"file:{uri_path[3:]}"
        uri = uri if "?" in uri else f"{uri}?mode=ro"
        return cls(uri=uri)

    def __init__(self, db=None, uri=None):
        self._uri = uri
        self._db = db

    async def connect(self):
        if self._db is None:
            self._db = await aiosqlite.connect(
                self._uri, uri=True, check_same_thread=False
            )

    async def is_tag_defined(self, name):
        async with self._db.cursor() as cursor:
            await cursor.execute("SELECT 1 FROM tags WHERE name = ?;", (name,))
            row = await cursor.fetchone()
            found_tagname = bool(row)
        return found_tagname

    async def get_public_tags(self):
        async with self._db.cursor() as cursor:
            await cursor.execute("SELECT name FROM public_tags;")
            public_tags = {name for (name,) in await cursor.fetchall()}
        return public_tags

    async def get_scopes_from_tag(self, tagname, username):
        async with self._db.cursor() as cursor:
            await cursor.execute(
                "SELECT scope_name FROM user_tag_scopes WHERE tag_name = ? AND user_name = ?;",
                (tagname, username),
            )
            user_tag_scopes = {scope for (scope,) in await cursor.fetchall()}
        return user_tag_scopes

    async def is_tag_owner(self, tagname, username):
        async with self._db.cursor() as cursor:
            await cursor.execute(
                "SELECT 1 FROM user_tag_owners WHERE tag_name = ? AND user_name = ?;",
                (tagname, username),
            )
            row = await cursor.fetchone()
            found_owner = bool(row)
        return found_owner

    async def is_tag_public(self, name):
        async with self._db.cursor() as cursor:
            await cursor.execute("SELECT 1 FROM public_tags WHERE name = ?;", (name,))
            row = await cursor.fetchone()
            found_public = bool(row)
        return found_public

    async def get_tags_from_scope(self, scope, username):
        async with self._db.cursor() as cursor:
            await cursor.execute(
                "SELECT tag_name FROM user_tag_scopes WHERE user_name = ? AND scope_name = ?;",
                (username, scope),
            )
            user_scope_tags = {tag for (tag,) in await cursor.fetchall()}
        return user_scope_tags


def create_access_tags_tables(db):
    with closing(db.cursor()) as cursor:
        tables_setup_sql = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA foreign_keys = ON;
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS tags (
  id        INTEGER PRIMARY KEY,
  name      TEXT    UNIQUE NOT NULL,
  is_public INTEGER NOT NULL DEFAULT 0
    CHECK (is_public IN (0,1))
);
CREATE TABLE IF NOT EXISTS users (
  id   INTEGER PRIMARY KEY,
  name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS scopes (
  id   INTEGER PRIMARY KEY,
  name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS tags_users_scopes (
  tag_id    INTEGER NOT NULL
    REFERENCES tags(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  user_id   INTEGER NOT NULL
    REFERENCES users(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  scope_id  INTEGER NOT NULL
    REFERENCES scopes(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  PRIMARY KEY (tag_id, user_id, scope_id)
);
CREATE TABLE IF NOT EXISTS tag_owners (
  tag_id   INTEGER NOT NULL
    REFERENCES tags(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  user_id  INTEGER NOT NULL
    REFERENCES users(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  PRIMARY KEY (tag_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_tags_is_public ON tags (is_public);
CREATE INDEX IF NOT EXISTS idx_tus_users_scopes ON tags_users_scopes (user_id, scope_id);
CREATE INDEX IF NOT EXISTS idx_tus_users_scopes_scopeid ON tags_users_scopes (scope_id);
CREATE INDEX IF NOT EXISTS idx_tag_owners ON tag_owners (user_id);
CREATE VIEW IF NOT EXISTS public_tags AS
  SELECT name
  FROM tags
  WHERE is_public = 1;
CREATE VIEW IF NOT EXISTS user_tag_scopes AS
  SELECT
    u.name AS user_name,
    t.name AS tag_name,
    s.name AS scope_name
  FROM tags_users_scopes tus
    JOIN users  u ON u.id = tus.user_id
    JOIN tags   t ON t.id = tus.tag_id
    JOIN scopes s ON s.id = tus.scope_id;
CREATE VIEW IF NOT EXISTS user_tag_owners AS
  SELECT
    u.name AS user_name,
    t.name AS tag_name
  FROM tag_owners towner
    JOIN users u ON u.id = towner.user_id
    JOIN tags  t ON t.id = towner.tag_id;
PRAGMA optimize;
"""
        cursor.executescript(tables_setup_sql)
        db.commit()


def update_access_tags_tables(db, scopes, tags, owners, public_tags):
    with closing(db.cursor()) as cursor:
        tables_stage_sql = """
BEGIN TRANSACTION;
CREATE TEMP TABLE IF NOT EXISTS stage_tags (
  id        INTEGER,
  name      TEXT    NOT NULL,
  is_public INTEGER NOT NULL
    CHECK (is_public IN (0,1))
);
CREATE TEMP TABLE IF NOT EXISTS stage_users (
  id   INTEGER,
  name TEXT NOT NULL
);
CREATE TEMP TABLE IF NOT EXISTS stage_scopes (
  id   INTEGER,
  name TEXT NOT NULL
);
CREATE TEMP TABLE IF NOT EXISTS stage_tags_users_scopes (
  tag_id   INTEGER NOT NULL,
  user_id  INTEGER NOT NULL,
  scope_id INTEGER NOT NULL
);
CREATE TEMP TABLE IF NOT EXISTS stage_tag_owners (
  tag_id  INTEGER NOT NULL,
  user_id INTEGER NOT NULL
);
"""
        cursor.executescript(tables_stage_sql)

        # put all items into staging
        all_tags = [(tag, 0) for tag in tags] + [(tag, 0) for tag in owners]
        all_public = [(tag,) for tag in public_tags]
        all_users = {(user,) for users in tags.values() for user in users}
        all_users.update({(user,) for users in owners.values() for user in users})
        all_scopes = [(scope,) for scope in scopes]
        cursor.executemany(
            "INSERT INTO stage_tags(name, is_public) VALUES (?,?);",
            all_tags,
        )
        cursor.executemany(
            "UPDATE stage_tags SET is_public = 1 WHERE name = (?);", all_public
        )
        cursor.executemany(
            "INSERT INTO stage_users(name) VALUES (?);",
            all_users,
        )
        cursor.executemany(
            "INSERT INTO stage_scopes(name) VALUES (?);",
            all_scopes,
        )

        # push item names and metadata from staging to prod
        # then pull back ID values from prod to staging
        # note that UPSERT should always have a WHERE clause
        #   to avoid ambiguity. See the SQLite docs section 2.2
        #   https://www.sqlite.org/lang_upsert.html
        stage_push_sql = """
BEGIN TRANSACTION;
INSERT INTO tags (name, is_public)
  SELECT name, is_public
  FROM stage_tags
  WHERE true
  ON CONFLICT (name)
  DO UPDATE SET is_public = excluded.is_public;
INSERT INTO users(name) SELECT name FROM stage_users WHERE true ON CONFLICT(name) DO NOTHING;
INSERT INTO scopes(name) SELECT name FROM stage_scopes WHERE true ON CONFLICT(name) DO NOTHING;
UPDATE stage_tags SET id = (SELECT id FROM tags WHERE tags.name = stage_tags.name);
UPDATE stage_users SET id = (SELECT id FROM users WHERE users.name = stage_users.name);
UPDATE stage_scopes SET id = (SELECT id FROM scopes WHERE scopes.name = stage_scopes.name);
"""
        cursor.executescript(stage_push_sql)

        # load db IDs for items into memory
        cursor.execute("SELECT id, name FROM stage_tags")
        tags_to_id = {intern(name): tag_id for (tag_id, name) in cursor.fetchall()}
        cursor.execute("SELECT id, name FROM stage_users")
        users_to_id = {intern(name): user_id for (user_id, name) in cursor.fetchall()}
        cursor.execute("SELECT id, name FROM stage_scopes")
        scopes_to_id = {
            intern(name): scope_id for (scope_id, name) in cursor.fetchall()
        }

        # flatten relationships and push to staging
        tags_users_scopes = [
            (tags_to_id[tag], users_to_id[user], scopes_to_id[scope])
            for tag, users in tags.items()
            for user, scopes in users.items()
            for scope in scopes
        ]
        tag_owners = [
            (tags_to_id[tag], users_to_id[user])
            for tag, users in owners.items()
            for user in users
        ]
        cursor.executemany(
            "INSERT INTO stage_tags_users_scopes(tag_id, user_id, scope_id) VALUES (?,?,?);",
            tags_users_scopes,
        )
        cursor.executemany(
            "INSERT INTO stage_tag_owners(tag_id, user_id) VALUES (?,?);", tag_owners
        )

        # delete outdated tags from prod and add updated relationships to db
        # finally, drop the staging tables
        # to-do: consider refactoring this to indvidual execute statements
        #        to avoid implicit pre-mature commit by executescript()
        upsert_delete_sql = """
BEGIN TRANSACTION;
DELETE from tags WHERE id NOT in (SELECT id FROM stage_tags);
DELETE from users WHERE id NOT in (SELECT id FROM stage_users);
DELETE from scopes WHERE id NOT in (SELECT id FROM stage_scopes);
INSERT INTO tags_users_scopes (tag_id, user_id, scope_id)
  SELECT tag_id, user_id, scope_id FROM stage_tags_users_scopes
  WHERE true
  ON CONFLICT (tag_id, user_id, scope_id) DO NOTHING;
DELETE FROM tags_users_scopes
  WHERE (tag_id, user_id, scope_id)
  NOT IN (SELECT tag_id, user_id, scope_id FROM stage_tags_users_scopes);
INSERT INTO tag_owners (tag_id, user_id)
  SELECT tag_id, user_id FROM stage_tag_owners
  WHERE true
  ON CONFLICT (tag_id, user_id) DO NOTHING;
DELETE FROM tag_owners
  WHERE (tag_id, user_id) NOT IN (SELECT tag_id, user_id FROM stage_tag_owners);
DROP TABLE IF EXISTS stage_tags;
DROP TABLE IF EXISTS stage_users;
DROP TABLE IF EXISTS stage_scopes;
DROP TABLE IF EXISTS stage_tags_users_scopes;
DROP TABLE IF EXISTS stage_tag_owners;
PRAGMA optimize;
"""
        cursor.executescript(upsert_delete_sql)
        db.commit()


class AccessTagsCompiler:
    _MAX_TAG_NESTING = 5

    def __init__(
        self,
        scopes,
        tag_config,
        tags_db,
        group_parser,
    ):
        self.scopes = scopes or {}
        self.tag_config = tag_config
        self.connection = sqlite3.connect(
            tags_db["uri"], uri=True, check_same_thread=False
        )
        self.group_parser = group_parser

        self.max_tag_nesting = max(self._MAX_TAG_NESTING, 0)
        self.public_tag = intern("public".casefold())
        self.invalid_tag_names = [name.casefold() for name in []]

        self.roles = {}
        self.tags = {}
        self.tag_owners = {}
        self.compiled_tags = {self.public_tag: {}}
        self.compiled_public = set({self.public_tag})
        self.compiled_tag_owners = {}

        create_access_tags_tables(self.connection)

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

        update_access_tags_tables(
            self.connection,
            self.scopes,
            self.compiled_tags,
            self.compiled_tag_owners,
            self.compiled_public,
        )

    def clear_raw_tags(self):
        self.roles = {}
        self.tags = {}
        self.tag_owners = {}

    def recompile(self):
        self.compiled_tags = {self.public_tag: {}}
        self.compiled_public = set({self.public_tag})
        self.compiled_tag_owners = {}
        self.compile()
