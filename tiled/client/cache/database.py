import sqlite3
from contextlib import closing
from pathlib import Path

from .. import context


VERSION = 1


def default_filepath(api_uri):
    # TO DO: If TILED_CACHE_DIR is on NFS, use a sqlite temporary database instead.
    return Path(
        context.TILED_CACHE_DIR, "http_response_cache", urllib.parse.quote_plus(str(api_uri))
    )


class Cache:
    def __init__(self, filepath, total_capacity=500_000_000, max_item_size=500_000, readonly=False):
        if readonly:
            raise NotImplementedError("readonly cache is planned but not yet implemented")
        self.total_capacity = total_capacity
        self.max_item_size = max_item_size
        self._readonly = readonly
        self._filepath = filepath
        self._db = sqlite3.connect(filepath)
        cursor = self._db.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        if not tables:
            # We have an empty database.
            self._create_tables()
        elif "tiled_http_response_cache_version" not in tables:
            # We have a nonempty database that we do not recognize.
            print(tables)
            raise RuntimeError(f"Database at {filepath} is not empty and not recognized as a tiled HTTP response cache.")
        else:
            # We have a nonempty database that we recognize.
            cursor = self._db.execute("SELECT * FROM tiled_http_response_cache_version;")
            (version,) = cursor.fetchone()
            if version != VERSION:
                # It is likely that this cache database will be very stable; we
                # *may* never need to change the schema. But if we do, we will
                # not bother with migrations. The cache is highly disposable.
                # Just silently blow it away and start over.
                Path(filepath).unlink()
                self._db = sqlite3.connect(filepath)
                self._create_tables()


    def _create_tables(self):
        with closing(self._db.cursor()) as cur:
            cur.execute("""CREATE TABLE responses (
cache_key TEXT,
headers JSON,
body BLOB,
size INTEGER,
time_created TEXT,
time_last_accessed TEXT
)""")
            cur.execute("CREATE TABLE tiled_http_response_cache_version (version INTEGER)")
            cur.execute("INSERT INTO tiled_http_response_cache_version (version) VALUES (?)", (VERSION,))
            self._db.commit()

    @property
    def readonly(self):
        return self._readonly

    @property
    def filepath(self):
        return self._filepath
