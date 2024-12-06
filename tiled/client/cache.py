import enum
import httpcore
import os
import platformdirs
import sqlite3
import typing as tp
import sys

from datetime import datetime
from hishel import BaseStorage, BaseSerializer
from hishel._sync._storages import StorageResponse, RemoveTypes
from hishel._serializers import Metadata
from pathlib import Path

from .utils import SerializableLock


CACHE_DATABASE_SCHEMA_VERSION = 2

# This is currently only used for checking SQlite thread-safety
PY311 = sys.version_info >= (3, 11)


def with_safe_threading(fn):
    """
    Ensure thread-safe SQLite access

    If we can check that the underlying SQLite module
    is built with thread-safety, then no need to lock.

    If we cannot check or the check is false, use a lock
    to ensure the databsae isn't accessed concurrently.
    """

    @wraps(fn)
    def wrapper(obj, *args, **kwargs):
        sqlite_is_safe = sqlite3.threadsafety == ThreadingMode.SERIALIZED
        lock_is_mine = False

        if not (PY311 and sqlite_is_safe):
            lock_is_mine = obj._lock.acquire()
        try:
            result = fn(obj, *args, **kwargs)
        finally:
            if lock_is_mine and obj._lock.locked():
                obj._lock.release()
        return result

    return wrapper


class ThreadingMode(enum.IntEnum):
    """
    Threading mode used in the sqlite3 package.

    https://docs.python.org/3/library/sqlite3.html#sqlite3.threadsafety
    """

    SINGLE_THREAD = 0
    MULTI_THREAD = 1
    SERIALIZED = 3


class Cache(BaseStorage):
    def __init__(
        self,
        serializer: tp.Optional[BaseSerializer] = None,
        connection: tp.Optional[sqlite3.Connection] = None,
        ttl: tp.Optional[tp.Union[int, float]] = None,
        filepath=None,
        capacity=500_000_000,
        max_item_size=500_000,
        readonly=False,
    ) -> None:
        super().__init__(serializer, ttl)
        self._connection: tp.Optional[sqlite3.Connection] = connection or None
        self._setup_completed: bool = False
        self._lock = SerializableLock()

        if filepath is None:
            # Resolve this here, not at module scope, because the test suite
            # injects TILED_CACHE_DIR env var to use a temporary directory.
            TILED_CACHE_DIR = Path(
                os.getenv("TILED_CACHE_DIR", platformdirs.user_cache_dir("tiled"))
            )
            # TODO Consider defaulting to a temporary database, with a warning,
            # if TILED_CACHE_DIR points to a networked filesystem. Unless perhaps
            # flock() support can be checked (nfs version, or lock manager, etc).
            filepath = TILED_CACHE_DIR / "http_response_cache.db"
        self._filepath = filepath
        self._capacity = None
        self.capacity = capacity
        self._max_item_size = None
        self.max_item_size = max_item_size
        self._readonly = readonly

    @with_safe_threading
    def _setup(self) -> None:
        if not self._setup_completed:
            if not self._connection:
                # The methods in the Cache storage object will not try to write when
                # in readonly mode. For extra safety, we open a readonly connection
                # to the database, so that SQLite itself will prohibit writing.
                database = f"file:{filepath}?ro" if self._readonly else filepath
                self._connection = sqlite3.connect(
                    database, uri=self._readonly, check_same_thread=False
                )
            cursor = self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            )
            tables = [row[0] for row in cursor.fetchall()]
            if not tables:
                # We have an empty database
                self._create_tables()
            elif "tiled_http_response_cache_version" not in tables:
                # We have a non-empty database that we do not recognize.
                raise RunetimeError(
                    f"Database at {filepath} is not empty and is not recognized as a Tiled HTTP response cache."
                )
            else:
                # We have a non-empty database that we recognize.
                cursor = self._connection.execute(
                    "SELECT * FROM tiled_http_response_cache_version;"
                )
                (version,) = cursor.fetchone()
                if version != CACHE_DATABASE_SCHEMA_VERSION:
                    # It is likely that this cache database will be very stable,
                    # but if we must make changes we will not bother with migrations.
                    # The cache is highly disposable. Just silently blow it away and start over.
                    Path(filepath).unlink()
                    self._connection = sqlite3.connect(
                        filepath, check_same_thread=False
                    )
                    self._create_tables()
            self._setup_completed = True

    def _create_tables(self) -> None:
        with closing(self._connection.cursor()) as cursor:
            cursor.execute(
                """CREATE TABLE responses (
cache_key TEXT PRIMARY KEY,
status_code INTEGER,
headers JSON,
body BLOB,
is_stream INTEGER,
encoding TEXT,
size INTEGER,
request JSON,
time_created REAL,
time_last_accessed REAL
)"""
            )
            cursor.execute(
                "CREATE TABLE tiled_http_response_cache_version (version INTEGER)"
            )
            cursor.execute(
                "INSERT INTO tiled_http_response_cache_version (version) VALUES (?)",
                (CACHE_DATABASE_SCHEMA_VERSION,),
            )
            self._connection.commit()

    def __repr__(self):
        module = type(self).__module__
        qualname = type(self).__qualname__
        memaddress = hex(id(self))
        dbfile = str(self.filepath)
        return f"<{module}.{qualname} object at {memaddress} using database {dbfile!r}>"

    def __getstate__(self):
        return (
            self._setup_completed,
            self._lock,
            self._filepath,
            self._capacity,
            self._max_item_size,
            self._readonly,
        )

    def __setstate__(self, state):
        (setup_completed, lock, filepath, capacity, max_item_size, readonly) = state
        self._lock = lock
        self._filepath = filepath
        self._capacity = capacity
        self._max_item_size = max_item_size
        self._readonly = readonly
        if setup_completed:
            self._setup()

    @property
    def filepath(self):
        "Filepath of the SQLite database used for storing cache data"
        return self._filepath

    @property
    def capacity(self):
        "Max capacity of the cache, in bytes. Includes the response AND request bodies."
        return self._capacity

    @capacity.setter
    def capacity(self, capacity):
        if capacity < 1:
            raise ValueError("Cache capacity cannot be less than 1 byte")
        elif self.max_item_size and capacity < self.max_item_size:
            raise ValueError("Cache capacity cannot be less than allowed item size")
        self._capacity = capacity

    @property
    def max_item_size(self):
        """
        Max size of a response body that can be accepted into the cache.
        The size of the request body will be included against this limit.
        """
        return self._max_item_size

    @max_item_size.setter
    def max_item_size(self, max_item_size):
        if max_item_size < 1:
            raise ValueError("Cached items cannot be less than 1 byte")
        elif max_item_size > self.capacity:
            raise ValueError("Cached items cannot be greater than cache capacity")
        self._max_item_size = max_item_size

    @property
    def readonly(self):
        "If readonly, cache can be read but not updated."
        return self._readonly

    def size(self):
        """
        Size of response bodies in cache in bytes.
        Includes the size of the corresponding request bodies.
        Does not include the size of headers and other auxiliary info.
        """
        with closing(self._connection.cursor()) as cursor:
            (total_size,) = cursor.execute("SELECT SUM(size) FROM responses").fetchone()
        return total_size or 0  # if empty, total_size is None

    def count(self):
        "Number of responses cached."
        with closing(self._connection.cursor()) as cursor:
            (count,) = cursor.execute("SELECT COUNT(*) FROM responses").fetchone()
        return count or 0  # if empty, count is None

    def close(self) -> None:
        "Close the cache."
        if self._connection is not None:
            self._connection.close()
