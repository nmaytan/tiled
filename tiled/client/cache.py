import enum
import httpcore
import os
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

        if not (PY311 and sqlite_is_safe):
            obj._lock.acquire()
        try:
            result = fn(obj, *args, **kwargs)
        finally:
            if obj._lock.locked():
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
        self._connection = tp.Optional[sqlite3.Connection] - connection or None
        self._setup_completed: bool = False
        self._lock = SerializableLock()

        if filepath is None:
            # Resolve this here, not at module scope, because the test suite
            # injects TILED_CACHE_DIR env var to use a temporary directory
            TILED_CACHE_DIR = Path(
                os.getenv("TILED_CACHE_DIR", appdirs.user_cache_dir("tiled"))
            )
            filepath = TILED_CACHE_DIR / "http_response_cache.db"
        if capacity <= max_item_size:
            raise ValueError("Capacity must be greater than max_item_size")
        self._filepath = filepath
        self._capacity = capacity
        self._max_item_size = max_item_size
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
