import enum
import os
import sqlite3
import sys
import threading
import typing as tp
import uuid
import warnings
from contextlib import closing
from datetime import datetime
from functools import wraps
from pathlib import Path

import platformdirs
from hishel import Entry, EntryMeta, SyncSqliteStorage
from httpcore import Request, Response

from tiled.utils import is_networked_filesystem

from .logger import logger

CACHE_DATABASE_SCHEMA_VERSION = 2

# This is currently only used for checking SQlite thread-safety
PY311 = sys.version_info >= (3, 11)


def with_thread_lock(fn):
    """Makes sure the wrapper isn't accessed concurrently."""

    @wraps(fn)
    def wrapper(obj, *args, **kwargs):
        obj._lock.acquire()
        try:
            result = fn(obj, *args, **kwargs)
        finally:
            obj._lock.release()
        return result

    return wrapper


class ThreadingMode(enum.IntEnum):
    """Threading mode used in the sqlite3 package.

    https://docs.python.org/3/library/sqlite3.html#sqlite3.threadsafety

    """

    SINGLE_THREAD = 0
    MULTI_THREAD = 1
    SERIALIZED = 3


def measure_entry_size(request, response):
    # httpcore exception that is == httpx.ResponseNotRead()
    # Trace out the way this works for a streaming response
    # Also handle streaming request
    size = 0

    if hasattr(response, "headers") and "content-length" in response.headers:
        size += int(response.headers["content-length"])

    if hasattr(request, "headers") and "content-length" in request.headers:
        size += int(request.headers["content-length"])

    return size


class Cache(SyncSqliteStorage):
    def __init__(
        self,
        *,
        connection: tp.Optional[sqlite3.Connection] = None,
        default_ttl: tp.Optional[tp.Union[int, float]] = None,
        filepath=None,
        capacity=500_000_000,
        max_item_size=500_000,
        readonly=False,
    ) -> None:
        # default_ttl is in seconds, capacity and max_item_size are in bytes

        self._setup_completed: bool = False

        TILED_CLIENT_CACHE_AVOID_UNSAFE_FILESYSTEM = os.getenv(
            "TILED_CLIENT_CACHE_AVOID_UNSAFE_FILESYSTEM", "true"
        )

        if filepath is None:
            # Resolve this here, not at module scope, because the test suite
            # injects TILED_CACHE_DIR env var to use a temporary directory.
            TILED_CACHE_DIR = Path(
                os.getenv("TILED_CACHE_DIR", platformdirs.user_cache_dir("tiled"))
            )

            # Defaults to a temporary, in memory, database with a warning when
            # TILED_CACHE_DIR points to a networked filesystem.
            if (
                TILED_CLIENT_CACHE_AVOID_UNSAFE_FILESYSTEM.lower() != "false"
                and is_networked_filesystem(TILED_CACHE_DIR)
            ):
                warnings.warn(
                    "The Tiled cache directory points to a networked filesystem. "
                    "Defaulting to a temporary database."
                )
                filepath = ":memory:"

            else:
                filepath = TILED_CACHE_DIR / "http_response_cache.db"
        else:
            if (
                TILED_CLIENT_CACHE_AVOID_UNSAFE_FILESYSTEM.lower() != "false"
                and is_networked_filesystem(filepath)
            ):
                warnings.warn(
                    "The provided filepath points to a networked filesystem. "
                    "Defaulting to a temporary database."
                )
                filepath = ":memory:"

        self._filepath = filepath
        self._capacity = None
        self._max_item_size = None
        self.capacity = capacity
        self.max_item_size = max_item_size
        self._readonly = readonly
        self._owner_thread = threading.current_thread().ident
        self.default_ttl = default_ttl

        super().__init__(
            connection=connection, database_path=filepath, default_ttl=default_ttl
        )

        self._setup()

    def write_safe(self):
        """Check that it is safe to write.

        SQLite is not threadsafe for concurrent _writes_ unless the
        underlying sqlite library was built with thread safety
        enabled. Even still, it may be a good idea to use a thread
        lock (``@with_thread_lock``) to prevent parallel writes.

        """
        is_main_thread = threading.current_thread().ident == self._owner_thread
        sqlite_is_safe = sqlite3.threadsafety == ThreadingMode.SERIALIZED
        return is_main_thread or sqlite_is_safe

    def _setup(self) -> None:
        if not self._setup_completed:
            if not self.connection:
                Path(self._filepath).parent.mkdir(parents=True, exist_ok=True)
                # The methods in the Cache storage object will not try to write when
                # in readonly mode. For extra safety, we open a readonly connection
                # to the database, so that SQLite itself will prohibit writing.
                database = (
                    f"file:{self._filepath}?mode=ro"
                    if self._readonly
                    else self._filepath
                )
                self.connection = sqlite3.connect(
                    database, uri=self._readonly, check_same_thread=False
                )
            cursor = self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            )
            tables = [row[0] for row in cursor.fetchall()]
            if not tables:
                # We have an empty database
                self._initialize_database()

            elif "tiled_http_response_cache_version" not in tables:
                # We have a non-empty database that we do not recognize.
                raise RuntimeError(
                    f"Database at {self._filepath} is not empty and is not "
                    f"recognized as a Tiled HTTP response cache."
                )
            else:
                # We have a non-empty database that we recognize.
                cursor = self.connection.execute(
                    "SELECT * FROM tiled_http_response_cache_version;"
                )
                (version,) = cursor.fetchone()
                if version != CACHE_DATABASE_SCHEMA_VERSION:
                    # It is likely that this cache database will be very stable,
                    # but if we must make changes we will not bother with migrations.
                    # The cache is highly disposable. Just silently blow it away and start over.
                    Path(self._filepath).unlink()
                    self.connection = sqlite3.connect(
                        self._filepath, check_same_thread=False
                    )
                    self._initialize_database()

            cursor.close()
            self._initialized = True
            self._setup_completed = True

    def _initialize_database(self) -> None:
        super()._initialize_database()
        with self._lock, closing(self.connection.cursor()) as cursor:
            cursor.execute("ALTER TABLE entries ADD COLUMN size INTEGER")
            cursor.execute("ALTER TABLE entries ADD COLUMN time_last_accessed INTEGER")

            cursor.execute(
                "CREATE TABLE tiled_http_response_cache_version (version INTEGER)"
            )
            cursor.execute(
                "INSERT INTO tiled_http_response_cache_version (version) VALUES (?)",
                (CACHE_DATABASE_SCHEMA_VERSION,),
            )

            self.connection.commit()

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
        """Filepath of the SQLite database used for storing cache data"""
        return self._filepath

    @property
    def capacity(self):
        """Max capacity of the cache, in bytes. Includes the response AND request bodies."""
        return self._capacity

    @capacity.setter
    def capacity(self, capacity):
        if capacity < 1:
            raise ValueError("Cache capacity cannot be less than 1 byte")
        elif self._max_item_size and capacity < self._max_item_size:
            raise ValueError("Cache capacity cannot be less than allowed entry size")
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
            raise ValueError("Cache entry size cannot be less than 1 byte")
        elif max_item_size > self.capacity:
            raise ValueError("Cache entry size cannot be greater than cache capacity")
        self._max_item_size = max_item_size

    @property
    def readonly(self):
        """If readonly, cache can be read but not updated."""
        return self._readonly

    def _create_entry(
        self,
        request: Request,
        response: Response,
        key: str,
        id_: uuid.UUID | None = None,
    ) -> Entry:
        """
        Store an entry in the cache.

        :param request: An HTTP request
        :type request: httpcore.Request
        :param response: An HTTP response
        :type response: httpcore.Response
        :param key: The key which identifies the entry in the cache
        :type key: str
        :param id_: The UUID identifying the entry
        :type id_: UUID

        """
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if not self.write_safe():
            raise RuntimeError("Write is not safe from another thread")
        if self.readonly:
            # Returns an Entry that is not commited to the database or streamed.
            return Entry(
                id=id_ or uuid.uuid4(),
                request=request,
                response=response,
                meta=EntryMeta(created_at=datetime.now().timestamp()),
                cache_key=key.encode("utf-8"),
            )
        with self._lock, closing(self.connection.cursor()) as cursor:
            # This is an intial check to see if the response/request are too large to cache as an entry
            request_and_response_size = measure_entry_size(request, response)
            if request_and_response_size > self.max_item_size:
                logger.debug(
                    f"Cache declined entry which is too large: "
                    f"{request_and_response_size} > {self.max_item_size} (bytes)"
                )
                # Like in the readonly branch, returns an Entry that is not commited to the database or streamed.
                return Entry(
                    id=id_ or uuid.uuid4(),
                    request=request,
                    response=response,
                    meta=EntryMeta(created_at=datetime.now().timestamp()),
                    cache_key=key.encode("utf-8"),
                )

            # Below commits to the database in the parent and handles the stream table
            parent_entry = super().create_entry(
                request=request, response=response, key=key, id_=id_
            )

            # 8 bytes in a REAL, and max 8 bytes for INTEGER so +8 for size and created_at and time_last_accessed
            starting_size = (
                len(parent_entry.cache_key) + len(parent_entry.id.bytes) + 24
            )
            if parent_entry.meta.deleted_at:
                starting_size += 8

            cursor.execute(
                "UPDATE entries SET size = ?, time_last_accessed = ? WHERE id = ?",
                (
                    request_and_response_size + starting_size,
                    datetime.now().timestamp(),
                    parent_entry.id.bytes,
                ),
            )

            # This accumulated_size_state was made into a dict so that both request and response can
            # access it and so we know if the event where the size exceeds the maximum was handles already so
            # that logic doesn't keep running as the stream finishes out.
            accumulated_size_state = {"size": starting_size, "exceed_handled": False}
            parent_entry.request.stream = self._check_max_stream_bytes(
                parent_entry.id, parent_entry.request.stream, accumulated_size_state
            )
            parent_entry.response.stream = self._check_max_stream_bytes(
                parent_entry.id, parent_entry.response.stream, accumulated_size_state
            )

            entry = Entry(
                id=parent_entry.id,
                request=parent_entry.request,
                response=parent_entry.response,
                meta=parent_entry.meta,
                cache_key=parent_entry.cache_key,
            )

            (total_size,) = cursor.execute(
                "SELECT SUM(size) FROM entries WHERE deleted_at is NULL"
            ).fetchone()
            total_size = total_size or 0  # If empty, total_size is None
            # This is the LRU eviction.
            while (total_size) > self.capacity:
                (entry_id, size) = cursor.execute(
                    """SELECT id, size FROM entries WHERE deleted_at is NULL ORDER BY time_last_accessed ASC"""
                ).fetchone()

                warnings.warn(
                    f"If reading data from cache with entry ID {entry_id}, "
                    f"stream may have been interrupted due to cache eviction."
                )
                cursor.execute("DELETE FROM entries WHERE id is ?", (entry_id,))
                total_size -= size

            self.connection.commit()
            return entry

    @with_thread_lock
    def create_entry(
        self,
        request: Request,
        response: Response,
        key: str,
        id_: uuid.UUID | None = None,
    ) -> Entry:
        if not self._setup_completed:
            self._setup()
        entry = self._create_entry(request, response, key, id_)
        return entry

    # Generator to keep track of how many bytes were streamed to ensure
    # entry remains under the max entry size for the cache.
    # If the entry exceeds, it is deleted from the cache but the stream continues.
    # The partial entry in streams is also deleted from the streams table
    def _check_max_stream_bytes(
        self, entry_id, stream_iterator, accumulated_size_state
    ):
        for chunk in stream_iterator:
            accumulated_size_state["size"] += len(chunk)
            if not accumulated_size_state["exceed_handled"]:
                # Below deletes the entry when too large
                if accumulated_size_state["size"] > self.max_item_size:
                    accumulated_size_state["exceed_handled"] = True
                    self.remove_entry(entry_id)  # This soft deletes for the entry table
                    logger.debug(
                        f"Cache declined entry which is too large with stream: > {self.max_item_size} (bytes)"
                    )
                else:
                    with self._lock, closing(self.connection.cursor()) as cursor:
                        cursor.execute(
                            "UPDATE entries SET size = ?, time_last_accessed = ? WHERE id = ?",
                            (
                                accumulated_size_state["size"],
                                datetime.now().timestamp(),
                                entry_id.bytes,
                            ),
                        )

                        self.connection.commit()

                        # This is the LRU eviction.
                        (total_size,) = cursor.execute(
                            "SELECT SUM(size) FROM entries WHERE deleted_at is NULL"
                        ).fetchone()
                        total_size = total_size or 0  # If empty, total_size is None

                        while (total_size) > self.capacity:
                            (entry_id, size) = cursor.execute(
                                """SELECT id, size FROM entries WHERE deleted_at
                                is NULL ORDER BY time_last_accessed ASC"""
                            ).fetchone()
                            warnings.warn(
                                f"If reading data from cache with entry ID {entry_id}, "
                                f"stream may have been interrupted due to cache eviction."
                            )
                            cursor.execute(
                                "DELETE FROM entries WHERE id is ?", (entry_id.bytes,)
                            )
                            total_size -= size
            yield chunk

            if accumulated_size_state["exceed_handled"]:
                # This deletes from streams after generator finishes. Does this after
                # the generator finishes because chunks are written lazily
                with self._lock, closing(self.connection.cursor()) as cursor:
                    cursor.execute(
                        "DELETE FROM streams WHERE entry_id = ?",
                        (entry_id.bytes,),
                    )
                    self.connection.commit()

    @with_thread_lock
    def get_entries(self, key: str) -> tp.List[Entry]:
        """
        Retreive a response from the cache according to the provided key.

        :param key: The key which identifies the entry in the cache
        :type key: str
        :return: A list of cached entries.
        :rtype: tp.List[Entry]
        """
        if not self._setup_completed:
            self._setup()

        parent_entries = super().get_entries(key=key)

        if not parent_entries:
            # Cache miss
            return []

        with self._lock, closing(self.connection.cursor()) as cursor:
            entries = []
            for entry in parent_entries:
                updated_entry = Entry(
                    id=entry.id,
                    request=entry.request,
                    meta=entry.meta,
                    response=entry.response,
                    cache_key=entry.cache_key,
                )
                entries.append(updated_entry)
                if not self.readonly and self.write_safe():
                    cursor.execute(
                        "UPDATE entries SET time_last_accessed = ? WHERE id = ?",
                        (datetime.now().timestamp(), entry.id.bytes),
                    )
                    self.connection.commit()
            return entries

    def update_entry(
        self,
        id: uuid.UUID,
        new_entry: tp.Union[Entry, tp.Callable[[Entry], Entry]],
    ) -> tp.Optional[Entry]:
        """
        Updates the Entry of the stored data.

        :param id: The UUID which identifies the entry in the cache
        :type id: UUID
        :param new_entry: The new Entry that we will be updating to.
        :type new_entry: tp.Union[Entry, tp.Callable[[Entry], Entry]]

        """
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if not self.readonly:
            completed_entry = super().update_entry(id=id, new_pair=new_entry)
            if completed_entry:
                with self._lock:
                    connection = self._ensure_connection()
                    cursor = connection.cursor()

                    # 8 bytes in a REAL, and max 8 bytes for INTEGER so +8 for size
                    # and created_at and time_last_accessed
                    starting_size = (
                        len(completed_entry.cache_key)
                        + len(completed_entry.id.bytes)
                        + 24
                    )
                    if completed_entry.meta.deleted_at:
                        starting_size += 8

                    request = completed_entry.request
                    response = completed_entry.response
                    request_and_response_size = measure_entry_size(request, response)
                    cursor.execute(
                        "UPDATE entries SET size = ?, time_last_accessed = ? WHERE id = ?",
                        (
                            request_and_response_size + starting_size,
                            datetime.now().timestamp(),
                            id.bytes,
                        ),
                    )
                    accumulated_size_state = {
                        "size": starting_size,
                        "exceed_handled": False,
                    }
                    completed_entry.request.stream = self._check_max_stream_bytes(
                        completed_entry.id,
                        completed_entry.request.stream,
                        accumulated_size_state,
                    )
                    completed_entry.response.stream = self._check_max_stream_bytes(
                        completed_entry.id,
                        completed_entry.response.stream,
                        accumulated_size_state,
                    )

                    connection.commit()
                    cursor.close()

                return completed_entry

    @with_thread_lock
    def clear(self):
        """Drop all entries from HTTP response cache."""
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        if self.readonly:
            raise RuntimeError("Cannot clear read-only cache")
        if not self.write_safe():
            raise RuntimeError(
                "Cannot clear cache from a different thread than the one it was created on"
            )
        with self._lock, closing(self.connection.cursor()) as cursor:
            cursor.execute("DELETE FROM streams")
            cursor.execute("DELETE FROM entries")
            self.connection.commit()

    def size(self):
        """
        Size of response bodies in cache in bytes.
        Includes the size of the corresponding request bodies.
        Does not include the size of headers and other auxiliary info.
        """
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        with self._lock, closing(self.connection.cursor()) as cursor:
            (total_size,) = cursor.execute(
                "SELECT SUM(size) FROM entries WHERE deleted_at is NULL"
            ).fetchone()
        return total_size or 0  # if empty, total_size is None

    def count(self):
        """Number of responses cached."""
        if self.connection is None or not self._setup_completed:
            raise RuntimeError("Cache is not connected")
        with self._lock, closing(self.connection.cursor()) as cursor:
            (count,) = cursor.execute(
                "SELECT COUNT(*) FROM entries WHERE deleted_at is NULL"
            ).fetchone()
        return count or 0  # if empty, count is None
