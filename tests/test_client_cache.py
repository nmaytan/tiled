import asyncio
import concurrent.futures
import os
import sqlite3
import threading
import time
from collections import namedtuple
from contextlib import closing

import numpy
import pytest

from tiled.adapters.array import ArrayAdapter
from tiled.adapters.mapping import MapAdapter
from tiled.client import Context, from_context, record_history
from tiled.client.cache import Cache, ThreadingMode, with_thread_lock
from tiled.server.app import build_app

tree = MapAdapter(
    {
        f"arr{i:03}": ArrayAdapter.from_array(i * numpy.arange(3), metadata={"i": i})
        for i in range(30)
    },
    metadata={"t": 1},
)


@pytest.fixture(scope="module")
def client():
    app = build_app(tree)
    with Context.from_app(app, cache=Cache()) as context:
        yield from_context(context)


@pytest.fixture(scope="module")
def network_client():
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("tiled.utils.os.path.realpath", lambda p: p)
    monkeypatch.setenv("TILED_CACHE_DIR", "/mnt/nfs/some/file")

    FakePartition = namedtuple(
        "FakePartition", ["device", "mountpoint", "fstype", "opts"]
    )
    if os.name == "nt":
        mock_partitions = [
            FakePartition("\\dev\\sda1", "\\", "ext4", "rw"),
            FakePartition("server:\\export", "\\mnt\\nfs", "nfs4", "rw"),
        ]
    else:
        mock_partitions = [
            FakePartition("/dev/sda1", "/", "ext4", "rw"),
            FakePartition("server:/export", "/mnt/nfs", "nfs4", "rw"),
        ]
    monkeypatch.setattr(
        "tiled.utils.psutil.disk_partitions",
        lambda *a, **k: mock_partitions,
    )
    app = build_app(tree)
    with Context.from_app(app, cache=Cache()) as context:
        yield from_context(context)


def test_cache(client, tmpdir):
    cache = client.context.cache
    client.context.cache.clear()
    before_count = cache.count()
    before_size = cache.size()
    # First time: not cached
    with record_history() as h:
        list(client.keys())
    for response in h.responses:
        assert not response.extensions.get("hishel_from_cache")

    after_count = cache.count()
    after_size = cache.size()
    assert after_count > before_count
    assert after_size > before_size

    # Second time: cached
    with record_history() as h:
        list(client.keys())
    for response in h.responses:
        assert response.extensions.get("hishel_from_cache")


def test_no_cache(client):
    original_cache = client.context.cache
    client.context.cache = None
    try:
        # First time: not cached
        with record_history() as h:
            list(client.keys())
        for response in h.responses:
            assert not response.extensions.get("hishel_from_cache")

        # Second time: cached
        with record_history() as h:
            list(client.keys())
        for response in h.responses:
            assert not response.extensions.get("hishel_from_cache")
    finally:
        client.context.cache = original_cache


def test_lru_eviction(client):
    # First time: not cached
    original_max_item_size = client.context.cache.max_item_size
    original_capacity = client.context.cache.capacity
    try:
        client.context.cache.clear()
        client.context.cache.max_item_size = 1000
        client.context.cache.capacity = 5000
        num_items = len(client)
        for i in range(num_items):
            client.values()[i]

        # Not all the items fit.
        # If this fails, perhaps the bytesize of an item has drifted.
        # Tweak the capacity.
        assert client.context.cache.count() < num_items

        # Most recently accessed: still cached
        with record_history() as h:
            client.values()[i]
        for response in h.responses:
            assert response.extensions.get("hishel_from_cache")

        # Least recently accessed: has been evicted
        with record_history() as h:
            client.values()[0]
        for response in h.responses:
            assert not response.extensions.get("hishel_from_cache")

        # Second time: cached
        with record_history() as h:
            client.metadata
        for response in h.responses:
            assert not response.extensions.get("hishel_from_cache")
    finally:
        client.context.cache.capacity = original_capacity
        client.context.cache.max_item_size = original_max_item_size


def test_item_too_large_to_store(client):
    original_max_item_size = client.context.cache.max_item_size
    try:
        client.context.cache.clear()
        client.context.cache.max_item_size = 10

        # First time: not cached
        with record_history() as h:
            list(client.keys())
        for response in h.responses:
            assert not response.extensions.get("hishel_from_cache")

        # Second time: still not cached
        with record_history() as h:
            list(client.keys())
        for response in h.responses:
            assert not response.extensions.get("hishel_from_cache")
    finally:
        client.context.cache.max_item_size = original_max_item_size


def test_readonly_cache(client):
    # Start with a writable cache.
    original_cache = client.context.cache
    try:
        client.context.cache.clear()
        # First time: not cached
        with record_history() as h:
            client.values()[0]
        for response in h.responses:
            assert not response.extensions.get("hishel_from_cache")

        # Second time: cached
        with record_history() as h:
            client.values()[0]
        for response in h.responses:
            assert response.extensions.get("hishel_from_cache")

        orig_size = client.context.cache.size()

        # Now use the same file as readonly cache.
        filepath = client.context.cache.filepath
        ro_cache = client.context.cache = Cache(filepath=filepath, readonly=True)

        # Still cached (from before)
        with record_history() as h:
            client.values()[0]
        for response in h.responses:
            assert response.extensions.get("hishel_from_cache")

        # Now look at something new...
        # First time: not cached
        with record_history() as h:
            client.values()[1]
        for response in h.responses:
            assert not response.extensions.get("hishel_from_cache")

        # Second time: still not cached
        with record_history() as h:
            client.values()[1]
        for response in h.responses:
            assert not response.extensions.get("hishel_from_cache")

        # And cache size has not changed
        assert ro_cache.size() == orig_size

        # Implementation detail: database connection is read-only,
        # for defense in depth.
        with pytest.raises(sqlite3.OperationalError):
            with closing(ro_cache.connection.cursor()) as cur:
                cur.execute("DELETE FROM entries")
    finally:
        client.context.cache = original_cache


def test_clear_cache(client):
    cache = client.context.cache
    client.values()[0]
    assert cache.size() > 0
    assert cache.count() > 0
    cache.clear()
    assert cache.size() == cache.count() == 0


def test_not_thread_safe(client, monkeypatch):
    # Check that writes fail if thread safety is disabled
    monkeypatch.setattr(sqlite3, "threadsafety", ThreadingMode.SINGLE_THREAD)
    cache = client.context.cache
    # Clear the cache in another thread
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future = executor.submit(cache.clear)
        with pytest.raises(RuntimeError):
            future.result(timeout=1)


@pytest.mark.skipif(
    sqlite3.threadsafety != ThreadingMode.SERIALIZED,
    reason="sqlite not built with thread safe support",
)
def test_thread_safety(client):
    cache = client.context.cache
    # Clear the cache in another thread
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future = executor.submit(cache.clear)
        future.result(timeout=1)


@pytest.mark.asyncio
async def test_thread_lock():
    """Check that we can prevent concurrent thread writes."""

    class Timer:
        _lock = threading.Lock()
        sleep_time = 0.01

        @with_thread_lock
        def sleep(self):
            time.sleep(self.sleep_time)

    timer = Timer()
    # Run the timer twice concurrently
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        coros = [
            loop.run_in_executor(executor, timer.sleep),
            loop.run_in_executor(executor, timer.sleep),
        ]
        t0 = time.perf_counter()
        await asyncio.gather(*coros)
        run_time = time.perf_counter() - t0
    # Check that the threads didn't run in parallel
    assert run_time >= (2.0 * timer.sleep_time), "Threads did not lock"


def test_networked_TILED_CACHE_DIR(network_client):
    cache = network_client.context.cache
    assert cache._filepath == ":memory:"


def test_not_networked_TILED_CACHE_DIR(client):
    cache = client.context.cache
    assert cache._filepath != ":memory:"


def test_cache_with_networked_TILED_CACHE_DIR(network_client):
    cache = network_client.context.cache
    network_client.context.cache.clear()
    before_count = cache.count()
    before_size = cache.size()
    # First time: not cached
    with record_history() as h:
        list(network_client.keys())
    for response in h.responses:
        assert not response.extensions.get("hishel_from_cache")

    after_count = cache.count()
    after_size = cache.size()
    assert after_count > before_count
    assert after_size > before_size

    # Second time: cached
    with record_history() as h:
        list(network_client.keys())
    for response in h.responses:
        assert response.extensions.get("hishel_from_cache")
