# Copyright 2021 - 2026 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for outbound mongo adapters that coordinate config state across workers.

Covers the distributed config lock (mutual exclusion during config updates) and
the config version tracker (monotonic counter that signals config changes).
Both adapters wrap a single mongo collection and share the ``mongo_collection``
fixture indirection.
"""

import asyncio

import pytest
from pymongo.asynchronous.collection import AsyncCollection

from ets.adapters.outbound.config_lock import ConfigLockAdapter
from ets.adapters.outbound.config_version import ConfigVersioner
from ets.constants import (
    CONFIG_LOCK_COLLECTION,
    CONFIG_LOCK_ID,
    CONFIG_VERSION_COLLECTION,
)

pytestmark = pytest.mark.asyncio()


# --- ConfigLockAdapter -------------------------------------------------------

lock_collection = pytest.mark.parametrize(
    "mongo_collection", [CONFIG_LOCK_COLLECTION], indirect=True
)


def make_lock(
    collection: AsyncCollection,
    *,
    worker_id: str = "worker-1",
    lock_expiry_seconds: int = 60,
    poll_interval: int = 1,
    timeout: int = 3,
) -> ConfigLockAdapter:
    """Construct a ConfigLockAdapter with sensible defaults for tests."""
    return ConfigLockAdapter(
        collection=collection,
        worker_id=worker_id,
        lock_expiry_seconds=lock_expiry_seconds,
        poll_interval=poll_interval,
        timeout=timeout,
    )


@lock_collection
async def test_acquire_lock_succeeds(mongo_collection: AsyncCollection):
    """First acquire on empty collection returns True."""
    lock = make_lock(mongo_collection)
    await lock.setup_index()

    assert await lock.try_acquire_lock() is True

    doc = await mongo_collection.find_one({"_id": CONFIG_LOCK_ID})
    assert doc is not None
    assert doc["worker_id"] == "worker-1"


@lock_collection
async def test_acquire_lock_fails_when_held(mongo_collection: AsyncCollection):
    """Second acquire while lock is held returns False."""
    lock1 = make_lock(mongo_collection)
    lock2 = make_lock(mongo_collection, worker_id="worker-2")
    await lock1.setup_index()

    assert await lock1.try_acquire_lock() is True
    assert await lock2.try_acquire_lock() is False


@lock_collection
async def test_release_then_acquire(mongo_collection: AsyncCollection):
    """Another worker can acquire after release."""
    lock1 = make_lock(mongo_collection)
    lock2 = make_lock(mongo_collection, worker_id="worker-2")
    await lock1.setup_index()

    await lock1.try_acquire_lock()
    await lock1.release_lock()
    assert await lock2.try_acquire_lock() is True


@lock_collection
async def test_release_by_wrong_worker_is_noop(mongo_collection: AsyncCollection):
    """Release by non-holder does not remove the lock."""
    lock1 = make_lock(mongo_collection)
    lock2 = make_lock(mongo_collection, worker_id="worker-2")
    await lock1.setup_index()

    await lock1.try_acquire_lock()
    await lock2.release_lock()

    doc = await mongo_collection.find_one({"_id": CONFIG_LOCK_ID})
    assert doc is not None
    assert doc["worker_id"] == "worker-1"


@lock_collection
async def test_wait_returns_immediately_when_unlocked(
    mongo_collection: AsyncCollection,
):
    """wait_for_lock_release returns immediately if no lock exists."""
    lock = make_lock(mongo_collection, timeout=2)
    await lock.setup_index()

    await lock.wait_for_lock_release()


@lock_collection
async def test_wait_returns_after_release(mongo_collection: AsyncCollection):
    """wait_for_lock_release returns once another task releases the lock."""
    holder = make_lock(mongo_collection, worker_id="holder")
    waiter = make_lock(
        mongo_collection, worker_id="waiter", poll_interval=1, timeout=10
    )
    await holder.setup_index()
    await holder.try_acquire_lock()

    async def release_after_delay():
        await asyncio.sleep(2)
        await holder.release_lock()

    release_task = asyncio.create_task(release_after_delay())
    await waiter.wait_for_lock_release()
    await release_task


@lock_collection
async def test_wait_raises_timeout(mongo_collection: AsyncCollection):
    """wait_for_lock_release raises TimeoutError after the timeout elapses."""
    poll_interval = 1
    timeout = 2
    holder = make_lock(mongo_collection, worker_id="holder")
    waiter = make_lock(
        mongo_collection,
        worker_id="waiter",
        poll_interval=poll_interval,
        timeout=timeout,
    )
    await holder.setup_index()
    await holder.try_acquire_lock()

    start = asyncio.get_event_loop().time()
    with pytest.raises(TimeoutError, match=f"{timeout} seconds"):
        await waiter.wait_for_lock_release()
    elapsed = asyncio.get_event_loop().time() - start

    # Did not wait an extra poll_interval beyond the configured timeout.
    assert elapsed < timeout + poll_interval


@lock_collection
async def test_ttl_index_exists(mongo_collection: AsyncCollection):
    """setup_index creates a TTL index on acquired_at."""
    lock = make_lock(mongo_collection)
    await lock.setup_index()

    indexes = await mongo_collection.index_information()
    ttl_indexes = [info for info in indexes.values() if "expireAfterSeconds" in info]
    assert len(ttl_indexes) == 1
    assert ttl_indexes[0]["expireAfterSeconds"] == 60


@lock_collection
async def test_setup_index_updates_ttl_via_collmod(mongo_collection: AsyncCollection):
    """Calling setup_index a second time with a different TTL updates the index in-place."""
    initial_lock = make_lock(mongo_collection)
    updated_lock = make_lock(mongo_collection, lock_expiry_seconds=300)
    await initial_lock.setup_index()
    await updated_lock.setup_index()

    indexes = await mongo_collection.index_information()
    ttl_indexes = [info for info in indexes.values() if "expireAfterSeconds" in info]
    assert len(ttl_indexes) == 1
    assert ttl_indexes[0]["expireAfterSeconds"] == 300


# --- ConfigVersioner ---------------------------------------------------------

version_collection = pytest.mark.parametrize(
    "mongo_collection", [CONFIG_VERSION_COLLECTION], indirect=True
)


@version_collection
async def test_get_version_returns_zero_when_empty(mongo_collection: AsyncCollection):
    """get_version returns 0 when no version document exists."""
    tracker = ConfigVersioner(collection=mongo_collection)
    assert await tracker.get_version() == 0


@version_collection
async def test_increment_from_zero(mongo_collection: AsyncCollection):
    """First increment creates the document with version 1."""
    tracker = ConfigVersioner(collection=mongo_collection)
    assert await tracker.increment_version() == 1
    assert await tracker.get_version() == 1


@version_collection
async def test_increment_is_monotonic(mongo_collection: AsyncCollection):
    """Multiple increments produce strictly increasing values."""
    tracker = ConfigVersioner(collection=mongo_collection)
    for expected in range(1, 6):
        assert await tracker.increment_version() == expected


@version_collection
async def test_multiple_trackers_share_version(mongo_collection: AsyncCollection):
    """Two tracker instances pointing at the same collection see the same version."""
    tracker_a = ConfigVersioner(collection=mongo_collection)
    tracker_b = ConfigVersioner(collection=mongo_collection)

    await tracker_a.increment_version()
    await tracker_a.increment_version()
    assert await tracker_b.get_version() == 2

    await tracker_b.increment_version()
    assert await tracker_a.get_version() == 3
