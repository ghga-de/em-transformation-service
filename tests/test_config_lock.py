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

"""Tests for the distributed config lock."""

import asyncio

import pytest
from pymongo.asynchronous.collection import AsyncCollection

from ets.adapters.outbound.config_lock import ConfigLockAdapter
from ets.constants import CONFIG_LOCK_COLLECTION, CONFIG_LOCK_ID

pytestmark = [
    pytest.mark.asyncio(),
    pytest.mark.parametrize(
        "mongo_collection", [CONFIG_LOCK_COLLECTION], indirect=True
    ),
]


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


async def test_acquire_lock_succeeds(mongo_collection: AsyncCollection):
    """First acquire on empty collection returns True."""
    lock = make_lock(mongo_collection)
    await lock.setup_index()

    assert await lock.try_acquire_lock() is True

    doc = await mongo_collection.find_one({"_id": CONFIG_LOCK_ID})
    assert doc is not None
    assert doc["worker_id"] == "worker-1"


async def test_acquire_lock_fails_when_held(mongo_collection: AsyncCollection):
    """Second acquire while lock is held returns False."""
    lock1 = make_lock(mongo_collection, worker_id="worker-1")
    lock2 = make_lock(mongo_collection, worker_id="worker-2")
    await lock1.setup_index()

    assert await lock1.try_acquire_lock() is True
    assert await lock2.try_acquire_lock() is False


async def test_release_then_acquire(mongo_collection: AsyncCollection):
    """Another worker can acquire after release."""
    lock1 = make_lock(mongo_collection, worker_id="worker-1")
    lock2 = make_lock(mongo_collection, worker_id="worker-2")
    await lock1.setup_index()

    await lock1.try_acquire_lock()
    await lock1.release_lock()
    assert await lock2.try_acquire_lock() is True


async def test_release_by_wrong_worker_is_noop(mongo_collection: AsyncCollection):
    """Release by non-holder does not remove the lock."""
    lock1 = make_lock(mongo_collection, worker_id="worker-1")
    lock2 = make_lock(mongo_collection, worker_id="worker-2")
    await lock1.setup_index()

    await lock1.try_acquire_lock()
    await lock2.release_lock()

    doc = await mongo_collection.find_one({"_id": CONFIG_LOCK_ID})
    assert doc is not None
    assert doc["worker_id"] == "worker-1"


async def test_wait_returns_immediately_when_unlocked(
    mongo_collection: AsyncCollection,
):
    """wait_for_lock_release returns immediately if no lock exists."""
    lock = make_lock(mongo_collection, timeout=2)
    await lock.setup_index()

    await lock.wait_for_lock_release()


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

    # Ensure this did not wait an extra poll_interval.
    assert elapsed < timeout + poll_interval


async def test_ttl_index_exists(mongo_collection: AsyncCollection):
    """setup_index creates a TTL index on acquired_at."""
    lock = make_lock(mongo_collection)
    await lock.setup_index()

    indexes = await mongo_collection.index_information()
    ttl_indexes = [info for info in indexes.values() if "expireAfterSeconds" in info]
    assert len(ttl_indexes) == 1
    assert ttl_indexes[0]["expireAfterSeconds"] == 60


async def test_setup_index_updates_ttl_via_collmod(mongo_collection: AsyncCollection):
    """Calling setup_index a second time with a different TTL updates the index in-place."""
    await make_lock(mongo_collection).setup_index()
    await make_lock(mongo_collection, lock_expiry_seconds=300).setup_index()

    indexes = await mongo_collection.index_information()
    ttl_indexes = [info for info in indexes.values() if "expireAfterSeconds" in info]
    assert len(ttl_indexes) == 1
    assert ttl_indexes[0]["expireAfterSeconds"] == 300
