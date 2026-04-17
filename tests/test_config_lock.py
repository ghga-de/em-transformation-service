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
from hexkit.providers.mongodb import ConfiguredMongoClient
from hexkit.providers.mongodb.testutils import MongoDbFixture

from ets.adapters.outbound.config_lock import ConfigLockAdapter
from ets.constants import CONFIG_LOCK_COLLECTION, CONFIG_LOCK_ID


def _make_lock(
    collection, *, worker_id: str = "worker-1", poll_interval: int = 1, timeout: int = 3
) -> ConfigLockAdapter:
    return ConfigLockAdapter(
        collection=collection,
        worker_id=worker_id,
        lock_expiry_seconds=60,
        poll_interval=poll_interval,
        timeout=timeout,
    )


@pytest.mark.asyncio()
async def test_acquire_lock_succeeds(mongodb: MongoDbFixture):
    """First acquire on empty collection returns True."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_LOCK_COLLECTION]
        lock = _make_lock(collection)
        await lock.setup_index()

        assert await lock.try_acquire_lock() is True

        doc = await collection.find_one({"_id": CONFIG_LOCK_ID})
        assert doc is not None
        assert doc["worker_id"] == "worker-1"


@pytest.mark.asyncio()
async def test_acquire_lock_fails_when_held(mongodb: MongoDbFixture):
    """Second acquire while lock held returns False."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_LOCK_COLLECTION]
        lock1 = _make_lock(collection, worker_id="worker-1")
        lock2 = _make_lock(collection, worker_id="worker-2")
        await lock1.setup_index()

        assert await lock1.try_acquire_lock() is True
        assert await lock2.try_acquire_lock() is False


@pytest.mark.asyncio()
async def test_release_then_acquire(mongodb: MongoDbFixture):
    """After release, another worker can acquire."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_LOCK_COLLECTION]
        lock1 = _make_lock(collection, worker_id="worker-1")
        lock2 = _make_lock(collection, worker_id="worker-2")
        await lock1.setup_index()

        await lock1.try_acquire_lock()
        await lock1.release_lock()
        assert await lock2.try_acquire_lock() is True


@pytest.mark.asyncio()
async def test_release_by_wrong_worker_is_noop(mongodb: MongoDbFixture):
    """Release by non-holder does not remove lock."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_LOCK_COLLECTION]
        lock1 = _make_lock(collection, worker_id="worker-1")
        lock2 = _make_lock(collection, worker_id="worker-2")
        await lock1.setup_index()

        await lock1.try_acquire_lock()
        await lock2.release_lock()

        doc = await collection.find_one({"_id": CONFIG_LOCK_ID})
        assert doc is not None
        assert doc["worker_id"] == "worker-1"


@pytest.mark.asyncio()
async def test_wait_returns_immediately_when_unlocked(mongodb: MongoDbFixture):
    """wait_for_lock_release returns immediately if no lock exists."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_LOCK_COLLECTION]
        lock = _make_lock(collection, timeout=2)
        await lock.setup_index()

        await lock.wait_for_lock_release()


@pytest.mark.asyncio()
async def test_wait_returns_after_release(mongodb: MongoDbFixture):
    """wait_for_lock_release returns once another task releases the lock."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_LOCK_COLLECTION]
        holder = _make_lock(collection, worker_id="holder")
        waiter = _make_lock(collection, worker_id="waiter", poll_interval=1, timeout=10)
        await holder.setup_index()

        await holder.try_acquire_lock()

        async def release_after_delay():
            await asyncio.sleep(2)
            await holder.release_lock()

        release_task = asyncio.create_task(release_after_delay())
        await waiter.wait_for_lock_release()
        await release_task


@pytest.mark.asyncio()
async def test_wait_raises_timeout(mongodb: MongoDbFixture):
    """wait_for_lock_release raises TimeoutError when lock persists."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_LOCK_COLLECTION]
        holder = _make_lock(collection, worker_id="holder")
        waiter = _make_lock(collection, worker_id="waiter", poll_interval=1, timeout=2)
        await holder.setup_index()

        await holder.try_acquire_lock()

        with pytest.raises(TimeoutError, match="2 seconds"):
            await waiter.wait_for_lock_release()


@pytest.mark.asyncio()
async def test_ttl_index_exists(mongodb: MongoDbFixture):
    """setup_indexes creates the TTL index on acquired_at."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_LOCK_COLLECTION]
        lock = _make_lock(collection)
        await lock.setup_index()

        indexes = await collection.index_information()
        ttl_indexes = {
            name: info for name, info in indexes.items() if "expireAfterSeconds" in info
        }
        assert len(ttl_indexes) == 1
        ttl_info = next(iter(ttl_indexes.values()))
        assert ttl_info["expireAfterSeconds"] == 60
