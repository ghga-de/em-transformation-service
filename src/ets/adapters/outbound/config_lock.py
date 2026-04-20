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

"""MongoDB adapter for the distributed config update lock."""

import asyncio
import logging

from hexkit.utils import now_utc_ms_prec
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError, OperationFailure

from ets.constants import CONFIG_LOCK_ID
from ets.ports.outbound.config_lock import ConfigLockPort

log = logging.getLogger(__name__)


class ConfigLockAdapter(ConfigLockPort):
    """MongoDB-backed distributed lock for config updates.

    Uses a single document with a fixed _id to guarantee mutual exclusion.
    A TTL index on `acquired_at` ensures stale locks from crashed instances are
    automatically cleaned up.
    """

    def __init__(
        self,
        *,
        collection: AsyncCollection,
        worker_id: str,
        lock_expiry_seconds: int,
        poll_interval: int,
        timeout: int,
    ):
        self._collection = collection
        self._worker_id = worker_id
        self._lock_expiry_seconds = lock_expiry_seconds
        self._poll_interval = poll_interval
        self._timeout = timeout

    async def setup_index(self) -> None:
        """Create or update the TTL index on acquired_at for automatic lock expiry.

        If the index already exists with a different expireAfterSeconds value,
        uses collMod to update the TTL rather than dropping and recreating.
        """
        try:
            await self._collection.create_index(
                "acquired_at", expireAfterSeconds=self._lock_expiry_seconds
            )
        except OperationFailure as exc:
            if exc.code != 85:  # IndexOptionsConflict, every other code should raise
                log.error(exc)
                raise
            # Index exists with different TTL options — update via collMod
            await self._collection.database.command(
                {
                    "collMod": self._collection.name,
                    "index": {
                        "keyPattern": {"acquired_at": 1},
                        "expireAfterSeconds": self._lock_expiry_seconds,
                    },
                }
            )

    async def try_acquire_lock(self) -> bool:
        """Attempt to acquire the config update lock.

        Returns True if the lock was acquired, False if another instance holds it.
        """
        try:
            await self._collection.insert_one(
                {
                    "_id": CONFIG_LOCK_ID,
                    "worker_id": self._worker_id,
                    "acquired_at": now_utc_ms_prec(),
                }
            )
            log.info("Config lock acquired by worker '%s'.", self._worker_id)
            return True
        except DuplicateKeyError:
            log.info(
                "Config lock already held, worker '%s' could not acquire it.",
                self._worker_id,
            )
            return False

    async def release_lock(self) -> None:
        """Release the config update lock held by this instance.

        Only deletes the lock document if this instance is the holder.
        """
        result = await self._collection.delete_one(
            {"_id": CONFIG_LOCK_ID, "worker_id": self._worker_id}
        )
        if result.deleted_count:
            log.info("Config lock released by worker '%s'.", self._worker_id)
        else:
            log.warning(
                "Config lock release failed for worker '%s'.\nEither the worker doesn't hold the lock "
                + "or it already expired via TTL.",
                self._worker_id,
            )

    async def wait_for_lock_release(self) -> None:
        """Poll until the config lock is released or timeout is reached.

        Raises:
            TimeoutError: If the lock is not released within the configured timeout.
        """
        elapsed = 0

        while elapsed <= self._timeout:
            doc = await self._collection.find_one({"_id": CONFIG_LOCK_ID})
            if doc is None:
                return

            log.info(
                "Worker '%s' waiting for config lock release (%ds elapsed of %ds).",
                self._worker_id,
                elapsed,
                self._timeout,
            )

            await asyncio.sleep(self._poll_interval)
            elapsed += self._poll_interval

        raise TimeoutError(
            f"Config lock was not released within {self._timeout} seconds."
        )
