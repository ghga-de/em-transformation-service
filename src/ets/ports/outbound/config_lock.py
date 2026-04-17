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

"""Port for the distributed config update lock."""

from abc import ABC, abstractmethod


class ConfigLockPort(ABC):
    """Port for coordinating config updates across service instances.

    Uses a single lock document in MongoDB to prevent concurrent
    config validation/derivation and to block AEMPack processing
    during config updates.
    """

    @abstractmethod
    async def try_acquire_lock(self) -> bool:
        """Attempt to acquire the config update lock.

        Returns True if the lock was acquired, False if another instance holds it.
        """

    @abstractmethod
    async def release_lock(self) -> None:
        """Release the config update lock held by this instance.

        Only deletes the lock document if this instance is the holder.
        """

    @abstractmethod
    async def wait_for_lock_release(self) -> None:
        """Poll until the config lock is released or timeout is reached.

        Raises:
            TimeoutError: If the lock is not released within the configured timeout.
        """
