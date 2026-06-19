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

"""Startup-time orchestrator for the config update."""

import logging
from pathlib import Path

from emts.core.config_updater import ConfigUpdater
from emts.ports.outbound.config_lock import ConfigLockPort
from emts.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort

log = logging.getLogger(__name__)


class ConfigManager:
    """Coordinates the startup config update across service instances."""

    def __init__(
        self,
        *,
        input_config_path: Path,
        config_lock: ConfigLockPort,
        config_updater: ConfigUpdater,
        incoming_aem_pack_queue: IncomingAEMPackQueuePort,
    ):
        self._input_config_path = input_config_path
        self._config_lock = config_lock
        self._config_updater = config_updater
        self._incoming_aem_pack_queue = incoming_aem_pack_queue

    async def run(self) -> None:
        """Acquire the config lock and run config resolution.

        If the lock is acquired, this instance is responsible for config
        validation/derivation. If the resolved config differs from what was
        previously persisted, all processed AEMPacks are marked for
        reprocessing before the lock is released. Instances that fail to
        acquire the lock just wait for the holder to finish.
        """
        await self._config_lock.setup_index()
        if not await self._config_lock.try_acquire_lock():
            await self._config_lock.wait_for_lock_release()
            log.info("Update lock released, loading persisted config.")
            return

        try:
            log.info("Lock acquired, starting config update.")
            config_has_changed = await self._config_updater.resolve_and_persist(
                self._input_config_path
            )
            if config_has_changed:
                await self._incoming_aem_pack_queue.mark_all_for_reprocessing()
            log.info("Config validation/update finished.")
        finally:
            # On failure, the lock is simply freed and updating can be retried
            # on the next startup.
            await self._config_lock.release_lock()
