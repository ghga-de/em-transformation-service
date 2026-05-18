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

"""Startup-time orchestrator for the locked config update."""

import logging
from pathlib import Path

from ets.ports.inbound.config_manager import ConfigManagerPort
from ets.ports.inbound.config_updater import ConfigUpdaterPort
from ets.ports.outbound.config_lock import ConfigLockPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort

log = logging.getLogger(__name__)


class ConfigUpdater(ConfigUpdaterPort):
    """Coordinates the locked startup config update across service instances."""

    def __init__(
        self,
        *,
        input_config_path: Path,
        config_lock: ConfigLockPort,
        config_manager: ConfigManagerPort,
        versioner: ConfigVersionerPort,
        incoming_aem_pack_queue: IncomingAEMPackQueuePort,
    ):
        self._input_config_path = input_config_path
        self._config_lock = config_lock
        self._config_manager = config_manager
        self._versioner = versioner
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
            log.info("Update lock released, loading persisted config placeholder.")
            return

        try:
            log.info("Lock acquired, starting config update.")
            previous_version = await self._versioner.get_version()
            await self._config_manager.resolve_and_persist(self._input_config_path)
            current_version = await self._versioner.get_version()
            if current_version != previous_version:
                await self._incoming_aem_pack_queue.mark_all_for_reprocessing()
            log.info("Config validation/update finished.")
        finally:
            # On failure, the lock is simply freed and updating can be retried
            # on the next startup.
            await self._config_lock.release_lock()
