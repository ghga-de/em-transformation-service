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

"""Top-level functions for the service"""

import logging
from pathlib import Path

from hexkit.log import configure_logging

from ets.config import Config
from ets.inject import (
    prepare_aem_pack_registry,
    prepare_config_lock,
    prepare_config_manager,
    prepare_event_subscriber,
    prepare_incoming_aem_pack_queue,
)
from ets.ports.inbound.config_manager import ConfigManagerPort
from ets.ports.outbound.config_lock import ConfigLockPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort

log = logging.getLogger(__name__)


async def _run_config_resolution(
    *,
    aem_pack_queue: IncomingAEMPackQueuePort,
    config_lock: ConfigLockPort,
    config_manager: ConfigManagerPort,
    config_versioner: ConfigVersionerPort,
    input_config_path: Path,
) -> None:
    """Acquire the config lock on startup and run config resolution.

    If the lock is acquired, this instance is responsible for config validation/derivation.
    If not, it just waits for the holder to finish.
    """
    await config_lock.setup_index()
    acquired = await config_lock.try_acquire_lock()
    if acquired:
        try:
            log.info("Lock acquired, starting config update.")
            previous_version = await config_versioner.get_version()
            await config_manager.resolve_and_persist(input_config_path)
            # in the case of a config change, mark all the processed original AEMPacks
            # for reprocessing before the lock release
            current_version = await config_versioner.get_version()

            if current_version != previous_version:
                await aem_pack_queue.mark_all_for_reprocessing()
            log.info("Config validation/update finished.")
        finally:
            # If something fails in the process responsible for updating,
            # the lock is simply freed and updating can be attempted again on next startup
            await config_lock.release_lock()
    else:
        await config_lock.wait_for_lock_release()
        log.info("Update lock released, loading persisted config placeholder.")


async def consume_events(run_forever: bool = True):
    """Run the event consumer"""
    config = Config()  # type: ignore[call-arg]
    configure_logging(config=config)

    async with (
        prepare_config_lock(config=config) as config_lock,
        prepare_config_manager(config=config) as config_manager,
        prepare_incoming_aem_pack_queue(config=config) as aem_pack_queue,
        prepare_event_subscriber(
            config=config, aem_pack_queue_override=aem_pack_queue
        ) as event_subscriber,
    ):
        await _run_config_resolution()
        # load config from DB here
        await event_subscriber.run(forever=run_forever)


async def process_aem_packs():
    """Run processing on incoming annotated experimental metadata that has been stored in the database."""
    config = Config()  # type: ignore[call-arg]
    configure_logging(config=config)

    async with (
        prepare_config_lock(config=config) as config_lock,
        prepare_aem_pack_registry(
            config=config, config_lock_override=config_lock
        ) as aem_pack_registry,
    ):
        await _run_config_resolution(config_lock)
        # load config from DB here
        await aem_pack_registry.process_aem_packs()
