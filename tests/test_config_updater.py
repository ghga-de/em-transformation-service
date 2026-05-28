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

"""Unit tests for the ConfigUpdater startup orchestrator."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from ets.core.config_manager import ConfigManager
from ets.core.config_updater import ConfigUpdater
from ets.ports.outbound.config_lock import ConfigLockPort
from ets.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort

CONFIG_PATH = Path("/fake/config.yaml")


def _make_updater(
    *,
    lock_acquired: bool,
    config_has_changed: bool = False,
    resolve_raises: Exception | None = None,
) -> tuple[
    ConfigUpdater,
    MagicMock,
    MagicMock,
    MagicMock,
]:
    """Build a ConfigUpdater wired with port mocks tuned for one branch."""
    config_lock = MagicMock(spec=ConfigLockPort)
    config_lock.setup_index = AsyncMock()
    config_lock.try_acquire_lock = AsyncMock(return_value=lock_acquired)
    config_lock.release_lock = AsyncMock()
    config_lock.wait_for_lock_release = AsyncMock()

    config_manager = MagicMock(spec=ConfigManager)
    if resolve_raises is not None:
        config_manager.resolve_and_persist = AsyncMock(side_effect=resolve_raises)
    else:
        config_manager.resolve_and_persist = AsyncMock(return_value=config_has_changed)

    incoming_aem_pack_queue = MagicMock(spec=IncomingAEMPackQueuePort)
    incoming_aem_pack_queue.mark_all_for_reprocessing = AsyncMock()

    updater = ConfigUpdater(
        input_config_path=CONFIG_PATH,
        config_lock=config_lock,
        config_manager=config_manager,
        incoming_aem_pack_queue=incoming_aem_pack_queue,
    )
    return updater, config_lock, config_manager, incoming_aem_pack_queue


@pytest.mark.asyncio()
async def test_lock_acquired_config_changed_marks_for_reprocessing():
    """When the persisted config changes during resolve, the queue is flagged."""
    updater, lock, manager, queue = _make_updater(
        lock_acquired=True, config_has_changed=True
    )

    await updater.run()

    lock.setup_index.assert_awaited_once()
    lock.try_acquire_lock.assert_awaited_once()
    manager.resolve_and_persist.assert_awaited_once_with(CONFIG_PATH)
    queue.mark_all_for_reprocessing.assert_awaited_once()
    lock.release_lock.assert_awaited_once()
    lock.wait_for_lock_release.assert_not_awaited()


@pytest.mark.asyncio()
async def test_lock_acquired_config_unchanged_does_not_reprocess():
    """When the persisted config is unchanged, the queue is not touched."""
    updater, lock, manager, queue = _make_updater(
        lock_acquired=True, config_has_changed=False
    )

    await updater.run()

    manager.resolve_and_persist.assert_awaited_once_with(CONFIG_PATH)
    queue.mark_all_for_reprocessing.assert_not_awaited()
    lock.release_lock.assert_awaited_once()


@pytest.mark.asyncio()
async def test_lock_not_acquired_waits_for_release():
    """A non-holder waits for the holder to finish without touching anything else."""
    updater, lock, manager, queue = _make_updater(lock_acquired=False)

    await updater.run()

    lock.setup_index.assert_awaited_once()
    lock.try_acquire_lock.assert_awaited_once()
    lock.wait_for_lock_release.assert_awaited_once()
    manager.resolve_and_persist.assert_not_awaited()
    queue.mark_all_for_reprocessing.assert_not_awaited()
    lock.release_lock.assert_not_awaited()


@pytest.mark.asyncio()
async def test_resolve_failure_releases_lock_and_propagates():
    """A failure during resolve still releases the lock and re-raises the error."""
    boom = RuntimeError("validation blew up")
    updater, lock, manager, queue = _make_updater(
        lock_acquired=True, resolve_raises=boom
    )

    with pytest.raises(RuntimeError, match="validation blew up"):
        await updater.run()

    manager.resolve_and_persist.assert_awaited_once_with(CONFIG_PATH)
    queue.mark_all_for_reprocessing.assert_not_awaited()
    lock.release_lock.assert_awaited_once()
