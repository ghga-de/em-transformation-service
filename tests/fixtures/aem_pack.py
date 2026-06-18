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

"""Fixtures, test data, and helpers for AEMPackRegistry tests."""

from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from hexkit.correlation import set_correlation_id
from pydantic import UUID4
from schemapack.spec.datapack import DataPack

from emts.config import Config
from emts.core.aem_pack_registry import AEMPackRegistry
from emts.core.config_updater import ConfigUpdater
from emts.core.models import (
    IncomingAEMPack,
    PersistedConfig,
)
from emts.ports.outbound.config_lock import ConfigLockPort
from emts.ports.outbound.dao import AEMPackDao
from emts.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort
from tests.fixtures.examples import load_aem_pack_config

# Fixed UUID used in parametrized tests
EXPECTED_AEM_ID = uuid4()

TEST_DATAPACK = DataPack.model_validate(
    {
        "datapack": "4.0.0",
        "resources": {
            "File": {
                "test_alias": {
                    "content": {
                        "checksum": "abc123",
                        "filename": "test.fastq",
                        "format": "FASTQ",
                        "size": 1024,
                    }
                }
            }
        },
    }
)

INVALID_DATAPACK = DataPack.model_validate(
    {
        "datapack": "4.0.0",
        "resources": {
            "File": {
                "test_alias": {
                    "content": {
                        "checksum": "abc123",
                    }
                }
            }
        },
    }
)


@pytest.fixture
def aem_pack_config(request: pytest.FixtureRequest) -> Generator[PersistedConfig]:
    """Load a PersistedConfig from a YAML path passed via ``indirect``.

    Accepts either a bare ``Path`` or a ``(Path, set[str])`` tuple where the
    second element specifies which models should have ``publish=True``.
    """
    param = request.param
    if isinstance(param, tuple):
        path, publish_models = param
    else:
        path = param
        publish_models = None
    yield load_aem_pack_config(path, publish_models=publish_models)


def make_ingress_pack(
    model_name: str,
    *,
    aem_id: UUID4 | None = None,
    pid: str | None = None,
    data: DataPack | None = None,
    annotation: dict | None = None,
    correlation_id: UUID4 | None = None,
) -> IncomingAEMPack:
    """Create an IncomingAEMPack for the given ingress model."""
    return IncomingAEMPack(
        id=aem_id or uuid4(),
        pid=pid or str(uuid4()),
        model_name=model_name,
        data=data or TEST_DATAPACK,
        annotation=annotation or {},
        correlation_id=correlation_id or uuid4(),
    )


async def queue_pack(
    registry: AEMPackRegistry,
    pack: IncomingAEMPack,
) -> None:
    """Queue a pack without claiming it."""
    async with set_correlation_id(pack.correlation_id):
        await registry.queue_unprocessed(pack)


async def queue_and_claim(
    registry: AEMPackRegistry,
    pack: IncomingAEMPack,
) -> IncomingAEMPack:
    """Queue a pack and atomically claim it for processing."""
    await queue_pack(registry, pack)
    claimed = await registry._incoming_aem_pack_queue.claim_next()
    assert claimed is not None, f"Failed to claim unprocessed pack {pack.id}"
    return claimed


async def process_pack(
    registry: AEMPackRegistry,
    pack: IncomingAEMPack,
) -> IncomingAEMPack:
    """Queue, claim, and fully process an ingress pack; return the claimed pack.

    For the common case where a test does not interleave any step between
    claiming and processing.
    """
    claimed = await queue_and_claim(registry, pack)
    await registry._process_next_aem_pack(
        incoming_aem=claimed, correlation_id=claimed.correlation_id
    )
    return claimed


@pytest.fixture
def mock_registry() -> AEMPackRegistry:
    """An AEMPackRegistry wired with mocked collaborators.

    Suitable for tests that exercise only the in-memory methods `_traverse_graph`,
    `_create_aem_pack` and ``_apply_workflow_to_data`, which do not need Mongo/Kafka
    containers.
    """
    return AEMPackRegistry(
        config=MagicMock(spec=Config),
        aem_pack_dao=AsyncMock(spec=AEMPackDao),
        config_updater=AsyncMock(spec=ConfigUpdater),
        config_lock=AsyncMock(spec=ConfigLockPort),
        incoming_aem_pack_queue=AsyncMock(spec=IncomingAEMPackQueuePort),
    )
