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
from pathlib import Path
from uuid import uuid4

import pytest
from hexkit.correlation import set_new_correlation_id
from hexkit.utils import now_utc_ms_prec
from pydantic import UUID4
from schemapack.spec.datapack import DataPack
from schemapack.spec.schemapack import SchemaPack

from ets.core.aem_pack_registry import (
    PROCESSOR_FIELD,
    STARTED_AT_FIELD,
    AEMPackRegistry,
)
from ets.core.model_derivation import ModelDeriver
from ets.core.models import (
    AEMPack,
    PersistedConfig,
    UnprocessedAEMPack,
)
from tests.fixtures.examples import load_model_derivation_config
from tests.fixtures.joint import DAOs

# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

TEST_SCHEMA_V1 = SchemaPack.model_validate(
    {
        "schemapack": "4.0.0",
        "classes": {
            "File": {
                "id": {"propertyName": "alias"},
                "content": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "additionalProperties": False,
                    "properties": {
                        "checksum": {"type": "string"},
                        "filename": {"type": "string"},
                        "format": {"type": "string"},
                        "size": {"type": "integer"},
                    },
                    "required": ["filename", "format", "checksum", "size"],
                    "type": "object",
                },
            }
        },
    }
)

TEST_DATAPACK_V1 = DataPack.model_validate(
    {"datapack": "3.0.0", "resources": {"File": {}}}
)

# Fixed UUID used in parametrized test_create_aem_pack[specified_id]
_SPECIFIED_AEM_ID = uuid4()

# ---------------------------------------------------------------------------
# Config loading helpers
# ---------------------------------------------------------------------------


def load_aem_pack_config(
    path: Path,
    publish_models: set[str] | None = None,
) -> PersistedConfig:
    """Load a YAML config, derive schemas, and return a PersistedConfig.

    This is the file-based equivalent of the old builder helpers.  The YAML
    defines the graph topology; ``ModelDeriver`` fills in derived schemas.
    """
    validated = load_model_derivation_config(path)
    deriver = ModelDeriver(config=validated)
    models = deriver.derive_models()

    if publish_models:
        models = [
            m.model_copy(update={"publish": True}) if m.name in publish_models else m
            for m in models
        ]

    return PersistedConfig(
        models=models, routes=validated.routes, workflows=validated.workflows
    )


# ---------------------------------------------------------------------------
# pytest fixtures
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Integration test helpers
# ---------------------------------------------------------------------------


async def populate_db_config(
    daos: DAOs,
    config_yaml_path: Path,
    publish_models: set[str] | None = None,
) -> PersistedConfig:
    """Load a valid model derivation YAML, derive schemas, and populate the DB.

    Returns the PersistedConfig matching what load_config_from_db() would return.
    """
    config = load_aem_pack_config(config_yaml_path, publish_models=publish_models)

    for model in config.models:
        await daos.model_dao.insert(model)
    for route in config.routes:
        await daos.route_dao.insert(route)
    for workflow in config.workflows:
        await daos.workflow_dao.insert(workflow)

    return config


def make_ingress_pack(
    model_name: str,
    *,
    aem_id: UUID4 | None = None,
    annotation: dict | None = None,
) -> UnprocessedAEMPack:
    """Create an UnprocessedAEMPack for the given ingress model."""
    return UnprocessedAEMPack(
        id=aem_id or uuid4(),
        model_name=model_name,
        original_id=None,
        data=TEST_DATAPACK_V1,
        annotation=annotation or {},
    )


async def collect_derived_packs(
    aem_pack_dao,
    original_id: UUID4,
) -> list[AEMPack]:
    """Collect all derived AEMPacks for a given original_id from the DAO."""
    return [
        pack
        async for pack in aem_pack_dao.find_all(mapping={"original_id": original_id})
    ]


async def queue_and_claim(
    registry: AEMPackRegistry,
    pack: UnprocessedAEMPack,
    service_instance_id: str,
) -> UnprocessedAEMPack:
    """Queue an unprocessed pack and atomically claim it for processing.

    Mirrors the claim step performed by process_aem_packs().
    """
    await registry.queue_unprocessed(pack)
    doc = await registry._unprocessed_aem_pack_collection.find_one_and_update(
        filter={"_id": pack.id, PROCESSOR_FIELD: None},
        update={
            "$set": {
                PROCESSOR_FIELD: service_instance_id,
                STARTED_AT_FIELD: now_utc_ms_prec(),
            }
        },
        return_document=True,
    )
    assert doc is not None, f"Failed to claim unprocessed pack {pack.id}"
    doc["id"] = doc.pop("_id")
    doc["data"] = DataPack.model_validate(doc["data"])
    return UnprocessedAEMPack(**doc)


async def process_pack(
    registry: AEMPackRegistry,
    incoming: UnprocessedAEMPack,
    config: PersistedConfig,
) -> None:
    """Call _process_next_aem_pack with a correlation ID set (required by the outbox DAO)."""
    async with set_new_correlation_id():
        await registry._process_next_aem_pack(incoming=incoming, config=config)
