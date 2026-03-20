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

from pathlib import Path
from uuid import uuid4

import pytest
from hexkit.correlation import set_new_correlation_id
from hexkit.utils import now_utc_ms_prec
from pydantic import UUID4
from schemapack.spec.datapack import DataPack
from schemapack.spec.schemapack import SchemaPack

from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.model_derivation import ModelDeriver
from ets.core.models import (
    AEMPack,
    Model,
    PersistedConfig,
    Route,
    UnprocessedAEMPack,
    Workflow,
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
# pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ingress_model() -> Model:
    """Create a test ingress model."""
    return Model(
        name="IngressModel",
        description="Test ingress model",
        is_ingress=True,
        version="1.0.0",
        schema_=TEST_SCHEMA_V1,
        order=0,
        publish=False,
    )


@pytest.fixture
def derived_model_1() -> Model:
    """Create a first derived model."""
    return Model(
        name="DerivedModel1",
        description="First derived model",
        is_ingress=False,
        version=None,
        schema_=TEST_SCHEMA_V1,
        order=1,
        publish=False,
    )


@pytest.fixture
def derived_model_2() -> Model:
    """Create a second derived model."""
    return Model(
        name="DerivedModel2",
        description="Second derived model",
        is_ingress=False,
        version=None,
        schema_=TEST_SCHEMA_V1,
        order=2,
        publish=False,
    )


@pytest.fixture
def test_workflow() -> Workflow:
    """Create a test workflow that renames the File id property to file_id."""
    return Workflow(
        name="rename_id_workflow",
        description="Test workflow that renames id",
        workflow={
            "operations": [
                {
                    "name": "rename_id_property",
                    "description": "Rename id from alias to file_id",
                    "args": {"class_name": "File", "id_property_name": "file_id"},
                }
            ]
        },
    )


@pytest.fixture
def test_workflow_2() -> Workflow:
    """Create a second test workflow that renames the File id property to resource_id."""
    return Workflow(
        name="rename_id_workflow_2",
        description="Test workflow that renames id to resource_id",
        workflow={
            "operations": [
                {
                    "name": "rename_id_property",
                    "description": "Rename id from alias to resource_id",
                    "args": {"class_name": "File", "id_property_name": "resource_id"},
                }
            ]
        },
    )


# ---------------------------------------------------------------------------
# Builder helpers for traverse_graph parametrize cases
# ---------------------------------------------------------------------------


def _make_model(
    name: str, *, is_ingress: bool = False, version: str | None = None, order: int = 0
) -> Model:
    return Model(
        name=name,
        is_ingress=is_ingress,
        version=version,
        schema_=TEST_SCHEMA_V1,
        order=order,
        publish=False,
    )


def _make_wf1() -> Workflow:
    return Workflow(
        name="rename_id_workflow",
        description="Rename to file_id",
        workflow={
            "operations": [
                {
                    "name": "rename_id_property",
                    "description": "Rename id from alias to file_id",
                    "args": {"class_name": "File", "id_property_name": "file_id"},
                }
            ]
        },
    )


def _make_wf2() -> Workflow:
    return Workflow(
        name="rename_id_workflow_2",
        description="Rename to resource_id",
        workflow={
            "operations": [
                {
                    "name": "rename_id_property",
                    "description": "Rename id from alias to resource_id",
                    "args": {"class_name": "File", "id_property_name": "resource_id"},
                }
            ]
        },
    )


def _build_single_route_case(include_ingress_in_dirty: bool = False):
    wf = _make_wf1()
    m_in = _make_model("IngressModel", is_ingress=True, version="1.0.0", order=0)
    m_d1 = _make_model("DerivedModel1", order=1)
    route = Route(
        input_model_name="IngressModel",
        workflow_name=wf.name,
        output_model_name="DerivedModel1",
    )
    config = PersistedConfig(models=[m_in, m_d1], routes=[route], workflows=[wf])
    incoming = AEMPack(
        id=uuid4(),
        model_name="IngressModel",
        original_id=None,
        data=TEST_DATAPACK_V1,
        annotation={},
    )
    dirty_map: dict[str, UUID4] = {"DerivedModel1": uuid4()}
    if include_ingress_in_dirty:
        dirty_map["IngressModel"] = uuid4()
    return config, incoming, dirty_map, ["IngressModel", "DerivedModel1"]


def _build_forking_routes_case():
    wf1, wf2 = _make_wf1(), _make_wf2()
    m_in = _make_model("IngressModel", is_ingress=True, version="1.0.0", order=0)
    m_d1 = _make_model("DerivedModel1", order=1)
    m_d2 = _make_model("DerivedModel2", order=2)
    routes = [
        Route(
            input_model_name="IngressModel",
            workflow_name=wf1.name,
            output_model_name="DerivedModel1",
        ),
        Route(
            input_model_name="IngressModel",
            workflow_name=wf2.name,
            output_model_name="DerivedModel2",
        ),
    ]
    config = PersistedConfig(
        models=[m_in, m_d1, m_d2], routes=routes, workflows=[wf1, wf2]
    )
    incoming = AEMPack(
        id=uuid4(),
        model_name="IngressModel",
        original_id=None,
        data=TEST_DATAPACK_V1,
        annotation={},
    )
    dirty_map: dict[str, UUID4] = {"DerivedModel1": uuid4(), "DerivedModel2": uuid4()}
    return (
        config,
        incoming,
        dirty_map,
        ["IngressModel", "DerivedModel1", "DerivedModel2"],
    )


def _build_chained_routes_case():
    wf1, wf2 = _make_wf1(), _make_wf2()
    m_in = _make_model("IngressModel", is_ingress=True, version="1.0.0", order=0)
    m_d1 = _make_model("DerivedModel1", order=1)
    m_d2 = _make_model("DerivedModel2", order=2)
    m_d3 = _make_model("DerivedModel3", order=3)
    routes = [
        Route(
            input_model_name="IngressModel",
            workflow_name=wf1.name,
            output_model_name="DerivedModel1",
        ),
        Route(
            input_model_name="DerivedModel1",
            workflow_name=wf2.name,
            output_model_name="DerivedModel2",
        ),
        Route(
            input_model_name="DerivedModel2",
            workflow_name=wf1.name,
            output_model_name="DerivedModel3",
        ),
    ]
    config = PersistedConfig(
        models=[m_in, m_d1, m_d2, m_d3], routes=routes, workflows=[wf1, wf2]
    )
    incoming = AEMPack(
        id=uuid4(),
        model_name="IngressModel",
        original_id=None,
        data=TEST_DATAPACK_V1,
        annotation={},
    )
    dirty_map: dict[str, UUID4] = {"DerivedModel1": uuid4(), "DerivedModel3": uuid4()}
    return config, incoming, dirty_map, ["DerivedModel1", "DerivedModel3"]


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
    validated = load_model_derivation_config(config_yaml_path)
    deriver = ModelDeriver(config=validated)
    models = deriver.derive_models()

    if publish_models:
        models = [
            m.model_copy(update={"publish": True}) if m.name in publish_models else m
            for m in models
        ]

    for model in models:
        await daos.model_dao.insert(model)
    for route in validated.routes:
        await daos.route_dao.insert(route)
    for workflow in validated.workflows:
        await daos.workflow_dao.insert(workflow)

    return PersistedConfig(
        models=models, routes=validated.routes, workflows=validated.workflows
    )


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
        filter={"_id": pack.id, "processor": None},
        update={
            "$set": {
                "processor": service_instance_id,
                "started_processing_at": now_utc_ms_prec(),
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
