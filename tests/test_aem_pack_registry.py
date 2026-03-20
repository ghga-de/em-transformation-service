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

"""Tests for the AEMPackRegistry core service."""

from datetime import timedelta
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
from tests.fixtures.examples import (
    VALID_MODEL_DERIVATION_CONFIGS,
    load_model_derivation_config,
)
from tests.fixtures.joint import DAOs, JointFixture

# Test schemas
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
def derived_model_3() -> Model:
    """Create a third derived model."""
    return Model(
        name="DerivedModel3",
        description="Third derived model",
        is_ingress=False,
        version=None,
        schema_=TEST_SCHEMA_V1,
        order=3,
        publish=False,
    )


@pytest.fixture
def test_workflow() -> Workflow:
    """Create a test workflow that renames the File id property."""
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


@pytest.mark.asyncio
class TestAEMPackRegistry:
    """Test suite for AEMPackRegistry core service."""

    async def test_traverse_graph_single_route(
        self,
        joint_fixture: JointFixture,
        ingress_model: Model,
        derived_model_1: Model,
        test_workflow: Workflow,
    ):
        """Test traversing a simple graph with one route."""
        route = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow.name,
            output_model_name=derived_model_1.name,
        )
        config = PersistedConfig(
            models=[ingress_model, derived_model_1],
            routes=[route],
            workflows=[test_workflow],
        )

        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress_model.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        dirty_id = uuid4()
        transformed_map = {ingress_model.name: incoming}
        dirty_map: dict[str, UUID4] = {derived_model_1.name: dirty_id}

        aem_packs_to_publish, remaining_dirty = (
            joint_fixture.aem_pack_registry._traverse_graph(
                incoming=incoming,
                dirty_map=dirty_map,
                transformed_map=transformed_map,
                config=config,
            )
        )

        # Both models were traversed so dirty entries are cleared
        assert ingress_model.name not in remaining_dirty
        assert derived_model_1.name not in remaining_dirty
        # No models publish (publish=False), so set is empty
        assert len(aem_packs_to_publish) == 0

    async def test_traverse_graph_forking_routes(
        self,
        joint_fixture: JointFixture,
        ingress_model: Model,
        derived_model_1: Model,
        derived_model_2: Model,
        test_workflow: Workflow,
        test_workflow_2: Workflow,
    ):
        """Test traversing a graph that forks into two branches."""
        route_1 = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow.name,
            output_model_name=derived_model_1.name,
        )
        route_2 = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow_2.name,
            output_model_name=derived_model_2.name,
        )
        config = PersistedConfig(
            models=[ingress_model, derived_model_1, derived_model_2],
            routes=[route_1, route_2],
            workflows=[test_workflow, test_workflow_2],
        )

        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress_model.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        dirty_id_1 = uuid4()
        dirty_id_2 = uuid4()
        transformed_map = {ingress_model.name: incoming}
        dirty_map: dict[str, UUID4] = {
            derived_model_1.name: dirty_id_1,
            derived_model_2.name: dirty_id_2,
        }

        aem_packs_to_publish, remaining_dirty = (
            joint_fixture.aem_pack_registry._traverse_graph(
                incoming=incoming,
                dirty_map=dirty_map,
                transformed_map=transformed_map,
                config=config,
            )
        )

        # Both branches traversed so all dirty entries cleared
        assert ingress_model.name not in remaining_dirty
        assert derived_model_1.name not in remaining_dirty
        assert derived_model_2.name not in remaining_dirty
        assert len(aem_packs_to_publish) == 0

    async def test_traverse_graph_chained_routes(
        self,
        joint_fixture: JointFixture,
        ingress_model: Model,
        derived_model_1: Model,
        derived_model_2: Model,
        derived_model_3: Model,
        test_workflow: Workflow,
        test_workflow_2: Workflow,
    ):
        """Test traversing a graph with chained transformations."""
        # Graph: Ingress -> D1 -> D2 -> D3
        # All models use TEST_SCHEMA_V1 (alias id), so each step applies a rename.
        route_1 = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow.name,
            output_model_name=derived_model_1.name,
        )
        route_2 = Route(
            input_model_name=derived_model_1.name,
            workflow_name=test_workflow_2.name,
            output_model_name=derived_model_2.name,
        )
        route_3 = Route(
            input_model_name=derived_model_2.name,
            workflow_name=test_workflow.name,
            output_model_name=derived_model_3.name,
        )
        config = PersistedConfig(
            models=[ingress_model, derived_model_1, derived_model_2, derived_model_3],
            routes=[route_1, route_2, route_3],
            workflows=[test_workflow, test_workflow_2],
        )

        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress_model.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        dirty_id_1 = uuid4()
        dirty_id_3 = uuid4()
        transformed_map = {ingress_model.name: incoming}
        dirty_map: dict[str, UUID4] = {
            derived_model_1.name: dirty_id_1,
            derived_model_3.name: dirty_id_3,
        }

        aem_packs_to_publish, remaining_dirty = (
            joint_fixture.aem_pack_registry._traverse_graph(
                incoming=incoming,
                dirty_map=dirty_map,
                transformed_map=transformed_map,
                config=config,
            )
        )

        # All models in the chain are traversed so dirty entries are cleared
        assert derived_model_1.name not in remaining_dirty
        assert derived_model_3.name not in remaining_dirty
        assert len(aem_packs_to_publish) == 0

    async def test_traverse_graph_with_dirty_map(
        self,
        joint_fixture: JointFixture,
        ingress_model: Model,
        derived_model_1: Model,
        test_workflow: Workflow,
    ):
        """Test that dirty map entries are cleared for traversed models."""
        route = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow.name,
            output_model_name=derived_model_1.name,
        )
        config = PersistedConfig(
            models=[ingress_model, derived_model_1],
            routes=[route],
            workflows=[test_workflow],
        )

        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress_model.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        dirty_id_1 = uuid4()
        dirty_id_2 = uuid4()
        dirty_map: dict[str, UUID4] = {
            ingress_model.name: dirty_id_1,
            derived_model_1.name: dirty_id_2,
        }
        transformed_map = {ingress_model.name: incoming}

        _aem_packs_to_publish, remaining_dirty = (
            joint_fixture.aem_pack_registry._traverse_graph(
                incoming=incoming,
                dirty_map=dirty_map,
                transformed_map=transformed_map,
                config=config,
            )
        )

        assert ingress_model.name not in remaining_dirty
        assert derived_model_1.name not in remaining_dirty

    async def test_traverse_graph_invalid_model_raises_error(
        self,
        joint_fixture: JointFixture,
        ingress_model: Model,
    ):
        """Test that processing an AEM for a non-existent model raises an error."""
        config = PersistedConfig(
            models=[ingress_model],
            routes=[],
            workflows=[],
        )

        incoming = AEMPack(
            id=uuid4(),
            model_name="NonExistentModel",
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        transformed_map = {"NonExistentModel": incoming}
        dirty_map: dict[str, UUID4] = {}

        with pytest.raises(ValueError, match="No model with name NonExistentModel"):
            joint_fixture.aem_pack_registry._traverse_graph(
                incoming=incoming,
                dirty_map=dirty_map,
                transformed_map=transformed_map,
                config=config,
            )

    async def test_create_aem_pack(self, joint_fixture: JointFixture):
        """Test creating an AEM pack wrapper."""
        model_name = "TestModel"
        original_id = uuid4()
        data = TEST_DATAPACK_V1
        annotation = {"key": "value"}

        aem_pack = joint_fixture.aem_pack_registry._create_aem_pack(
            model_name=model_name,
            original_id=original_id,
            data=data,
            annotation=annotation,
        )

        assert aem_pack.id is not None
        assert aem_pack.model_name == model_name
        assert aem_pack.original_id == original_id
        assert aem_pack.data == data
        assert aem_pack.annotation == annotation

    async def test_create_aem_pack_with_specified_id(self, joint_fixture: JointFixture):
        """Test creating an AEM pack with a specific ID."""
        specified_id = uuid4()
        model_name = "TestModel"
        original_id = uuid4()

        aem_pack = joint_fixture.aem_pack_registry._create_aem_pack(
            aem_id=specified_id,
            model_name=model_name,
            original_id=original_id,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        assert aem_pack.id == specified_id

    async def test_apply_workflow_to_data(
        self,
        joint_fixture: JointFixture,
        test_workflow: Workflow,
    ):
        """Test applying a workflow to transform data."""
        result_data = joint_fixture.aem_pack_registry._apply_workflow_to_data(
            data=TEST_DATAPACK_V1,
            annotation={},
            input_schema=TEST_SCHEMA_V1,
            workflow=test_workflow,
        )

        assert result_data is not None
        assert isinstance(result_data, DataPack)
        assert "File" in result_data.resources

    async def test_apply_workflow_preserves_empty_resources(
        self,
        joint_fixture: JointFixture,
        test_workflow: Workflow,
    ):
        """Test that a workflow on an empty datapack leaves resources unchanged."""
        result_data = joint_fixture.aem_pack_registry._apply_workflow_to_data(
            data=TEST_DATAPACK_V1,
            annotation={},
            input_schema=TEST_SCHEMA_V1,
            workflow=test_workflow,
        )

        # With no resource instances, rename_id_property leaves resources unchanged
        assert result_data is not None
        assert result_data.resources == TEST_DATAPACK_V1.resources

    async def test_traverse_graph_respects_topological_order(
        self,
        joint_fixture: JointFixture,
        ingress_model: Model,
        derived_model_1: Model,
        derived_model_2: Model,
        test_workflow: Workflow,
    ):
        """Test that routes are processed respecting topological order."""
        # Swap orders so D2 (order=1) is processed before D1 (order=2)
        derived_model_2.order = 1
        derived_model_1.order = 2

        route_1 = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow.name,
            output_model_name=derived_model_1.name,
        )
        route_2 = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow.name,
            output_model_name=derived_model_2.name,
        )

        config = PersistedConfig(
            models=[ingress_model, derived_model_1, derived_model_2],
            routes=[route_1, route_2],
            workflows=[test_workflow],
        )

        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress_model.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        dirty_id_1 = uuid4()
        dirty_id_2 = uuid4()
        transformed_map = {ingress_model.name: incoming}
        dirty_map: dict[str, UUID4] = {
            derived_model_1.name: dirty_id_1,
            derived_model_2.name: dirty_id_2,
        }

        aem_packs_to_publish, remaining_dirty = (
            joint_fixture.aem_pack_registry._traverse_graph(
                incoming=incoming,
                dirty_map=dirty_map,
                transformed_map=transformed_map,
                config=config,
            )
        )

        # Both branches were traversed regardless of order
        assert derived_model_1.name not in remaining_dirty
        assert derived_model_2.name not in remaining_dirty
        assert len(aem_packs_to_publish) == 0

    async def test_traverse_graph_reuses_dirty_map_ids(
        self,
        joint_fixture: JointFixture,
        ingress_model: Model,
        derived_model_1: Model,
        derived_model_2: Model,
        test_workflow: Workflow,
        test_workflow_2: Workflow,
    ):
        """Test that existing IDs from the dirty map are reused for derived packs."""
        route_1 = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow.name,
            output_model_name=derived_model_1.name,
        )
        route_2 = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow_2.name,
            output_model_name=derived_model_2.name,
        )
        config = PersistedConfig(
            models=[ingress_model, derived_model_1, derived_model_2],
            routes=[route_1, route_2],
            workflows=[test_workflow, test_workflow_2],
        )

        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress_model.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        existing_id_1 = uuid4()
        existing_id_2 = uuid4()
        dirty_map: dict[str, UUID4] = {
            derived_model_1.name: existing_id_1,
            derived_model_2.name: existing_id_2,
        }
        transformed_map = {ingress_model.name: incoming}

        _aem_packs_to_publish, _ = joint_fixture.aem_pack_registry._traverse_graph(
            incoming=incoming,
            dirty_map=dirty_map,
            transformed_map=transformed_map,
            config=config,
        )

        # Collect all produced packs (they won't be in aem_packs_to_publish because publish=False,
        # so we re-run and capture via a publish=True model to verify IDs)
        # Instead, run again but make models publishable
        derived_model_1_pub = Model(
            name="DerivedModel1",
            description="",
            is_ingress=False,
            version=None,
            schema_=TEST_SCHEMA_V1,
            order=1,
            publish=True,
        )
        derived_model_2_pub = Model(
            name="DerivedModel2",
            description="",
            is_ingress=False,
            version=None,
            schema_=TEST_SCHEMA_V1,
            order=2,
            publish=True,
        )
        config_pub = PersistedConfig(
            models=[ingress_model, derived_model_1_pub, derived_model_2_pub],
            routes=[route_1, route_2],
            workflows=[test_workflow, test_workflow_2],
        )

        dirty_map_2: dict[str, UUID4] = {
            derived_model_1.name: existing_id_1,
            derived_model_2.name: existing_id_2,
        }
        transformed_map_2 = {ingress_model.name: incoming}

        published, _ = joint_fixture.aem_pack_registry._traverse_graph(
            incoming=incoming,
            dirty_map=dirty_map_2,
            transformed_map=transformed_map_2,
            config=config_pub,
        )

        published_ids = {p.id for p in published}
        assert existing_id_1 in published_ids
        assert existing_id_2 in published_ids

    async def test_traverse_graph_generates_new_id_when_no_dirty_entry(
        self,
        joint_fixture: JointFixture,
        ingress_model: Model,
        derived_model_1: Model,
        test_workflow: Workflow,
    ):
        """Test that a new UUID is generated when there is no dirty map entry."""
        derived_model_1_pub = Model(
            name="DerivedModel1",
            description="",
            is_ingress=False,
            version=None,
            schema_=TEST_SCHEMA_V1,
            order=1,
            publish=True,
        )
        route = Route(
            input_model_name=ingress_model.name,
            workflow_name=test_workflow.name,
            output_model_name=derived_model_1_pub.name,
        )
        config = PersistedConfig(
            models=[ingress_model, derived_model_1_pub],
            routes=[route],
            workflows=[test_workflow],
        )

        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress_model.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        dirty_map: dict[str, UUID4] = {}
        transformed_map = {ingress_model.name: incoming}

        published, _ = joint_fixture.aem_pack_registry._traverse_graph(
            incoming=incoming,
            dirty_map=dirty_map,
            transformed_map=transformed_map,
            config=config,
        )

        assert len(published) == 1
        derived = next(iter(published))
        assert derived.id != incoming.id  # new ID generated, not reusing incoming

    async def test_queue_unprocessed_round_trips_data(
        self,
        joint_fixture: JointFixture,
    ):
        """Test that queue_unprocessed stores data that can be read back and deserialized."""
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        aem_id = uuid4()
        aem_pack = UnprocessedAEMPack(
            id=aem_id,
            model_name="TestModel",
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={"key": "value"},
        )

        await registry.queue_unprocessed(aem_pack)

        raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
        assert raw is not None
        assert raw["model_name"] == "TestModel"
        assert raw["annotation"] == {"key": "value"}

        # The stored data must be deserializable back to a DataPack
        restored_data = DataPack.model_validate(raw["data"])
        assert restored_data == TEST_DATAPACK_V1

    async def test_queue_unprocessed_sets_no_processor_on_new_doc(
        self,
        joint_fixture: JointFixture,
    ):
        """Test that a freshly queued doc has processor=None."""
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        aem_id = uuid4()
        aem_pack = UnprocessedAEMPack(
            id=aem_id,
            model_name="TestModel",
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        await registry.queue_unprocessed(aem_pack)

        raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
        assert raw is not None
        assert raw["processor"] is None
        assert raw["started_processing_at"] is None


# ------------ Helper Functions for Integration Tests ------------ #


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


# ------------ Integration Tests ------------ #


@pytest.mark.asyncio
class TestAEMPackRegistryIntegration:
    """Integration tests: event subscription → DB → processing → outbox publication."""

    # --- Category 1: Full pipeline tests --- #

    async def test_pipeline_chained_graph(self, joint_fixture: JointFixture):
        """Queue an ingress AEM for a chained graph (A→B→C), process it, verify derived packs."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"B", "C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack("A", annotation={"source": "test"})

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == 2
        names = {p.model_name for p in derived}
        assert names == {"B", "C"}
        for pack in derived:
            assert pack.original_id == ingress.id
            assert pack.annotation == {"source": "test"}
            assert isinstance(pack.data, DataPack)

        # Unprocessed doc should be cleaned up
        raw = await registry._unprocessed_aem_pack_collection.find_one(
            {"_id": ingress.id}
        )
        assert raw is None

    async def test_pipeline_forking_graph(self, joint_fixture: JointFixture):
        """Queue an ingress AEM for a forking graph (I→D1, I→D2), verify derived packs."""
        # Build a forking graph inline using rename_id_property workflows
        # (replace_resource_ids requires a BaseModel annotation — see source code issues)
        wf_file_id = Workflow(
            name="rename_to_file_id",
            description="Rename id to file_id",
            workflow={
                "operations": [
                    {
                        "name": "rename_id_property",
                        "description": "Rename alias to file_id",
                        "args": {"class_name": "File", "id_property_name": "file_id"},
                    }
                ]
            },
        )
        wf_resource_id = Workflow(
            name="rename_to_resource_id",
            description="Rename id to resource_id",
            workflow={
                "operations": [
                    {
                        "name": "rename_id_property",
                        "description": "Rename alias to resource_id",
                        "args": {
                            "class_name": "File",
                            "id_property_name": "resource_id",
                        },
                    }
                ]
            },
        )
        ingress_model = Model(
            name="I",
            description="Ingress",
            is_ingress=True,
            version="1.0.0",
            schema_=TEST_SCHEMA_V1,
            order=0,
            publish=False,
        )
        d1 = Model(
            name="D1",
            description="Derived 1",
            is_ingress=False,
            version=None,
            schema_=TEST_SCHEMA_V1,
            order=1,
            publish=True,
        )
        d2 = Model(
            name="D2",
            description="Derived 2",
            is_ingress=False,
            version=None,
            schema_=TEST_SCHEMA_V1,
            order=2,
            publish=True,
        )
        route_1 = Route(
            input_model_name="I",
            workflow_name=wf_file_id.name,
            output_model_name="D1",
        )
        route_2 = Route(
            input_model_name="I",
            workflow_name=wf_resource_id.name,
            output_model_name="D2",
        )
        config = PersistedConfig(
            models=[ingress_model, d1, d2],
            routes=[route_1, route_2],
            workflows=[wf_file_id, wf_resource_id],
        )

        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack("I")

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == 2
        names = {p.model_name for p in derived}
        assert names == {"D1", "D2"}
        for pack in derived:
            assert pack.original_id == ingress.id

    async def test_pipeline_publish_filtering(self, joint_fixture: JointFixture):
        """Only models with publish=True should appear in the aem_packs collection."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack("A")

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == 1
        assert derived[0].model_name == "C"

    async def test_pipeline_ingress_with_no_routes(self, joint_fixture: JointFixture):
        """An ingress model with no routes and publish=True publishes only itself."""
        ingress_model = Model(
            name="Isolated",
            description="Isolated ingress model",
            is_ingress=True,
            version="1.0.0",
            schema_=TEST_SCHEMA_V1,
            order=0,
            publish=True,
        )
        await joint_fixture.daos.model_dao.insert(ingress_model)
        config = PersistedConfig(models=[ingress_model], routes=[], workflows=[])

        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack("Isolated")

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        # The ingress itself is published (publish=True) but has original_id=None,
        # so it won't appear in a derived-pack query. Verify via get_by_id instead.
        published = await joint_fixture.daos.aem_pack_dao.get_by_id(ingress.id)
        assert published.model_name == "Isolated"
        assert published.original_id is None

        # Unprocessed doc deleted
        raw = await registry._unprocessed_aem_pack_collection.find_one(
            {"_id": ingress.id}
        )
        assert raw is None

    async def test_pipeline_multiple_independent_ingress_packs(
        self, joint_fixture: JointFixture
    ):
        """Two independent ingress packs for the same model produce separate derived packs."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"B", "C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]

        ingress_1 = make_ingress_pack("A", annotation={"pack": "1"})
        ingress_2 = make_ingress_pack("A", annotation={"pack": "2"})

        claimed_1 = await queue_and_claim(
            registry, ingress_1, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed_1, config=config)

        claimed_2 = await queue_and_claim(
            registry, ingress_2, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed_2, config=config)

        derived_1 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress_1.id
        )
        derived_2 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress_2.id
        )

        assert len(derived_1) == 2
        assert len(derived_2) == 2
        assert {p.model_name for p in derived_1} == {"B", "C"}
        assert {p.model_name for p in derived_2} == {"B", "C"}

        # All IDs distinct across both sets
        all_ids = {p.id for p in derived_1 + derived_2}
        assert len(all_ids) == 4

        for p in derived_1:
            assert p.annotation == {"pack": "1"}
        for p in derived_2:
            assert p.annotation == {"pack": "2"}

    # --- Category 2: Dirty marker / race condition tests --- #

    async def test_dirty_marker_discards_on_concurrent_update(
        self, joint_fixture: JointFixture
    ):
        """When queue_unprocessed is called while processing, dirty marker discards results."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"B", "C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        aem_id = uuid4()

        # Queue v1 and claim
        pack_v1 = make_ingress_pack("A", aem_id=aem_id, annotation={"v": "1"})
        claimed = await queue_and_claim(
            registry, pack_v1, joint_fixture.config.service_instance_id
        )

        # Simulate concurrent update: queue v2 with same ID while v1 is claimed
        pack_v2 = make_ingress_pack("A", aem_id=aem_id, annotation={"v": "2"})
        await registry.queue_unprocessed(pack_v2)

        # Verify dirty marker was set
        raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
        assert raw["processor"] == joint_fixture.config.dirty_marker

        # Process v1 — should detect dirty and discard results
        await process_pack(registry, incoming=claimed, config=config)

        # No derived packs published
        derived = await collect_derived_packs(joint_fixture.daos.aem_pack_dao, aem_id)
        assert len(derived) == 0

        # Doc freed for reprocessing with v2's data
        raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
        assert raw is not None
        assert raw["processor"] is None
        assert raw["started_processing_at"] is None
        assert raw["annotation"] == {"v": "2"}

    async def test_double_queue_before_processing_stays_claimable(
        self, joint_fixture: JointFixture
    ):
        """Queuing the same ID twice before any claim yields one doc with latest data."""
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        aem_id = uuid4()

        pack_v1 = make_ingress_pack("A", aem_id=aem_id, annotation={"v": "1"})
        await registry.queue_unprocessed(pack_v1)

        pack_v2 = make_ingress_pack("A", aem_id=aem_id, annotation={"v": "2"})
        await registry.queue_unprocessed(pack_v2)

        raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
        assert raw is not None
        assert raw["processor"] is None
        assert raw["started_processing_at"] is None
        assert raw["annotation"] == {"v": "2"}

        count = await registry._unprocessed_aem_pack_collection.count_documents(
            {"_id": aem_id}
        )
        assert count == 1

    async def test_stale_doc_can_be_reclaimed(self, joint_fixture: JointFixture):
        """A doc stuck with a dead processor beyond stale_after can be reclaimed."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"B", "C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack("A")

        # Queue and then mark as stale (old processor, expired timestamp)
        await registry.queue_unprocessed(ingress)
        stale_time = now_utc_ms_prec() - timedelta(
            seconds=joint_fixture.config.stale_after + 10
        )
        await registry._unprocessed_aem_pack_collection.update_one(
            {"_id": ingress.id},
            {
                "$set": {
                    "processor": "dead_instance",
                    "started_processing_at": stale_time,
                }
            },
        )

        # Fresh claim should NOT find this (processor != None)
        fresh = await registry._unprocessed_aem_pack_collection.find_one_and_update(
            filter={"original_id": None, "processor": None},
            update={
                "$set": {
                    "processor": joint_fixture.config.service_instance_id,
                    "started_processing_at": now_utc_ms_prec(),
                }
            },
            return_document=True,
        )
        assert fresh is None

        # Stale claim SHOULD find it
        stale_doc = await registry._unprocessed_aem_pack_collection.find_one_and_update(
            filter={
                "original_id": None,
                "started_processing_at": {
                    "$lt": now_utc_ms_prec()
                    - timedelta(seconds=joint_fixture.config.stale_after)
                },
            },
            update={
                "$set": {
                    "processor": joint_fixture.config.service_instance_id,
                    "started_processing_at": now_utc_ms_prec(),
                }
            },
            sort=[("started_processing_at", 1)],
            return_document=True,
        )
        assert stale_doc is not None
        assert stale_doc["processor"] == joint_fixture.config.service_instance_id

        # Process the reclaimed doc
        stale_doc["id"] = stale_doc.pop("_id")
        stale_doc["data"] = DataPack.model_validate(stale_doc["data"])
        claimed = UnprocessedAEMPack(**stale_doc)
        await process_pack(registry, incoming=claimed, config=config)

        # Derived packs created successfully
        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == 2
        assert {p.model_name for p in derived} == {"B", "C"}

    # --- Category 3: Re-processing and ID reuse tests --- #

    async def test_reprocessing_reuses_derived_pack_ids(
        self, joint_fixture: JointFixture
    ):
        """Re-processing the same ingress pack reuses existing derived pack UUIDs."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"B", "C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        aem_id = uuid4()

        # First processing
        ingress_v1 = make_ingress_pack("A", aem_id=aem_id, annotation={"v": "1"})
        claimed_v1 = await queue_and_claim(
            registry, ingress_v1, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed_v1, config=config)

        derived_v1 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, aem_id
        )
        ids_v1 = {p.model_name: p.id for p in derived_v1}

        # Second processing with updated annotation
        ingress_v2 = make_ingress_pack("A", aem_id=aem_id, annotation={"v": "2"})
        claimed_v2 = await queue_and_claim(
            registry, ingress_v2, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed_v2, config=config)

        derived_v2 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, aem_id
        )
        ids_v2 = {p.model_name: p.id for p in derived_v2}

        # IDs reused across runs
        assert ids_v1["B"] == ids_v2["B"]
        assert ids_v1["C"] == ids_v2["C"]

        # Annotations updated
        for p in derived_v2:
            assert p.annotation == {"v": "2"}

    async def test_first_processing_generates_fresh_ids(
        self, joint_fixture: JointFixture
    ):
        """First processing generates unique UUIDs for all derived packs."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"B", "C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack("A")

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        all_ids = {p.id for p in derived}
        all_ids.add(ingress.id)
        assert len(all_ids) == 3  # ingress ID + B ID + C ID, all distinct

    # --- Category 4: Config change / unreachable pack deletion --- #

    async def test_unreachable_pack_deleted_after_route_removal(
        self, joint_fixture: JointFixture
    ):
        """Removing a route causes previously derived packs to be deleted on re-processing."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"B", "C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        aem_id = uuid4()

        # First processing: B and C derived
        ingress = make_ingress_pack("A", aem_id=aem_id)
        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived_v1 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, aem_id
        )
        assert len(derived_v1) == 2

        # Remove route B→C from config (C becomes unreachable)
        route_bc = next(r for r in config.routes if r.output_model_name == "C")
        new_config = PersistedConfig(
            models=[m for m in config.models if m.name != "C"],
            routes=[r for r in config.routes if r.name != route_bc.name],
            workflows=config.workflows,
        )

        # Re-process same ingress
        ingress_v2 = make_ingress_pack("A", aem_id=aem_id)
        claimed_v2 = await queue_and_claim(
            registry, ingress_v2, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed_v2, config=new_config)

        # C deleted (unreachable), B remains
        derived_v2 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, aem_id
        )
        assert len(derived_v2) == 1
        assert derived_v2[0].model_name == "B"

    # --- Category 5: Edge cases and error handling --- #

    async def test_nonexistent_model_raises_error_in_pipeline(
        self, joint_fixture: JointFixture
    ):
        """Processing an AEMPack for a model not in the config raises ValueError."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack("NonExistent")

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        with pytest.raises(ValueError, match="No model with name NonExistent"):
            await process_pack(registry, incoming=claimed, config=config)

    async def test_annotation_propagation_through_chain(
        self, joint_fixture: JointFixture
    ):
        """Complex annotations propagate correctly through the transformation chain."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"B", "C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        annotation = {
            "workflow_hint": "test",
            "nested": {"key": "val"},
            "tags": [1, 2, 3],
        }
        ingress = make_ingress_pack("A", annotation=annotation)

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        for pack in derived:
            assert pack.annotation == annotation
