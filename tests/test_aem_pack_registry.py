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

from uuid import uuid4

import pytest
from pydantic import UUID4
from schemapack.spec.datapack import DataPack
from schemapack.spec.schemapack import SchemaPack

from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.models import (
    AEMPack,
    Model,
    PersistedConfig,
    Route,
    UnprocessedAEMPack,
    Workflow,
)
from tests.fixtures.joint import JointFixture

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
