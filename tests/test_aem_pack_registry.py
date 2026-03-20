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
from uuid import uuid4

import pytest
from hexkit.utils import now_utc_ms_prec
from pydantic import UUID4
from schemapack.spec.datapack import DataPack

from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.models import (
    AEMPack,
    Model,
    PersistedConfig,
    Route,
    UnprocessedAEMPack,
    Workflow,
)
from tests.fixtures.aem_pack_registry import (
    _SPECIFIED_AEM_ID,
    TEST_DATAPACK_V1,
    TEST_SCHEMA_V1,
    aem_pack_config,  # noqa: F401
    collect_derived_packs,
    load_aem_pack_config,
    make_ingress_pack,
    populate_db_config,
    process_pack,
    queue_and_claim,
)
from tests.fixtures.examples import (
    AEM_PACK_REGISTRY_CONFIGS,
    VALID_MODEL_DERIVATION_CONFIGS,
)
from tests.fixtures.joint import JointFixture


@pytest.mark.asyncio
class TestAEMPackRegistry:
    """Test suite for AEMPackRegistry core service."""

    @pytest.mark.parametrize(
        "aem_pack_config, dirty_names",
        [
            pytest.param(
                AEM_PACK_REGISTRY_CONFIGS["single_route"],
                {"DerivedModel1"},
                id="single_route",
            ),
            pytest.param(
                AEM_PACK_REGISTRY_CONFIGS["single_route"],
                {"IngressModel", "DerivedModel1"},
                id="single_route_with_ingress",
            ),
            pytest.param(
                AEM_PACK_REGISTRY_CONFIGS["forking_routes"],
                {"DerivedModel1", "DerivedModel2"},
                id="forking_routes",
            ),
            pytest.param(
                AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
                {"DerivedModel1", "DerivedModel3"},
                id="chained_routes",
            ),
        ],
        indirect=["aem_pack_config"],
    )
    async def test_traverse_graph_clears_dirty_map(
        self,
        joint_fixture: JointFixture,
        aem_pack_config: PersistedConfig,
        dirty_names: set[str],
    ):
        """Traversal with various graph topologies clears all dirty map entries; no packs published when publish=False."""
        ingress = next(m for m in aem_pack_config.models if m.is_ingress)
        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )
        dirty_map: dict[str, UUID4] = {name: uuid4() for name in dirty_names}

        aem_packs_to_publish, remaining_dirty = (
            joint_fixture.aem_pack_registry._traverse_graph(
                incoming=incoming,
                dirty_map=dirty_map,
                transformed_map={incoming.model_name: incoming},
                config=aem_pack_config,
            )
        )

        for name in dirty_names:
            assert name not in remaining_dirty
        assert len(aem_packs_to_publish) == 0

    @pytest.mark.parametrize(
        "aem_pack_config",
        [AEM_PACK_REGISTRY_CONFIGS["single_route"]],
        ids=["single_route"],
        indirect=True,
    )
    async def test_traverse_graph_invalid_model_raises_error(
        self,
        joint_fixture: JointFixture,
        aem_pack_config: PersistedConfig,
    ):
        """Test that processing an AEM for a non-existent model raises an error."""
        incoming = AEMPack(
            id=uuid4(),
            model_name="NonExistentModel",
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        with pytest.raises(ValueError, match="No model with name NonExistentModel"):
            joint_fixture.aem_pack_registry._traverse_graph(
                incoming=incoming,
                dirty_map={},
                transformed_map={"NonExistentModel": incoming},
                config=aem_pack_config,
            )

    @pytest.mark.parametrize(
        "aem_id,expect_specified",
        [(None, False), (_SPECIFIED_AEM_ID, True)],
        ids=["auto_id", "specified_id"],
    )
    async def test_create_aem_pack(
        self, joint_fixture: JointFixture, aem_id, expect_specified
    ):
        """Test creating an AEM pack wrapper, with and without a pre-specified ID."""
        model_name = "TestModel"
        original_id = uuid4()
        annotation = {"key": "value"}

        aem_pack = joint_fixture.aem_pack_registry._create_aem_pack(
            aem_id=aem_id,
            model_name=model_name,
            original_id=original_id,
            data=TEST_DATAPACK_V1,
            annotation=annotation,
        )

        assert aem_pack.id is not None
        assert aem_pack.model_name == model_name
        assert aem_pack.original_id == original_id
        assert aem_pack.data == TEST_DATAPACK_V1
        assert aem_pack.annotation == annotation
        if expect_specified:
            assert aem_pack.id == _SPECIFIED_AEM_ID

    @pytest.mark.parametrize(
        "aem_pack_config",
        [AEM_PACK_REGISTRY_CONFIGS["single_route"]],
        ids=["single_route"],
        indirect=True,
    )
    async def test_apply_workflow_to_data(
        self,
        joint_fixture: JointFixture,
        aem_pack_config: PersistedConfig,
    ):
        """Test applying a workflow to transform data; empty resources are left unchanged."""
        workflow = aem_pack_config.workflows[0]
        ingress = next(m for m in aem_pack_config.models if m.is_ingress)

        result_data = joint_fixture.aem_pack_registry._apply_workflow_to_data(
            data=TEST_DATAPACK_V1,
            annotation={},
            input_schema=ingress.schema_,
            workflow=workflow,
        )

        assert isinstance(result_data, DataPack)
        assert "File" in result_data.resources
        # With no resource instances, rename_id_property leaves resources unchanged
        assert result_data.resources == TEST_DATAPACK_V1.resources

    @pytest.mark.parametrize(
        "aem_pack_config",
        [AEM_PACK_REGISTRY_CONFIGS["forking_routes"]],
        ids=["forking_routes"],
        indirect=True,
    )
    async def test_traverse_graph_respects_topological_order(
        self,
        joint_fixture: JointFixture,
        aem_pack_config: PersistedConfig,
    ):
        """Test that routes are processed respecting topological order."""
        models_by_name = {m.name: m for m in aem_pack_config.models}
        # Swap orders so DerivedModel2 (order=1) is processed before DerivedModel1 (order=2)
        models_by_name["DerivedModel2"].order = 1
        models_by_name["DerivedModel1"].order = 2

        ingress = next(m for m in aem_pack_config.models if m.is_ingress)
        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        dirty_map: dict[str, UUID4] = {
            "DerivedModel1": uuid4(),
            "DerivedModel2": uuid4(),
        }

        aem_packs_to_publish, remaining_dirty = (
            joint_fixture.aem_pack_registry._traverse_graph(
                incoming=incoming,
                dirty_map=dirty_map,
                transformed_map={incoming.model_name: incoming},
                config=aem_pack_config,
            )
        )

        # Both branches were traversed regardless of order
        assert "DerivedModel1" not in remaining_dirty
        assert "DerivedModel2" not in remaining_dirty
        assert len(aem_packs_to_publish) == 0

    @pytest.mark.parametrize(
        "aem_pack_config",
        [
            (
                AEM_PACK_REGISTRY_CONFIGS["forking_routes"],
                {"DerivedModel1", "DerivedModel2"},
            )
        ],
        ids=["forking_routes"],
        indirect=True,
    )
    async def test_traverse_graph_reuses_dirty_map_ids(
        self,
        joint_fixture: JointFixture,
        aem_pack_config: PersistedConfig,
    ):
        """Test that existing IDs from the dirty map are reused for derived packs."""
        ingress = next(m for m in aem_pack_config.models if m.is_ingress)
        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        existing_id_1 = uuid4()
        existing_id_2 = uuid4()
        dirty_map: dict[str, UUID4] = {
            "DerivedModel1": existing_id_1,
            "DerivedModel2": existing_id_2,
        }

        published, _ = joint_fixture.aem_pack_registry._traverse_graph(
            incoming=incoming,
            dirty_map=dirty_map,
            transformed_map={incoming.model_name: incoming},
            config=aem_pack_config,
        )

        published_ids = {p.id for p in published}
        assert existing_id_1 in published_ids
        assert existing_id_2 in published_ids

    @pytest.mark.parametrize(
        "aem_pack_config",
        [(AEM_PACK_REGISTRY_CONFIGS["single_route"], {"DerivedModel1"})],
        ids=["single_route"],
        indirect=True,
    )
    async def test_traverse_graph_generates_new_id_when_no_dirty_entry(
        self,
        joint_fixture: JointFixture,
        aem_pack_config: PersistedConfig,
    ):
        """Test that a new UUID is generated when there is no dirty map entry."""
        ingress = next(m for m in aem_pack_config.models if m.is_ingress)
        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress.name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )

        published, _ = joint_fixture.aem_pack_registry._traverse_graph(
            incoming=incoming,
            dirty_map={},
            transformed_map={incoming.model_name: incoming},
            config=aem_pack_config,
        )

        assert len(published) == 1
        derived = next(iter(published))
        assert derived.id != incoming.id  # new ID generated, not reusing incoming

    @pytest.mark.parametrize(
        "aem_pack_config, ingress_name",
        [
            pytest.param(
                (AEM_PACK_REGISTRY_CONFIGS["bottleneck"], {"D1", "D2"}),
                "I1",
                id="bottleneck_from_I1",
            ),
            pytest.param(
                (AEM_PACK_REGISTRY_CONFIGS["bottleneck"], {"D1", "D2"}),
                "I2",
                id="bottleneck_from_I2",
            ),
        ],
        indirect=["aem_pack_config"],
    )
    async def test_traverse_graph_bottleneck(
        self,
        joint_fixture: JointFixture,
        aem_pack_config: PersistedConfig,
        ingress_name: str,
    ):
        """Traversal from each ingress through a bottleneck node clears the dirty map and publishes downstream."""
        incoming = AEMPack(
            id=uuid4(),
            model_name=ingress_name,
            original_id=None,
            data=TEST_DATAPACK_V1,
            annotation={},
        )
        existing_b = uuid4()
        existing_d1 = uuid4()
        existing_d2 = uuid4()
        dirty_map: dict[str, UUID4] = {
            "B": existing_b,
            "D1": existing_d1,
            "D2": existing_d2,
        }

        published, remaining_dirty = joint_fixture.aem_pack_registry._traverse_graph(
            incoming=incoming,
            dirty_map=dirty_map,
            transformed_map={incoming.model_name: incoming},
            config=aem_pack_config,
        )

        # All downstream dirty entries cleared
        assert "B" not in remaining_dirty
        assert "D1" not in remaining_dirty
        assert "D2" not in remaining_dirty

        # D1 and D2 published (publish=True), B not published (publish=False)
        assert {p.model_name for p in published} == {"D1", "D2"}

        # Existing dirty map IDs reused
        published_ids = {p.id for p in published}
        assert existing_d1 in published_ids
        assert existing_d2 in published_ids

    async def test_queue_unprocessed_creates_correct_document(
        self,
        joint_fixture: JointFixture,
    ):
        """queue_unprocessed creates a doc with correct fields, no processor, and deserializable data."""
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
        assert raw["processor"] is None
        assert raw["started_processing_at"] is None
        assert DataPack.model_validate(raw["data"]) == TEST_DATAPACK_V1


# ------------ Integration Tests ------------ #


@pytest.mark.asyncio
class TestAEMPackRegistryIntegration:
    """Integration tests: event subscription → DB → processing → outbox publication."""

    # --- Category 1: Full pipeline tests --- #

    @pytest.mark.parametrize(
        "publish_models,annotation,expected_names",
        [
            ({"B", "C"}, {"source": "test"}, {"B", "C"}),
            ({"C"}, {}, {"C"}),
            (
                {"B", "C"},
                {"workflow_hint": "test", "nested": {"key": "val"}, "tags": [1, 2, 3]},
                {"B", "C"},
            ),
        ],
        ids=["publish_B_and_C", "publish_C_only", "complex_annotation"],
    )
    async def test_pipeline_chained_routes_variants(
        self,
        joint_fixture: JointFixture,
        publish_models: set[str],
        annotation: dict,
        expected_names: set[str],
    ):
        """Queue an ingress AEM through a chained graph (A→B→C); only published models appear with correct data."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models=publish_models,
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack("A", annotation=annotation)

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == len(expected_names)
        assert {p.model_name for p in derived} == expected_names
        for pack in derived:
            assert pack.original_id == ingress.id
            assert pack.annotation == annotation
            assert isinstance(pack.data, DataPack)

        # Unprocessed doc should be cleaned up
        raw = await registry._unprocessed_aem_pack_collection.find_one(
            {"_id": ingress.id}
        )
        assert raw is None

    async def test_pipeline_forking_graph(self, joint_fixture: JointFixture):
        """Queue an ingress AEM for a forking graph (I→D1, I→D2), verify derived packs."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["forking_routes"],
            publish_models={"DerivedModel1", "DerivedModel2"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack("IngressModel")

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == 2
        names = {p.model_name for p in derived}
        assert names == {"DerivedModel1", "DerivedModel2"}
        for pack in derived:
            assert pack.original_id == ingress.id

    @pytest.mark.parametrize(
        "ingress_name",
        ["I1", "I2"],
        ids=["bottleneck_from_I1", "bottleneck_from_I2"],
    )
    async def test_pipeline_bottleneck(
        self, joint_fixture: JointFixture, ingress_name: str
    ):
        """Process an AEMPack through a bottleneck graph (I1→B→D1,D2 / I2→B→D1,D2) for each ingress."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["bottleneck"],
            publish_models={"D1", "D2"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry  # type: ignore[assignment]
        ingress = make_ingress_pack(ingress_name, annotation={"src": ingress_name})

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == 2
        assert {p.model_name for p in derived} == {"D1", "D2"}
        for pack in derived:
            assert pack.original_id == ingress.id
            assert pack.annotation == {"src": ingress_name}
            assert isinstance(pack.data, DataPack)

        # Unprocessed doc cleaned up
        raw = await registry._unprocessed_aem_pack_collection.find_one(
            {"_id": ingress.id}
        )
        assert raw is None

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
