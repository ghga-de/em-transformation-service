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

"""End-to-end pipeline tests: queue → claim → process → verify derived packs."""

from uuid import uuid4

import pytest
from schemapack.spec.datapack import DataPack

from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.models import Model, PersistedConfig
from tests.fixtures.aem_pack_registry import (
    TEST_SCHEMA_V1,
    collect_derived_packs,
    make_ingress_pack,
    populate_db_config,
    process_pack,
    queue_and_claim,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture


@pytest.mark.asyncio
class TestPipeline:
    """End-to-end pipeline tests: queue → claim → process → verify derived packs."""

    @pytest.mark.parametrize(
        "publish_models,annotation,expected_names",
        [
            (
                {"DerivedModel1", "DerivedModel2", "DerivedModel3"},
                {},
                {"DerivedModel1", "DerivedModel2", "DerivedModel3"},
            ),
            ({"DerivedModel3"}, {}, {"DerivedModel3"}),
        ],
        ids=["publish_all_derived", "publish_terminal_only"],
    )
    async def test_chained_routes(
        self,
        joint_fixture: JointFixture,
        publish_models: set[str],
        annotation: dict,
        expected_names: set[str],
    ):
        """Queue an ingress AEM through a chained graph (IngressModel→DerivedModel1→DerivedModel2→DerivedModel3); only published models appear with correct data."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
            publish_models=publish_models,
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        ingress = make_ingress_pack("IngressModel", annotation=annotation)

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == len(expected_names)
        assert {pack.model_name for pack in derived} == expected_names
        for pack in derived:
            assert pack.original_id == ingress.id
            assert pack.annotation == annotation
            assert isinstance(pack.data, DataPack)

        # Unprocessed doc should be cleaned up
        raw = await registry._unprocessed_aem_pack_collection.find_one(
            {"_id": ingress.id}
        )
        assert raw is None

    async def test_forking_graph(self, joint_fixture: JointFixture):
        """Queue an ingress AEM for a forking graph (I→D1, I→D2), verify derived packs."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["forking_routes"],
            publish_models={"DerivedModel1", "DerivedModel2"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        ingress = make_ingress_pack("IngressModel")

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == 2
        names = {pack.model_name for pack in derived}
        assert names == {"DerivedModel1", "DerivedModel2"}
        for pack in derived:
            assert pack.original_id == ingress.id

    @pytest.mark.parametrize(
        "ingress_name",
        ["IngressModel1", "IngressModel2"],
        ids=["bottleneck_from_IngressModel1", "bottleneck_from_IngressModel2"],
    )
    async def test_bottleneck(self, joint_fixture: JointFixture, ingress_name: str):
        """Process an AEMPack through a bottleneck graph (IngressModel1→BottleneckModel→DerivedModel1,DerivedModel2 / IngressModel2→BottleneckModel→DerivedModel1,DerivedModel2) for each ingress."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["bottleneck"],
            publish_models={"DerivedModel1", "DerivedModel2"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        ingress = make_ingress_pack(ingress_name)

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        assert len(derived) == 2
        assert {pack.model_name for pack in derived} == {
            "DerivedModel1",
            "DerivedModel2",
        }
        for pack in derived:
            assert pack.original_id == ingress.id
            assert isinstance(pack.data, DataPack)

        # Unprocessed doc cleaned up
        raw = await registry._unprocessed_aem_pack_collection.find_one(
            {"_id": ingress.id}
        )
        assert raw is None

    async def test_ingress_with_no_routes(self, joint_fixture: JointFixture):
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

        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
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

    async def test_multiple_independent_ingress_packs(
        self, joint_fixture: JointFixture
    ):
        """Two independent ingress packs for the same model produce separate derived packs."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
            publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry

        ingress_1 = make_ingress_pack("IngressModel")
        ingress_2 = make_ingress_pack("IngressModel")

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

        assert len(derived_1) == 3
        assert len(derived_2) == 3
        assert {pack.model_name for pack in derived_1} == {
            "DerivedModel1",
            "DerivedModel2",
            "DerivedModel3",
        }
        assert {pack.model_name for pack in derived_2} == {
            "DerivedModel1",
            "DerivedModel2",
            "DerivedModel3",
        }

        # All IDs distinct across both sets
        all_ids = {pack.id for pack in derived_1 + derived_2}
        assert len(all_ids) == 6

        for pack in derived_1 + derived_2:
            assert pack.annotation == {}

    async def test_correlation_id_propagated_to_derived_packs(
        self, joint_fixture: JointFixture
    ):
        """Derived pack events carry the correlation ID of the original ingress event."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
            publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        expected_correlation_id = uuid4()
        ingress = make_ingress_pack(
            "IngressModel", correlation_id=expected_correlation_id
        )

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )

        async with joint_fixture.kafka.record_events(
            in_topic=joint_fixture.config.aem_pack_topic, capture_headers=True
        ) as recorder:
            await process_pack(registry, incoming=claimed, config=config)

        events = recorder.recorded_events
        assert len(events) == 3  # All 3 derived models published
        for event in events:
            assert event.headers is not None
            assert event.headers["correlation_id"] == str(expected_correlation_id)
