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

"""Test different configurations with end-to-end tests: queue → claim → process → verify derived packs."""

from uuid import uuid4

import pytest
from schemapack.spec.datapack import DataPack

from ets.core.aem_pack_registry import AEMPackRegistry
from tests.fixtures.aem_pack_registry import (
    make_ingress_pack,
    populate_db_config,
    queue_and_claim,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio


async def test_chained_routes(joint_fixture: JointFixture):
    """Ensure only derived AEMPacks with publish=True are collected for publishing."""
    config = await populate_db_config(
        daos=joint_fixture.daos,
        config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel3"},
    )
    registry = joint_fixture.aem_pack_registry
    ingress = make_ingress_pack(model_name="IngressModel")

    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )
    await registry._process_next_aem_pack(
        incoming_aem=unprocessed,
        correlation_id=unprocessed.correlation_id,
        config=config,
    )

    derived_and_published = [
        pack
        async for pack in joint_fixture.daos.aem_pack_dao.find_all(
            mapping={"pid": ingress.pid}
        )
    ]
    assert len(derived_and_published) == 1
    assert derived_and_published[0].model_name == "DerivedModel3"
    assert derived_and_published[0].pid == ingress.pid
    assert isinstance(derived_and_published[0].data, DataPack)

    # Unprocessed doc preserved and marked as processed
    raw = await joint_fixture.incoming_aem_pack_collection.find_one({"_id": ingress.id})
    assert raw is not None
    assert raw["processed_at"] is not None
    assert raw["processor"] is None


async def test_forking_graph(joint_fixture: JointFixture):
    """Ensure a forking graph produces one derived pack per branch."""
    config = await populate_db_config(
        daos=joint_fixture.daos,
        config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["forking_routes"],
        publish_models={"DerivedModel1", "DerivedModel2"},
    )
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    ingress = make_ingress_pack("IngressModel")

    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )
    await registry._process_next_aem_pack(
        incoming_aem=unprocessed,
        correlation_id=unprocessed.correlation_id,
        config=config,
    )

    derived_and_published = [
        pack
        async for pack in joint_fixture.daos.aem_pack_dao.find_all(
            mapping={"pid": ingress.pid}
        )
    ]
    assert len(derived_and_published) == 2
    names = {pack.model_name for pack in derived_and_published}
    assert names == {"DerivedModel1", "DerivedModel2"}
    for pack in derived_and_published:
        assert pack.pid == ingress.pid


@pytest.mark.parametrize(
    "ingress_name",
    ["IngressModel1", "IngressModel2"],
    ids=["bottleneck_from_IngressModel1", "bottleneck_from_IngressModel2"],
)
async def test_bottleneck(joint_fixture: JointFixture, ingress_name: str):
    """Ensure both ingress nodes route through the bottleneck produce derived packs."""
    config = await populate_db_config(
        daos=joint_fixture.daos,
        config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["bottleneck"],
        publish_models={"DerivedModel1", "DerivedModel2"},
    )
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    ingress = make_ingress_pack(ingress_name)

    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )
    await registry._process_next_aem_pack(
        incoming_aem=unprocessed,
        correlation_id=unprocessed.correlation_id,
        config=config,
    )

    derived_and_published = [
        pack
        async for pack in joint_fixture.daos.aem_pack_dao.find_all(
            mapping={"pid": ingress.pid}
        )
    ]
    assert len(derived_and_published) == 2
    assert {pack.model_name for pack in derived_and_published} == {
        "DerivedModel1",
        "DerivedModel2",
    }
    for pack in derived_and_published:
        assert pack.pid == ingress.pid
        assert isinstance(pack.data, DataPack)

    # Unprocessed doc preserved and marked as processed
    raw = await joint_fixture.incoming_aem_pack_collection.find_one({"_id": ingress.id})
    assert raw is not None
    assert raw["processed_at"] is not None
    assert raw["processor"] is None


async def test_multiple_independent_ingress_packs(joint_fixture: JointFixture):
    """Ensure independent ingress packs produce separate derived packs with distinct IDs."""
    config = await populate_db_config(
        daos=joint_fixture.daos,
        config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry

    ingress_1 = make_ingress_pack("IngressModel")
    ingress_2 = make_ingress_pack("IngressModel")

    claimed_1 = await queue_and_claim(
        registry=registry,
        pack=ingress_1,
    )
    await registry._process_next_aem_pack(
        incoming_aem=claimed_1, correlation_id=claimed_1.correlation_id, config=config
    )

    claimed_2 = await queue_and_claim(
        registry=registry,
        pack=ingress_2,
    )
    await registry._process_next_aem_pack(
        incoming_aem=claimed_2, correlation_id=claimed_2.correlation_id, config=config
    )

    derived_1 = [
        pack
        async for pack in joint_fixture.daos.aem_pack_dao.find_all(
            mapping={"pid": ingress_1.pid}
        )
    ]
    derived_2 = [
        pack
        async for pack in joint_fixture.daos.aem_pack_dao.find_all(
            mapping={"pid": ingress_2.pid}
        )
    ]

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


async def test_correlation_id_propagated_to_derived_packs(joint_fixture: JointFixture):
    """Ensure derived pack events carry the correlation ID of the originating ingress."""
    config = await populate_db_config(
        daos=joint_fixture.daos,
        config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    expected_correlation_id = uuid4()
    ingress = make_ingress_pack(
        model_name="IngressModel", correlation_id=expected_correlation_id
    )

    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )

    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.derived_aem_pack_topic, capture_headers=True
    ) as recorder:
        await registry._process_next_aem_pack(
            incoming_aem=unprocessed,
            correlation_id=unprocessed.correlation_id,
            config=config,
        )

    events = recorder.recorded_events
    assert len(events) == 3  # All 3 derived models published
    for event in events:
        assert event.headers is not None
        assert event.headers["correlation_id"] == str(expected_correlation_id)
