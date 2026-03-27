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

"""Tests for queueing, claiming, and stale-doc recovery."""

import asyncio
import logging
from unittest.mock import patch
from uuid import uuid4

import pytest
from hexkit.correlation import set_correlation_id
from schemapack.spec.datapack import DataPack

from ets.core.aem_pack_registry import (
    PROCESSOR_FIELD,
    AEMPackRegistry,
)
from ets.core.models import IncomingAEMPack
from tests.fixtures.aem_pack_registry import (
    TEST_DATAPACK,
    make_ingress_pack,
    populate_db_config,
    queue_and_claim,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio


async def test_queue_creates_correct_document(joint_fixture: JointFixture):
    """Ensure queue_unprocessed creates a doc with correct fields, no processor, and deserializable data."""
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    aem_id = uuid4()
    expected_correlation_id = uuid4()
    aem_pack = IncomingAEMPack(
        id=aem_id,
        model_name="TestModel",
        original_id=None,
        data=TEST_DATAPACK,
        annotation={},
        correlation_id=expected_correlation_id,
    )

    async with set_correlation_id(expected_correlation_id):
        await registry.queue_unprocessed(aem_pack)

    raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["model_name"] == "TestModel"
    assert raw["annotation"] == {}
    assert raw["processor"] is None
    assert raw["processed_at"] is None
    assert str(raw["correlation_id"]) == str(expected_correlation_id)
    assert DataPack.model_validate(raw["data"]) == TEST_DATAPACK


async def test_double_queue_before_processing_stays_claimable(
    joint_fixture: JointFixture,
):
    """Ensure queuing the same ID twice before any claim yields one doc with latest data."""
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    aem_id = uuid4()

    pack_v1 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    async with set_correlation_id(pack_v1.correlation_id):
        await registry.queue_unprocessed(pack_v1)

    pack_v2 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    async with set_correlation_id(pack_v2.correlation_id):
        await registry.queue_unprocessed(pack_v2)

    raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["processor"] is None
    assert raw["processed_at"] is None
    assert raw["annotation"] == {}

    count = await registry._unprocessed_aem_pack_collection.count_documents(
        {"_id": aem_id}
    )
    assert count == 1


async def test_abandoned_pack_reclaimed_by_same_instance(joint_fixture: JointFixture):
    """Ensure process_aem_packs reclaims a pack left claimed by a previous crash of this instance."""
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    ingress = make_ingress_pack("IngressModel")

    async with set_correlation_id(ingress.correlation_id):
        await registry.queue_unprocessed(ingress)
    # Simulate a previous crash: the doc is already claimed by this instance
    await registry._unprocessed_aem_pack_collection.update_one(
        {"_id": ingress.id},
        {"$set": {PROCESSOR_FIELD: joint_fixture.config.service_instance_id}},
    )

    claimed: list[IncomingAEMPack] = []

    async def capture_and_stop(*, incoming_aem, correlation_id, config):
        claimed.append(incoming_aem)
        raise RuntimeError("STOP, testing time!")

    with (
        patch.object(registry, "_process_next_aem_pack", capture_and_stop),
        pytest.raises(RuntimeError),
    ):
        await registry.process_aem_packs()

    assert len(claimed) == 1
    assert claimed[0].id == ingress.id


async def test_fresh_pack_claimed_on_first_query(joint_fixture: JointFixture):
    """Ensure process_aem_packs claims a fresh (unprocessed) pack via the first query."""
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    ingress = make_ingress_pack("IngressModel")

    async with set_correlation_id(ingress.correlation_id):
        await registry.queue_unprocessed(ingress)

    claimed: list[IncomingAEMPack] = []

    async def capture_and_stop(*, incoming_aem, correlation_id, config):
        claimed.append(incoming_aem)
        raise RuntimeError("STOP, testing time!")

    with (
        patch.object(registry, "_process_next_aem_pack", capture_and_stop),
        pytest.raises(RuntimeError),
    ):
        await registry.process_aem_packs()

    assert len(claimed) == 1
    assert claimed[0].id == ingress.id


async def test_idle_path_logs_and_sleeps(
    joint_fixture: JointFixture,
    caplog: pytest.LogCaptureFixture,
):
    """Ensure process_aem_packs logs and sleeps when no packs are queued."""
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry

    with (
        caplog.at_level(logging.INFO, logger="ets.core.aem_pack_registry"),
        pytest.raises(asyncio.TimeoutError),
    ):
        await asyncio.wait_for(registry.process_aem_packs(), timeout=0.5)

    assert any("No new AEM found" in record.message for record in caplog.records)


async def test_concurrent_queue_publishes_and_leaves_for_reprocessing(
    joint_fixture: JointFixture,
):
    """Ensure processing publishes results even when a new version was queued concurrently, and leaves the doc for reprocessing."""
    config = await populate_db_config(
        daos=joint_fixture.daos,
        config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    aem_id = uuid4()

    # Queue v1 and claim
    pack_v1 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    unprocessed = await queue_and_claim(
        registry=registry,
        pack=pack_v1,
        service_instance_id=joint_fixture.config.service_instance_id,
    )

    # Simulate concurrent update: queue v2 with same ID while v1 is claimed
    pack_v2 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    async with set_correlation_id(pack_v2.correlation_id):
        await registry.queue_unprocessed(pack_v2)

    # Processor is preserved so in-flight instance can complete; needs_reprocessing signals v2 is pending
    raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["processor"] == joint_fixture.config.service_instance_id
    assert raw["needs_reprocessing"] is True

    # Process v1 — should still publish
    await registry._process_next_aem_pack(
        incoming_aem=unprocessed,
        correlation_id=unprocessed.correlation_id,
        config=config,
    )

    derived = [
        pack
        async for pack in joint_fixture.daos.aem_pack_dao.find_all(
            mapping={"original_id": aem_id}
        )
    ]
    assert len(derived) == 3

    # Doc flagged for reprocessing: processor released, processed_at stamped, needs_reprocessing still True
    raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["processor"] is None
    assert raw["processed_at"] is not None
    assert raw["needs_reprocessing"] is True
