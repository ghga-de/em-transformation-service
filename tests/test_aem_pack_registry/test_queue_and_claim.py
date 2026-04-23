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
from pydantic import UUID4
from schemapack.exceptions import ValidationError
from schemapack.spec.datapack import DataPack

from ets.constants import PROCESSOR_FIELD
from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.models import IncomingAEMPack
from tests.fixtures.aem_pack_registry import (
    INVALID_DATAPACK,
    TEST_DATAPACK,
    make_ingress_pack,
    populate_db_config,
    queue_and_claim,
    queue_pack,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio


async def test_queue_creates_correct_document(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure queue_unprocessed creates a doc with correct fields, no processor, and deserializable data."""
    aem_id = uuid4()
    pack = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    await queue_pack(registry, pack)

    raw = await joint_fixture.incoming_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["model_name"] == "IngressModel"
    assert raw["annotation"] == {}
    assert raw["processor"] is None
    assert raw["processed_at"] is None
    assert str(raw["correlation_id"]) == str(pack.correlation_id)
    assert DataPack.model_validate(raw["data"]) == TEST_DATAPACK


async def test_double_queue_before_processing_stays_claimable(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure queuing the same ID twice before any claim yields one doc with latest data."""
    aem_id = uuid4()

    pack_v1 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    await queue_pack(registry, pack_v1)

    pack_v2 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    await queue_pack(registry, pack_v2)

    raw = await joint_fixture.incoming_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["processor"] is None
    assert raw["processed_at"] is None
    assert raw["annotation"] == {}

    count = await joint_fixture.incoming_aem_pack_collection.count_documents(
        {"_id": aem_id}
    )
    assert count == 1


async def test_abandoned_pack_reclaimed_by_same_instance(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure process_aem_packs reclaims a pack left claimed by a previous crash of this instance."""
    ingress = make_ingress_pack("IngressModel")
    await queue_pack(registry, ingress)

    # Simulate a previous crash: the doc is already claimed by this instance
    await joint_fixture.incoming_aem_pack_collection.update_one(
        {"_id": ingress.id},
        {"$set": {PROCESSOR_FIELD: joint_fixture.config.worker_id}},
    )

    await _assert_pack_claimed_during_processing(registry, ingress.id)


async def test_fresh_pack_claimed_on_first_query(registry: AEMPackRegistry):
    """Ensure process_aem_packs claims a fresh (unprocessed) pack via the first query."""
    ingress = make_ingress_pack("IngressModel")
    await queue_pack(registry, ingress)

    await _assert_pack_claimed_during_processing(registry, ingress.id)


async def test_queue_rejects_unknown_model_name(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure queue_unprocessed raises and does not enqueue when model_name is not in config."""
    pack = make_ingress_pack(model_name="UnknownModel")

    with pytest.raises(ValueError, match="UnknownModel"):
        await registry.queue_unprocessed(pack)

    raw = await joint_fixture.incoming_aem_pack_collection.find_one({"_id": pack.id})
    assert raw is None


async def test_queue_rejects_datapack_not_matching_schema(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure queue_unprocessed raises and does not enqueue when DataPack fails schema validation."""
    pack = make_ingress_pack(model_name="IngressModel", data=INVALID_DATAPACK)

    with pytest.raises(ValidationError):
        await registry.queue_unprocessed(pack)

    raw = await joint_fixture.incoming_aem_pack_collection.find_one({"_id": pack.id})
    assert raw is None


async def _assert_pack_claimed_during_processing(
    registry: AEMPackRegistry, expected_id: UUID4
) -> None:
    """Helper to assert that a pack is claimed during process_aem_packs."""
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
    assert claimed[0].id == expected_id


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
    )

    # Simulate concurrent update: queue v2 with same ID while v1 is claimed
    pack_v2 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    await queue_pack(registry, pack_v2)

    # Processor is preserved so in-flight instance can complete; needs_reprocessing signals v2 is pending
    raw = await joint_fixture.incoming_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["processor"] == joint_fixture.config.worker_id
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
            mapping={"pid": pack_v1.pid}
        )
    ]
    assert len(derived) == 3

    # Doc flagged for reprocessing: processor released, processed_at stamped, needs_reprocessing still True
    raw = await joint_fixture.incoming_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["processor"] is None
    assert raw["processed_at"] is not None
    assert raw["needs_reprocessing"] is True


async def test_queue_rejects_marked_for_deletion(registry: AEMPackRegistry):
    """Ensure claim_next does not pick up incoming aem_packs marked for deletion."""
    pack = make_ingress_pack(model_name="IngressModel")
    await queue_pack(registry, pack)

    await registry._soft_delete_aem_packs(incoming_aem_id=pack.id)

    claimed = await registry._incoming_aem_pack_queue.claim_next()
    assert claimed is None


async def test_claimed_aem_pack_deleted_before_processing_not_publish(
    joint_fixture: JointFixture,
):
    """Ensure that if an aem_pack is claimed for processing, then marked for deletion
    before processing, the result is not published to the transformed aem-pack collection.
    """
    config = await populate_db_config(
        daos=joint_fixture.daos,
        config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["single_route"],
        publish_models={"DerivedModel1"},
    )
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    aem_id = uuid4()

    # Queue and claim the pack
    pack = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    claimed = await queue_and_claim(registry=registry, pack=pack)

    # Simulate deletion after the claim but before the processing
    await registry._soft_delete_aem_packs(incoming_aem_id=aem_id)

    # Process the claimed pack, shouldn't publish
    await registry._process_next_aem_pack(
        incoming_aem=claimed,
        correlation_id=claimed.correlation_id,
        config=config,
    )

    # There should be no derived packs published for this pid
    derived = [
        pack
        async for pack in joint_fixture.daos.aem_pack_dao.find_all(
            mapping={"pid": aem_id}
        )
    ]
    assert len(derived) == 0
