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

from datetime import timedelta
from uuid import uuid4

import pytest
from hexkit.utils import now_utc_ms_prec
from schemapack.spec.datapack import DataPack

from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.models import UnprocessedAEMPack
from tests.fixtures.aem_pack_registry import (
    TEST_DATAPACK,
    collect_derived_packs,
    make_ingress_pack,
    populate_db_config,
    process_pack,
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
    aem_pack = UnprocessedAEMPack(
        id=aem_id,
        model_name="TestModel",
        original_id=None,
        data=TEST_DATAPACK,
        annotation={},
        correlation_id=expected_correlation_id,
    )

    await registry.queue_unprocessed(aem_pack)

    raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["model_name"] == "TestModel"
    assert raw["annotation"] == {}
    assert raw["processor"] is None
    assert raw["started_processing_at"] is None
    assert str(raw["correlation_id"]) == str(expected_correlation_id)
    assert DataPack.model_validate(raw["data"]) == TEST_DATAPACK


async def test_double_queue_before_processing_stays_claimable(
    joint_fixture: JointFixture,
):
    """Ensure queuing the same ID twice before any claim yields one doc with latest data."""
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    aem_id = uuid4()

    pack_v1 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    await registry.queue_unprocessed(pack_v1)

    pack_v2 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    await registry.queue_unprocessed(pack_v2)

    raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["processor"] is None
    assert raw["started_processing_at"] is None
    assert raw["annotation"] == {}

    count = await registry._unprocessed_aem_pack_collection.count_documents(
        {"_id": aem_id}
    )
    assert count == 1


async def test_stale_doc_can_be_reclaimed(joint_fixture: JointFixture, monkeypatch):
    """Ensure a doc stuck with a dead processor beyond stale_after can be reclaimed."""
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    ingress = make_ingress_pack("IngressModel")

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

    # Check the stale pack is passed by intercepting the call and skipping processing
    claimed: list[UnprocessedAEMPack] = []

    async def capture_and_stop(*, incoming, config):
        claimed.append(incoming)
        raise RuntimeError("STOP, testing time!")

    monkeypatch.setattr(registry, "_process_next_aem_pack", capture_and_stop)

    with pytest.raises(RuntimeError):
        await registry.process_aem_packs()

    assert len(claimed) == 1
    assert claimed[0].id == ingress.id


async def test_dirty_marker_discards_on_concurrent_update(joint_fixture: JointFixture):
    """Ensure queueing a new ingress AEM version while processing will discard results and not publish."""
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
    await registry.queue_unprocessed(pack_v2)

    # Verify dirty marker was set
    raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["processor"] == joint_fixture.config.dirty_marker

    # Process v1 — should detect dirty and discard results
    await process_pack(registry=registry, incoming=unprocessed, config=config)

    # No derived packs published
    derived = await collect_derived_packs(
        aem_pack_dao=joint_fixture.daos.aem_pack_dao, original_id=aem_id
    )
    assert len(derived) == 0

    # Doc freed for reprocessing with v2's data
    raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
    assert raw is not None
    assert raw["processor"] is None
    assert raw["started_processing_at"] is None
    assert raw["annotation"] == {}
