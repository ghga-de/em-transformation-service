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
    TEST_DATAPACK_V1,
    collect_derived_packs,
    make_ingress_pack,
    populate_db_config,
    process_pack,
    queue_and_claim,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture


@pytest.mark.asyncio
class TestQueueAndClaim:
    """Tests for queueing, claiming, and stale-doc recovery."""

    async def test_queue_creates_correct_document(
        self,
        joint_fixture: JointFixture,
    ):
        """queue_unprocessed creates a doc with correct fields, no processor, and deserializable data."""
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        aem_id = uuid4()
        expected_correlation_id = uuid4()
        aem_pack = UnprocessedAEMPack(
            id=aem_id,
            model_name="TestModel",
            original_id=None,
            data=TEST_DATAPACK_V1,
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
        assert DataPack.model_validate(raw["data"]) == TEST_DATAPACK_V1

    async def test_double_queue_before_processing_stays_claimable(
        self, joint_fixture: JointFixture
    ):
        """Queuing the same ID twice before any claim yields one doc with latest data."""
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        aem_id = uuid4()

        pack_v1 = make_ingress_pack("IngressModel", aem_id=aem_id)
        await registry.queue_unprocessed(pack_v1)

        pack_v2 = make_ingress_pack("IngressModel", aem_id=aem_id)
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

    async def test_stale_doc_can_be_reclaimed(self, joint_fixture: JointFixture):
        """A doc stuck with a dead processor beyond stale_after can be reclaimed."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
            publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        ingress = make_ingress_pack("IngressModel")

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
        assert len(derived) == 3
        assert {pack.model_name for pack in derived} == {
            "DerivedModel1",
            "DerivedModel2",
            "DerivedModel3",
        }

    async def test_dirty_marker_discards_on_concurrent_update(
        self, joint_fixture: JointFixture
    ):
        """When queue_unprocessed is called while processing, dirty marker discards results."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
            publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        aem_id = uuid4()

        # Queue v1 and claim
        pack_v1 = make_ingress_pack("IngressModel", aem_id=aem_id)
        claimed = await queue_and_claim(
            registry, pack_v1, joint_fixture.config.service_instance_id
        )

        # Simulate concurrent update: queue v2 with same ID while v1 is claimed
        pack_v2 = make_ingress_pack("IngressModel", aem_id=aem_id)
        await registry.queue_unprocessed(pack_v2)

        # Verify dirty marker was set
        raw = await registry._unprocessed_aem_pack_collection.find_one({"_id": aem_id})
        assert raw is not None
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
        assert raw["annotation"] == {}
