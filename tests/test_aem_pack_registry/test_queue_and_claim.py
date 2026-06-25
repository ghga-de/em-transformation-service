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
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from pydantic import UUID4
from schemapack.exceptions import ValidationError
from schemapack.spec.datapack import DataPack

from emts.core.aem_pack_registry import AEMPackRegistry
from emts.core.models import IncomingAEMPack
from tests.fixtures.aem_pack import (
    INVALID_DATAPACK,
    TEST_DATAPACK,
    make_ingress_pack,
    process_pack,
    queue_and_claim,
    queue_pack,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


async def test_queue_creates_correct_document(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure queue_unprocessed creates a doc with correct fields, no claim, and deserializable data."""
    aem_id = uuid4()
    pack = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    await queue_pack(registry, pack)

    raw = await joint_fixture.incoming_doc(aem_id)
    assert raw is not None
    assert raw["model_name"] == "IngressModel"
    assert raw["annotation"] == {}
    assert raw["claimed_at"] is None
    assert raw["processed_at"] is None
    # A fresh insert is never flagged for reprocessing (only an overwrite of an
    # already claimed/processed doc is).
    assert raw["needs_reprocessing"] is False
    assert str(raw["correlation_id"]) == str(pack.correlation_id)
    assert DataPack.model_validate(raw["data"]) == TEST_DATAPACK


async def test_successful_pack_is_not_reprocessed(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """A pack queued fresh and processed once must not be picked up again. Regression
    for needs_reprocessing being wrongly set on first insert, which made claim_next
    reclaim every pack for a redundant second processing run.
    """
    pack = make_ingress_pack("IngressModel")
    await process_pack(registry, pack)

    doc = await joint_fixture.incoming_doc(pack.id)
    assert doc is not None
    assert doc["processed_at"] is not None
    assert doc["needs_reprocessing"] is False

    # The terminal pack is not reclaimed for a redundant reprocess.
    assert await registry._incoming_aem_pack_queue.claim_next() is None


async def test_double_queue_before_processing_stays_claimable(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure queuing the same ID twice before any claim yields one doc with latest data."""
    aem_id = uuid4()

    pack_v1 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id, version=1)
    await queue_pack(registry, pack_v1)

    pack_v2 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id, version=2)
    await queue_pack(registry, pack_v2)

    raw = await joint_fixture.incoming_doc(aem_id)
    assert raw is not None
    assert raw["claimed_at"] is None
    assert raw["processed_at"] is None
    assert raw["annotation"] == {}

    count = await joint_fixture.incoming_aem_pack_collection.count_documents(
        {"_id": aem_id}
    )
    assert count == 1


async def test_queue_rejects_non_newer_version(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure an equal-or-lower version does not overwrite the stored document."""
    aem_id = uuid4()

    await queue_pack(
        registry,
        make_ingress_pack(
            model_name="IngressModel", aem_id=aem_id, annotation={"v": 2}, version=2
        ),
    )

    # An older version for the same id must be rejected, leaving the stored doc intact.
    await queue_pack(
        registry,
        make_ingress_pack(
            model_name="IngressModel", aem_id=aem_id, annotation={"v": 1}, version=1
        ),
    )

    raw = await joint_fixture.incoming_doc(aem_id)
    assert raw is not None
    assert raw["version"] == 2
    assert raw["annotation"] == {"v": 2}


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

    raw = await joint_fixture.incoming_doc(pack.id)
    assert raw is None


async def test_queue_rejects_datapack_not_matching_schema(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure queue_unprocessed raises and does not enqueue when DataPack fails schema validation."""
    pack = make_ingress_pack(model_name="IngressModel", data=INVALID_DATAPACK)

    with pytest.raises(ValidationError):
        await registry.queue_unprocessed(pack)

    raw = await joint_fixture.incoming_doc(pack.id)
    assert raw is None


async def _assert_pack_claimed_during_processing(
    registry: AEMPackRegistry, expected_id: UUID4
) -> None:
    """Helper to assert that a pack is claimed during process_aem_packs."""
    claimed: list[IncomingAEMPack] = []

    async def capture_and_stop(*, incoming_aem, correlation_id):
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
        caplog.at_level(logging.INFO, logger="emts.core.aem_pack_registry"),
        pytest.raises(asyncio.TimeoutError),
    ):
        await asyncio.wait_for(registry.process_aem_packs(), timeout=0.5)

    assert any("No new AEM found" in record.message for record in caplog.records)


async def test_superseded_version_discards_stale_results(
    joint_fixture: JointFixture,
):
    """A claim processed after a strictly newer version was queued must NOT publish its
    now-stale results, and must not reach the terminal state. The content it derived is
    outdated, so it is discarded and the doc is left unprocessed for the newer version
    to be reprocessed in its place.
    """
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    aem_id = uuid4()
    pid = str(uuid4())

    # Claim v1, then a strictly newer v2 of the same pack arrives while v1 is in flight.
    pack_v1 = make_ingress_pack(
        model_name="IngressModel", aem_id=aem_id, pid=pid, version=1
    )
    unprocessed = await queue_and_claim(registry=registry, pack=pack_v1)

    pack_v2 = make_ingress_pack(
        model_name="IngressModel", aem_id=aem_id, pid=pid, version=2
    )
    await queue_pack(registry, pack_v2)

    # v2 overwrote the content; v1's claim is preserved and reprocessing is flagged.
    raw = await joint_fixture.incoming_doc(aem_id)
    assert raw is not None
    assert raw["version"] == 2
    assert raw["claimed_at"] is not None
    assert raw["needs_reprocessing"] is True

    # Finish processing the now-stale v1.
    await registry._process_next_aem_pack(
        incoming_aem=unprocessed,
        correlation_id=unprocessed.correlation_id,
    )

    # Nothing published, and the pack is not marked processed: v1's results were
    # discarded. The claim is retained, so v2 is reprocessed once it ages out (stale).
    assert await joint_fixture.derived_packs(pid) == []
    raw = await joint_fixture.incoming_doc(aem_id)
    assert raw is not None
    assert raw["processed_at"] is None
    assert raw["version"] == 2
    assert raw["needs_reprocessing"] is True


async def test_newer_version_reprocessed_after_stale_claim_discarded(
    joint_fixture: JointFixture,
):
    """End-to-end: after a stale v1 claim is discarded, it is v2's content — not v1's —
    that the next claim reprocesses and publishes. The superseded version never wins.
    """
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    aem_id = uuid4()
    pid = str(uuid4())

    pack_v1 = make_ingress_pack(
        model_name="IngressModel", aem_id=aem_id, pid=pid, version=1
    )
    unprocessed = await queue_and_claim(registry=registry, pack=pack_v1)
    pack_v2 = make_ingress_pack(
        model_name="IngressModel", aem_id=aem_id, pid=pid, version=2
    )
    await queue_pack(registry, pack_v2)

    # Stale v1 finishes and is discarded (verified in the previous test).
    await registry._process_next_aem_pack(
        incoming_aem=unprocessed,
        correlation_id=unprocessed.correlation_id,
    )
    assert await joint_fixture.derived_packs(pid) == []

    # v1's claim ages past the TTL, so the next claim reclaims the doc — now carrying
    # v2's content — and processing it publishes v2's derived packs.
    await joint_fixture.incoming_aem_pack_collection.update_one(
        {"_id": aem_id},
        {"$set": {"claimed_at": datetime.now(UTC) - timedelta(seconds=600)}},
    )
    reclaimed = await registry._incoming_aem_pack_queue.claim_next()
    assert reclaimed is not None
    assert reclaimed.id == aem_id
    assert reclaimed.version == 2

    await registry._process_next_aem_pack(
        incoming_aem=reclaimed,
        correlation_id=reclaimed.correlation_id,
    )

    assert len(await joint_fixture.derived_packs(pid)) == 3
    raw = await joint_fixture.incoming_doc(aem_id)
    assert raw is not None
    assert raw["processed_at"] is not None
    assert raw["version"] == 2


async def test_mark_processed_rejects_superseded_version(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """The atomic backstop to the best-effort pre-publish guard: even if a slow worker
    slips past the guard, mark_processed must not reach the terminal state for a version
    the store has already moved past.
    """
    aem_id = uuid4()
    pid = str(uuid4())
    pack_v1 = make_ingress_pack(
        model_name="IngressModel", aem_id=aem_id, pid=pid, version=1
    )
    await queue_and_claim(registry=registry, pack=pack_v1)
    pack_v2 = make_ingress_pack(
        model_name="IngressModel", aem_id=aem_id, pid=pid, version=2
    )
    await queue_pack(registry, pack_v2)

    # A slow v1 worker tries to commit after v2 superseded it: must be a no-op.
    await registry._incoming_aem_pack_queue.mark_processed(pack_v1.id, pack_v1.version)

    raw = await joint_fixture.incoming_doc(aem_id)
    assert raw is not None
    assert raw["processed_at"] is None
    assert raw["version"] == 2


async def test_queue_rejects_marked_for_deletion(registry: AEMPackRegistry):
    """Ensure claim_next does not pick up incoming aem_packs marked for deletion."""
    pack = make_ingress_pack(model_name="IngressModel")
    await queue_pack(registry, pack)

    await registry._soft_delete_aem_pack(pack.id)

    claimed = await registry._incoming_aem_pack_queue.claim_next()
    assert claimed is None


async def test_claimed_aem_pack_deleted_before_processing_not_publish(
    joint_fixture: JointFixture,
):
    """Ensure that if an aem_pack is claimed for processing, then marked for deletion
    before processing finishes, the result is not published to the transformed aem-pack collection.
    """
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["single_route"]
    )
    aem_id = uuid4()

    # Queue and claim the pack
    pack = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
    claimed = await queue_and_claim(registry=registry, pack=pack)

    # Simulate deletion after claiming, but before processing
    await registry._soft_delete_aem_pack(aem_id)

    # Process the claimed pack, shouldn't publish
    await registry._process_next_aem_pack(
        incoming_aem=claimed,
        correlation_id=claimed.correlation_id,
    )

    # There should be no derived packs published for this pid
    derived = await joint_fixture.derived_packs(pack.pid)
    assert len(derived) == 0
