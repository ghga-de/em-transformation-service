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

"""Tests that an unexpected per-pack failure is isolated from the processing loop and
bounded, instead of killing the worker or becoming a poison pill.
"""

from unittest.mock import AsyncMock, patch

import pytest

from emts.constants import ATTEMPTS_FIELD, FAILED_AT_FIELD, PROCESSED_AT_FIELD
from emts.core.aem_pack_registry import AEMPackRegistry
from tests.fixtures.aem_pack import make_ingress_pack, queue_pack
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


class _StopLoop(Exception):
    """Sentinel used to break out of the otherwise-infinite processing loop."""


async def test_unexpected_failure_is_isolated_and_bounded(
    registry: AEMPackRegistry,
    joint_fixture: JointFixture,
):
    """A non-DataDerivationError raised by processing must not propagate out of the
    loop. The pack is retried (freed in between) up to ``processing_max_attempts`` times
    and then parked as failed with a single FAILED event, after which nothing remains to
    claim.
    """
    max_attempts = registry._config.processing_max_attempts
    assert max_attempts >= 2, "test assumes at least one free-and-retry before the cap"

    pack = make_ingress_pack("IngressModel")
    await queue_pack(registry, pack)

    queue = registry._incoming_aem_pack_queue
    real_claim_next = queue.claim_next

    async def claim_or_stop():
        """Drive the real claim, but break the loop once the queue is drained."""
        claimed = await real_claim_next()
        if claimed is None:
            raise _StopLoop
        return claimed

    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.aem_pack_processing_status_topic
    ) as recorder:
        with (
            patch.object(
                registry,
                "_process_next_aem_pack",
                AsyncMock(side_effect=RuntimeError("boom")),
            ) as process_mock,
            patch.object(queue, "claim_next", side_effect=claim_or_stop),
            patch.object(queue, "free", wraps=queue.free) as free_spy,
            patch.object(
                queue, "mark_as_failed", wraps=queue.mark_as_failed
            ) as failed_spy,
        ):
            with pytest.raises(_StopLoop):
                await registry.process_aem_packs()

    # Reclaimed and re-run exactly max_attempts times (freed and picked up again) ...
    assert process_mock.await_count == max_attempts
    # ... freed for every attempt below the cap ...
    assert free_spy.await_count == max_attempts - 1
    # ... and parked as failed exactly once when the cap was reached.
    assert failed_spy.await_count == 1

    doc = await joint_fixture.incoming_doc(pack.id)
    assert doc is not None
    assert doc[ATTEMPTS_FIELD] == max_attempts
    # mark_as_failed records the failure and prevents re-claiming.
    assert doc[FAILED_AT_FIELD] is not None
    assert doc[PROCESSED_AT_FIELD] is not None

    failed_events = [
        e for e in recorder.recorded_events if e.payload["status"] == "failed"
    ]
    assert len(failed_events) == 1
    payload = failed_events[0].payload
    assert payload["pid"] == pack.pid
    assert payload["version"] == pack.version
    assert payload["error_type"] == "RuntimeError"
    assert "boom" in str(payload["error_message"])


async def test_superseded_pack_is_not_parked_on_failure(
    registry: AEMPackRegistry,
    joint_fixture: JointFixture,
):
    """If the claimed pack is superseded/terminal by the time it fails, the loop neither
    frees nor parks it: ``increment_attempts`` matches nothing and the handler is a no-op.
    """
    pack = make_ingress_pack("IngressModel")
    await queue_pack(registry, pack)

    queue = registry._incoming_aem_pack_queue
    real_claim_next = queue.claim_next
    claims = 0

    async def claim_then_supersede_then_stop():
        nonlocal claims
        claimed = await real_claim_next()
        if claimed is None:
            raise _StopLoop
        claims += 1
        # Simulate a newer version landing while in flight by bumping the stored version
        # out from under this claim, so the version-guarded handler matches nothing.
        await joint_fixture.incoming_aem_pack_collection.update_one(
            {"_id": claimed.id}, {"$set": {"version": claimed.version + 1}}
        )
        return claimed

    with (
        patch.object(
            registry,
            "_process_next_aem_pack",
            AsyncMock(side_effect=RuntimeError("boom")),
        ),
        patch.object(queue, "claim_next", side_effect=claim_then_supersede_then_stop),
        patch.object(queue, "free", wraps=queue.free) as free_spy,
        patch.object(queue, "mark_as_failed", wraps=queue.mark_as_failed) as failed_spy,
    ):
        with pytest.raises(_StopLoop):
            await registry.process_aem_packs()

    # The version no longer matches, so the pack is neither freed nor parked as failed.
    assert claims == 1
    assert free_spy.await_count == 0
    assert failed_spy.await_count == 0

    doc = await joint_fixture.incoming_doc(pack.id)
    assert doc is not None
    assert doc[FAILED_AT_FIELD] is None
    # attempts stays at 0: the guarded increment did not match the stale version.
    assert doc[ATTEMPTS_FIELD] == 0
