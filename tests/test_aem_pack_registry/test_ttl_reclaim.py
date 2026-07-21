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

"""Tests for TTL-based claim expiry, reclamation priority, and the single-winner
guarantee that replaces worker-id-based reclamation.
"""

from datetime import UTC, datetime, timedelta

import pytest

from emts.constants import (
    CLAIMED_AT_FIELD,
    NEEDS_REPROCESSING_FIELD,
    PROCESSED_AT_FIELD,
)
from emts.core.aem_pack_registry import AEMPackRegistry
from tests.fixtures.aem_pack import make_ingress_pack, queue_and_claim, queue_pack
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


def _ago(seconds: int) -> datetime:
    """A timezone-aware UTC timestamp ``seconds`` in the past."""
    return datetime.now(UTC) - timedelta(seconds=seconds)


async def _set_claimed_at(
    joint_fixture: JointFixture, aem_id, seconds_ago: int
) -> None:
    """Force a doc's claim to look ``seconds_ago`` old."""
    await joint_fixture.incoming_aem_packs.update_one(
        {"_id": aem_id}, {"$set": {CLAIMED_AT_FIELD: _ago(seconds_ago)}}
    )


async def test_stale_claim_is_reclaimed(
    registry: AEMPackRegistry,
    joint_fixture: JointFixture,
):
    """Ensure a claim older than the TTL is reclaimed."""
    pack = make_ingress_pack("IngressModel")
    await queue_pack(registry, pack)
    # claim_ttl_seconds is 300 in the test config; 600s old is comfortably stale.
    await _set_claimed_at(joint_fixture, pack.id, 600)

    reclaimed = await registry._incoming_aem_pack_queue.claim_next()

    assert reclaimed is not None
    assert reclaimed.id == pack.id

    # The claim timestamp was refreshed, so an immediate re-poll does not reclaim again.
    assert await registry._incoming_aem_pack_queue.claim_next() is None


async def test_recent_claim_is_not_reclaimed(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure a claim still within the TTL is left alone."""
    pack = make_ingress_pack("IngressModel")
    await queue_pack(registry, pack)
    # Within the 300s TTL: not stale.
    await _set_claimed_at(joint_fixture, pack.id, 100)

    assert await registry._incoming_aem_pack_queue.claim_next() is None


async def test_claim_priority_fresh_then_stale_then_reprocess(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure claim_next prefers fresh packs, then stale ones, then those marked for reprocessing."""
    fresh = make_ingress_pack("IngressModel")
    stale = make_ingress_pack("IngressModel")
    reprocess = make_ingress_pack("IngressModel")
    for pack in (fresh, stale, reprocess):
        await queue_pack(registry, pack)

    coll = joint_fixture.incoming_aem_packs
    await coll.update_one({"_id": stale.id}, {"$set": {CLAIMED_AT_FIELD: _ago(600)}})
    await coll.update_one(
        {"_id": reprocess.id},
        {"$set": {PROCESSED_AT_FIELD: _ago(100), NEEDS_REPROCESSING_FIELD: True}},
    )

    queue = registry._incoming_aem_pack_queue
    first = await queue.claim_next()
    second = await queue.claim_next()
    third = await queue.claim_next()
    fourth = await queue.claim_next()

    assert first is not None and first.id == fresh.id
    assert second is not None and second.id == stale.id
    assert third is not None and third.id == reprocess.id
    assert fourth is None


async def test_stale_reclaim_picks_oldest_claim_first(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure that among stale packs, the oldest is reclaimed first."""
    older = make_ingress_pack("IngressModel")
    newer = make_ingress_pack("IngressModel")
    await queue_pack(registry, older)
    await queue_pack(registry, newer)
    await _set_claimed_at(joint_fixture, older.id, 700)
    await _set_claimed_at(joint_fixture, newer.id, 500)

    reclaimed = await registry._incoming_aem_pack_queue.claim_next()
    assert reclaimed is not None
    assert reclaimed.id == older.id


async def test_mark_processed_is_single_winner(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure only the first mark_processed wins and a concurrent reclaimer discards."""
    pack = make_ingress_pack("IngressModel")
    await queue_and_claim(registry=registry, pack=pack)
    queue = registry._incoming_aem_pack_queue

    await queue.mark_processed(pack.id, pack.version)
    first = await joint_fixture.incoming_doc(pack.id)
    assert first is not None
    assert first[PROCESSED_AT_FIELD] is not None
    assert first[CLAIMED_AT_FIELD] is None

    # A second, racing worker reaching the same point must not overwrite the result.
    await queue.mark_processed(pack.id, pack.version)
    second = await joint_fixture.incoming_doc(pack.id)
    assert second is not None
    assert second[PROCESSED_AT_FIELD] == first[PROCESSED_AT_FIELD]


async def test_extend_all_claims_compensates_every_in_flight_claim(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure a single call refreshes all in-flight claims, mirroring the lock-holder
    compensating every instance frozen while it held the lock.
    """
    first = make_ingress_pack("IngressModel")
    second = make_ingress_pack("IngressModel")
    await queue_and_claim(registry=registry, pack=first)
    await queue_and_claim(registry=registry, pack=second)
    queue = registry._incoming_aem_pack_queue

    # Both claims would be stale (350s > 300s TTL) ...
    await _set_claimed_at(joint_fixture, first.id, 350)
    await _set_claimed_at(joint_fixture, second.id, 350)
    # ... one broadcast pulls both back inside the TTL.
    await queue.extend_all_claims(120)

    assert await queue.claim_next() is None


async def test_extend_all_claims_noop_on_processed_pack(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure extend_all_claims never resurrects a processed pack's claim."""
    pack = make_ingress_pack("IngressModel")
    await queue_and_claim(registry=registry, pack=pack)
    queue = registry._incoming_aem_pack_queue
    await queue.mark_processed(pack.id, pack.version)

    await queue.extend_all_claims(120)

    doc = await joint_fixture.incoming_doc(pack.id)
    assert doc is not None
    assert doc[CLAIMED_AT_FIELD] is None
