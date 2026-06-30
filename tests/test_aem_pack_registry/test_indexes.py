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

"""Tests for the secondary indexes backing the claim queries."""

import pytest

from emts.constants import (
    CLAIMED_AT_FIELD,
    NEEDS_REPROCESSING_FIELD,
    PROCESSED_AT_FIELD,
    TOMBSTONE_FIELD,
)
from emts.core.aem_pack_registry import AEMPackRegistry
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


async def test_ensure_indexes_creates_claim_indexes_and_is_idempotent(
    registry: AEMPackRegistry,
    joint_fixture: JointFixture,
):
    """
    Ensure create_claim_indexes creates the compound indexes covering the claim branches
    and is safe to call repeatedly.
    """
    queue = registry._incoming_aem_pack_queue
    await queue.create_claim_indexes()
    await queue.create_claim_indexes()

    info = await joint_fixture.incoming_aem_pack_collection.index_information()
    key_sets = {tuple(tuple(part) for part in spec["key"]) for spec in info.values()}

    # Branches 1 (fresh) and 2 (stale reclaim).
    assert (
        (PROCESSED_AT_FIELD, 1),
        (CLAIMED_AT_FIELD, 1),
        (TOMBSTONE_FIELD, 1),
    ) in key_sets
    # Branch 3 (reprocess).
    assert (
        (NEEDS_REPROCESSING_FIELD, 1),
        (PROCESSED_AT_FIELD, 1),
        (TOMBSTONE_FIELD, 1),
        (CLAIMED_AT_FIELD, 1),
    ) in key_sets
