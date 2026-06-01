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

"""Test for consuming an annotated em pack deletion event from the queue"""

from uuid import uuid4

import pytest
from hexkit.correlation import set_correlation_id

from ets.constants import TOMBSTONE_FIELD
from ets.core.aem_pack_registry import AEMPackRegistry
from tests.fixtures.aem_pack import make_ingress_pack, process_pack, queue_pack
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


async def test_mark_for_deletion_sets_tombstone(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure mark_for_deletion sets the tombstone field."""
    pack = make_ingress_pack(model_name="IngressModel")
    await queue_pack(registry, pack)

    await registry._soft_delete_aem_pack(pack.id)

    raw = await joint_fixture.incoming_aem_pack_collection.find_one(
        {"_id": pack.id, TOMBSTONE_FIELD: True}
    )
    assert raw is not None


async def test_hard_delete_removes_marked_document(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure hard delete removes the document from the collection."""
    pack = make_ingress_pack(model_name="IngressModel")
    await queue_pack(registry, pack)

    await registry._soft_delete_aem_pack(pack.id)
    await registry._hard_delete_aem_pack(pack.id)

    assert await joint_fixture.incoming_doc(pack.id) is None


async def test_hard_delete_only_deletes_marked(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure hard delete only deletes documents marked for deletion."""
    pack_1 = make_ingress_pack(model_name="IngressModel")
    pack_2 = make_ingress_pack(model_name="IngressModel")
    await queue_pack(registry, pack_1)
    await queue_pack(registry, pack_2)

    await registry._soft_delete_aem_pack(pack_1.id)
    await registry._hard_delete_aem_pack(pack_1.id)
    await registry._hard_delete_aem_pack(pack_2.id)

    assert await joint_fixture.incoming_doc(pack_1.id) is None
    assert await joint_fixture.incoming_doc(pack_2.id) is not None


async def test_delete_nonexistent_pack_is_idempotent(
    registry: AEMPackRegistry, joint_fixture: JointFixture
):
    """Ensure delete_aem_packs does not raise when the pack was never queued."""
    aem_id = uuid4()

    await registry.delete_aem_pack_and_descendants(incoming_aem_id=aem_id)

    assert await joint_fixture.incoming_doc(aem_id) is None


async def test_published_aem_packs_deleted_after_processing(
    joint_fixture: JointFixture,
):
    """Test that published AEM packs derived from an incoming pack are deleted when the incoming pack is deleted."""
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["single_route"]
    )
    aem_id = uuid4()

    # pid=str(aem_id) ensures delete_aem_packs can find derived packs via str(incoming_aem_id)
    pack = make_ingress_pack(model_name="IngressModel", aem_id=aem_id, pid=str(aem_id))
    claimed = await process_pack(registry, pack)
    assert len(await joint_fixture.derived_packs(pack.pid)) == 1

    async with set_correlation_id(claimed.correlation_id):
        await registry.delete_aem_pack_and_descendants(incoming_aem_id=aem_id)

    assert len(await joint_fixture.derived_packs(pack.pid)) == 0
    assert await joint_fixture.incoming_doc(aem_id) is None
