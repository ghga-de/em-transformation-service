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

"""Tests for the AEMPack processing-status (lifecycle) events on the happy path."""

import pytest

from tests.fixtures.aem_pack import make_ingress_pack, process_pack, queue_pack
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


async def test_happy_path_publishes_queued_and_processed_events(
    joint_fixture: JointFixture,
):
    """A successfully derived AEMPack emits a QUEUED event when it enters the queue
    and a PROCESSED event when it reaches the processed state, both on the status
    topic and carrying the (pid, model_name, version) of the incoming pack.
    """
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"], publish_models={"DerivedModel3"}
    )
    ingress = make_ingress_pack(model_name="IngressModel", version=5)

    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.aem_pack_processing_status_topic
    ) as recorder:
        await process_pack(registry, ingress)

    by_status = {e.payload["status"]: e.payload for e in recorder.recorded_events}

    # Exactly the two final states, no failure event.
    assert set(by_status) == {"queued", "processed"}

    for status in ("queued", "processed"):
        payload = by_status[status]
        assert payload["pid"] == ingress.pid
        assert payload["model_name"] == ingress.model_name
        assert payload["version"] == 5
        # Error context is reserved for FAILED events.
        assert payload["transformation_step"] is None
        assert payload["error_type"] is None
        assert payload["error_message"] is None


async def test_rejected_republish_emits_no_queued_event(joint_fixture: JointFixture):
    """A republish that is not a strictly newer version is rejected by the queue and
    must not emit a QUEUED status event.
    """
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"], publish_models={"DerivedModel3"}
    )
    ingress = make_ingress_pack(model_name="IngressModel", version=2)
    # First queue stores the pack (newer than nothing).
    await process_pack(registry, ingress)

    # A re-publish at the same version is stale and should be rejected.
    stale = make_ingress_pack(
        model_name="IngressModel", aem_id=ingress.id, pid=ingress.pid, version=2
    )
    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.aem_pack_processing_status_topic
    ) as recorder:
        await queue_pack(registry, stale)

    assert recorder.recorded_events == []
