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

"""Tests for data-derivation failure handling and the published failure event."""

from unittest.mock import patch

import pytest
from metldata.workflow.exceptions import WorkflowExecutionError

from emts.ports.inbound.aem_pack_registry import DataDerivationError
from tests.fixtures.aem_pack import make_ingress_pack, process_pack, queue_and_claim
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


async def test_failed_derivation_publishes_event_and_marks_failed(
    joint_fixture: JointFixture,
):
    """A data-derivation failure publishes a single failure event, marks the pack
    failed, and publishes no (partial) derived packs.
    """
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    ingress = make_ingress_pack(model_name="IngressModel", version=3)

    workflow_error = WorkflowExecutionError(
        step_index=0, step_name="some_step", error=ValueError("transformation failed")
    )

    def _raise_data_derivation_error(*, incoming, dirty_map, transformed_map, config):
        raise DataDerivationError(
            pid=ingress.pid,
            model_name=ingress.model_name,
            error=workflow_error,
            transformation_step="some_step",
        )

    async with joint_fixture.kafka.record_events(
        in_topic=joint_fixture.config.aem_pack_processing_status_topic
    ) as recorder:
        with patch.object(registry, "_traverse_graph", _raise_data_derivation_error):
            await process_pack(registry, ingress)

    # Exactly one failure event, carrying the failure context. (A queued event is
    # also emitted on the same status topic when the pack is first queued.)
    failed_events = [
        e for e in recorder.recorded_events if e.payload["status"] == "failed"
    ]
    assert len(failed_events) == 1
    payload = failed_events[0].payload
    assert payload["pid"] == ingress.pid
    assert payload["model_name"] == ingress.model_name
    assert payload["version"] == 3
    assert payload["transformation_step"] == "some_step"
    assert payload["error_type"] == "WorkflowExecutionError"
    assert "transformation failed" in str(payload["error_message"])

    # The incoming pack is marked failed ...
    doc = await joint_fixture.incoming_doc(ingress.id)
    assert doc is not None
    assert doc["failed_at"] is not None

    # ... and nothing was published as a derived pack.
    assert await joint_fixture.derived_packs(ingress.pid) == []


async def test_config_change_clears_failed_and_requeues(joint_fixture: JointFixture):
    """A config change (mark_all_for_reprocessing) clears failed_at and flags the
    failed pack for reprocessing, so it is retried under the new config.
    """
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"]
    )
    ingress = make_ingress_pack(model_name="IngressModel")
    claimed = await queue_and_claim(registry=registry, pack=ingress)
    await registry._incoming_aem_pack_queue.mark_as_failed(
        claimed.id, claimed.version
    )

    # Precondition: the pack is parked as failed.
    doc = await joint_fixture.incoming_doc(ingress.id)
    assert doc is not None
    assert doc["failed_at"] is not None

    await registry._incoming_aem_pack_queue.mark_all_for_reprocessing()

    # The failure is cleared and the pack is queued for a fresh attempt.
    doc = await joint_fixture.incoming_doc(ingress.id)
    assert doc is not None
    assert doc["failed_at"] is None
    assert doc["needs_reprocessing"] is True
