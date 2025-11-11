# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Tests for core functionality via API calls."""

import pytest
from hexkit.providers.akafka.testutils import (
    EventRecorder,
    ExpectedEvent,
    check_recorded_events,
)

from ets.core.models import DerivedEM
from tests.fixtures.joint import DATA_COLLECTION, WORKFLOW_COLLECTION, JointFixture

pytestmark = pytest.mark.asyncio()


async def test_health_check(joint_fixture: JointFixture):
    """Test that the health check endpoint works."""
    response = await joint_fixture.rest_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "OK"}


async def test_api_calls(joint_fixture: JointFixture):
    """Test functionality with incoming API call."""
    # populate the workflow and data collections
    await joint_fixture.workflow_dao.insert(WORKFLOW_COLLECTION)
    await joint_fixture.data_dao.insert(DATA_COLLECTION)

    # Event recorder is a test utility from the hexkit. Test spy/observer.
    # It acts as a kafka consumer that subscribes to a specific topic,
    # records events published during the test, lets you assert the events match
    # your expectations. It is a part of test infrastructure.
    event_recorder = EventRecorder(
        kafka_servers=joint_fixture.kafka.config.kafka_servers,
        topic=joint_fixture.config.my_topic,
    )
    workflow_id = "workflow-123"
    async with event_recorder:
        response = await joint_fixture.rest_client.get(f"/data/{workflow_id}")
    assert response.status_code == 200
    assert len(event_recorder.recorded_events) == 1

    expected_payload = DerivedEM(
        data="datapack: 4.0.0", dummy_field="dummy", my_id="workflow-123"
    )
    expected_event = ExpectedEvent(
        payload=expected_payload.model_dump(), type_=joint_fixture.config.my_type
    )
    check_recorded_events(
        recorded_events=event_recorder.recorded_events,
        expected_events=[expected_event],
    )
