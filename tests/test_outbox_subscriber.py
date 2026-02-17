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

# event sub plus db interaction -> test if kafka fixture plus mongodb fixtures,
# fire event with the payload does it reach the db, kafka f=ix send event,
# mongo fix check if it is in db in the right format
# model name has tp exist somewhere - will we check the db or will we check the config?

"""Verify functionality related to the consumption of KafkaOutbox events."""

import json

import pytest

from ets.adapters.inbound.event_sub import AEMPack
from tests.conftest import TEST_AEM_PACK
from tests.fixtures.joint import JointFixture

CHANGE_EVENT_TYPE = "upserted"

pytestmark = pytest.mark.asyncio()


@pytest.mark.parametrize("aem_pack_payload", [TEST_AEM_PACK])
async def test_aem_pack_upsert(
    joint_fixture: JointFixture, aem_pack_payload: AEMPack
) -> None:
    """Ensure that the AEMPack upsert event is processed correctly.
    Please note that the validation of the data from AEMPack and the validation
    of the model that it refers to are not implemented in the core yet.

    This test aims to verify that when an AEMPackPayload event is published to the
    Kafka topic, the outbox subscriber receives the correct payload and inserts it to the db
    correctly.
    """
    # Publish the change event.
    payload = json.loads(aem_pack_payload.model_dump_json())
    await joint_fixture.kafka.publish_event(
        payload=payload,
        type_=CHANGE_EVENT_TYPE,
        topic=joint_fixture.config.aem_pack_upsert_topic,
        key=str(aem_pack_payload.id),
    )

    # Run the outbox subscriber.
    await joint_fixture.event_subscriber.run(forever=False)

    # Check that the AEMPack data is found in the database.
    result = await joint_fixture.daos.aem_pack_dao.get_by_id(aem_pack_payload.id)
    expected = AEMPack(
        id=aem_pack_payload.id,
        model_name=aem_pack_payload.model_name,
        original_id=aem_pack_payload.original_id,
        data=aem_pack_payload.data,
        annotation=aem_pack_payload.annotation,
    )
    assert result == expected
