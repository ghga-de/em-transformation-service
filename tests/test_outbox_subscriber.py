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

# event sub plus db interaction -> test if kafka fixture plus mongodb fixtures,
# fire event with the payload does it reach the db, kafka f=ix send event,
# mongo fix check if it is in db in the right format
# model name has tp exist somewhere - will we check the db or will we check the config?


import pytest

from ets.adapters.inbound.event_sub import AnnotatedEMPackReceived
from tests.conftest import TEST_ANNOTATED_EM_PACK
from tests.fixtures.joint import JointFixture

CHANGE_EVENT_TYPE = "upserted"

pytestmark = pytest.mark.asyncio()


@pytest.mark.parametrize("annotated_em_pack", [TEST_ANNOTATED_EM_PACK])
async def test_annotated_em_pack_upsert(
    joint_fixture: JointFixture, annotated_em_pack: AnnotatedEMPackReceived
) -> None:
    """Ensure that the annotated EM pack upsert event is processed correctly.
    Please note that the validation of the data from AnnotatedEMPack and the validation
    of the model that it refers to are not implemented in the core yet.

    This test aims to verify that when an AnnotatedEMPackReceived event is published to the
    Kafka topic, the outbox subscriber receives the correct payload and inserts it to the db
    correctly.
    """
    # Publish the change event.
    await joint_fixture.kafka.publish_event(
        payload=annotated_em_pack.model_dump(),
        type_=CHANGE_EVENT_TYPE,
        topic=joint_fixture.config.annotated_em_pack_upsert_topic,
        key=str(annotated_em_pack.annotated_em_pack_id),
    )

    # Run the outbox subscriber.
    await joint_fixture.event_subscriber.run(forever=False)

    # Check that the annotated em pack data is found.
    result = await joint_fixture.annotated_em_pack_dao.get_by_id(
        annotated_em_pack.annotated_em_pack_id
    )
    assert result == annotated_em_pack
