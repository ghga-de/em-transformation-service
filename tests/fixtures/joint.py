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

from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest_asyncio
from hexkit.providers.akafka import KafkaEventSubscriber
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbFixture

from ets.adapters.outbound import dao
from ets.config import Config
from ets.inject import prepare_core, prepare_event_subscriber
from ets.ports.inbound.annotated_em_pack_registry import AnnotatedEMPackRegistryPort
from ets.ports.outbound.dao import AnnotatedEMPackDao
from tests.fixtures.config import get_config


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    mongodb: MongoDbFixture
    annotated_em_pack_registry: AnnotatedEMPackRegistryPort
    config: Config
    event_subscriber: KafkaEventSubscriber
    kafka: KafkaFixture
    annotated_em_pack_dao: AnnotatedEMPackDao


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    mongodb: MongoDbFixture, kafka: KafkaFixture
) -> AsyncGenerator[JointFixture]:
    """A fixture that embeds all other fixtures for integration testing."""
    # merge configs from different sources with the default one:
    config = get_config(sources=[mongodb.config, kafka.config], kafka_enable_dlq=True)
    annotated_em_pack_dao = await dao.get_annotated_em_pack_dao(
        dao_factory=mongodb.dao_factory,
    )

    async with (
        prepare_core(config=config) as annotated_em_pack_registry,
        prepare_event_subscriber(
            config=config, core_override=annotated_em_pack_registry
        ) as event_subscriber,
    ):
        yield JointFixture(
            mongodb=mongodb,
            annotated_em_pack_registry=annotated_em_pack_registry,
            config=config,
            event_subscriber=event_subscriber,
            kafka=kafka,
            annotated_em_pack_dao=annotated_em_pack_dao,
        )
