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

"""A fixture that consolidates service components and test fixtures into one class"""

from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest_asyncio
from hexkit.providers.akafka import KafkaEventSubscriber
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbFixture

from ets.adapters.outbound.dao import (
    get_aem_pack_dao,
    get_model_dao,
    get_route_dao,
    get_workflow_dao,
)
from ets.config import Config
from ets.inject import prepare_config_manager, prepare_core, prepare_event_subscriber
from ets.ports.inbound.aem_pack_registry import AEMPackRegistryPort
from ets.ports.inbound.config_manager import ConfigManagerPort
from ets.ports.outbound.dao import AEMPackDao, ModelDao, RouteDao, WorkflowDao
from tests.fixtures.config import get_config


@dataclass
class DAOs:
    """Wrapper class to hold all DAOs needed for testing"""

    aem_pack_dao: AEMPackDao
    model_dao: ModelDao
    route_dao: RouteDao
    workflow_dao: WorkflowDao


@dataclass
class JointFixture:
    """Returned by the `joint_fixture`."""

    aem_pack_registry: AEMPackRegistryPort
    config: Config
    config_manager: ConfigManagerPort
    daos: DAOs
    event_subscriber: KafkaEventSubscriber
    kafka: KafkaFixture
    mongodb: MongoDbFixture


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    mongodb: MongoDbFixture, kafka: KafkaFixture
) -> AsyncGenerator[JointFixture]:
    """A fixture that embeds all other fixtures for integration testing."""
    # merge configs from different sources with the default one:
    config = get_config(sources=[mongodb.config, kafka.config], kafka_enable_dlq=True)
    aem_pack_dao = await get_aem_pack_dao(
        dao_factory=mongodb.dao_factory,
    )
    model_dao = await get_model_dao(dao_factory=mongodb.dao_factory)
    route_dao = await get_route_dao(dao_factory=mongodb.dao_factory)
    workflow_dao = await get_workflow_dao(dao_factory=mongodb.dao_factory)
    daos = DAOs(
        aem_pack_dao=aem_pack_dao,
        model_dao=model_dao,
        route_dao=route_dao,
        workflow_dao=workflow_dao,
    )

    async with (
        prepare_core(config=config) as aem_pack_registry,
        prepare_event_subscriber(
            config=config, core_override=aem_pack_registry
        ) as event_subscriber,
        prepare_config_manager(config=config) as config_manager,
    ):
        yield JointFixture(
            aem_pack_registry=aem_pack_registry,
            daos=daos,
            config=config,
            config_manager=config_manager,
            event_subscriber=event_subscriber,
            kafka=kafka,
            mongodb=mongodb,
        )
