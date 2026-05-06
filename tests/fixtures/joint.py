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

"""A fixture that consolidates service components and test fixtures into one class"""

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import cast

import pytest_asyncio
from hexkit.providers.akafka import KafkaEventSubscriber
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb import ConfiguredMongoClient
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.mongokafka import MongoKafkaDaoPublisherFactory
from pymongo.asynchronous.collection import AsyncCollection

from ets.adapters.outbound.config_lock import ConfigLockAdapter
from ets.adapters.outbound.dao import (
    get_aem_pack_dao,
    get_persisted_model_dao,
    get_route_dao,
    get_workflow_dao,
)
from ets.config import Config
from ets.constants import (
    CONFIG_LOCK_COLLECTION,
    INCOMING_AEM_PACK_COLLECTION,
)
from ets.core.aem_pack_registry import AEMPackRegistry
from ets.inject import (
    prepare_aem_pack_registry,
    prepare_config_adapters,
    prepare_event_subscriber,
)
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_lock import ConfigLockPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.config_writer import ConfigWriterPort
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

    aem_pack_registry: AEMPackRegistry
    config: Config
    config_lock: ConfigLockPort
    daos: DAOs
    event_subscriber: KafkaEventSubscriber
    incoming_aem_pack_collection: AsyncCollection
    kafka: KafkaFixture
    loader: ConfigLoaderPort
    versioner: ConfigVersionerPort
    writer: ConfigWriterPort
    mongodb: MongoDbFixture


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    mongodb: MongoDbFixture, kafka: KafkaFixture
) -> AsyncGenerator[JointFixture]:
    """A fixture that embeds all other fixtures for integration testing."""
    # merge configs from different sources with the default one:
    config = get_config(sources=[mongodb.config, kafka.config], kafka_enable_dlq=True)
    model_dao = await get_persisted_model_dao(dao_factory=mongodb.dao_factory)
    route_dao = await get_route_dao(dao_factory=mongodb.dao_factory)
    workflow_dao = await get_workflow_dao(dao_factory=mongodb.dao_factory)

    async with (
        MongoKafkaDaoPublisherFactory.construct(config=config) as dao_pub_factory,
        ConfiguredMongoClient(config=config) as async_mongo_client,
    ):
        aem_pack_dao = await get_aem_pack_dao(
            dao_publisher_factory=dao_pub_factory,
            topic=config.derived_aem_pack_topic,
        )
        incoming_aem_pack_collection = async_mongo_client[config.db_name][
            INCOMING_AEM_PACK_COLLECTION
        ]
        config_lock_collection = async_mongo_client[config.db_name][
            CONFIG_LOCK_COLLECTION
        ]
        config_lock = ConfigLockAdapter(
            collection=config_lock_collection,
            worker_id=config.worker_id,
            lock_expiry_seconds=config.lock_expiry_seconds,
            poll_interval=config.lock_poll_interval,
            timeout=config.lock_timeout,
        )
        await config_lock.setup_index()
        daos = DAOs(
            aem_pack_dao=aem_pack_dao,
            model_dao=model_dao,
            route_dao=route_dao,
            workflow_dao=workflow_dao,
        )

        async with (
            prepare_aem_pack_registry(config=config) as aem_pack_registry,
            prepare_event_subscriber(
                config=config, core_override=aem_pack_registry
            ) as event_subscriber,
            prepare_config_adapters(config=config) as config_adapters,
        ):
            yield JointFixture(
                aem_pack_registry=cast(AEMPackRegistry, aem_pack_registry),
                config=config,
                config_lock=config_lock,
                daos=daos,
                event_subscriber=event_subscriber,
                incoming_aem_pack_collection=incoming_aem_pack_collection,
                kafka=kafka,
                loader=config_adapters.loader,
                versioner=config_adapters.version,
                writer=config_adapters.writer,
                mongodb=mongodb,
            )
