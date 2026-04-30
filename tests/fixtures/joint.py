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
from pathlib import Path
from typing import cast

import pytest_asyncio
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb import ConfiguredMongoClient
from hexkit.providers.mongodb.testutils import MongoDbFixture
from hexkit.providers.mongokafka import MongoKafkaDaoPublisherFactory
from pydantic_settings import BaseSettings
from pymongo.asynchronous.collection import AsyncCollection

from ets.adapters.outbound.dao import (
    get_aem_pack_dao,
    get_persisted_model_dao,
    get_route_dao,
    get_workflow_dao,
)
from ets.config import Config
from ets.constants import INCOMING_AEM_PACK_COLLECTION
from ets.core.aem_pack_registry import AEMPackRegistry
from ets.inject import prepare_aem_pack_registry, prepare_config_adapters
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_writer import ConfigWriterPort
from ets.ports.outbound.dao import AEMPackDao, ModelDao, RouteDao, WorkflowDao
from tests.fixtures.config_examples import BASE_DIR

TEST_CONFIG_YAML = BASE_DIR / "test_config.yaml"


def get_config(
    sources: list[BaseSettings] | None = None,
    default_config_yaml: Path = TEST_CONFIG_YAML,
    **kwargs,
) -> Config:
    """Merge parameters from ``TEST_CONFIG_YAML`` with values from testcontainer fixtures."""
    sources_dict: dict[str, object] = {}
    if sources is not None:
        for source in sources:
            sources_dict.update(**source.model_dump())
    sources_dict.update(**kwargs)
    return Config(config_yaml=default_config_yaml, **sources_dict)  # type: ignore


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
    daos: DAOs
    incoming_aem_pack_collection: AsyncCollection
    kafka: KafkaFixture
    loader: ConfigLoaderPort
    writer: ConfigWriterPort


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    mongodb: MongoDbFixture, kafka: KafkaFixture
) -> AsyncGenerator[JointFixture]:
    """Embed all integration-test dependencies into a single fixture."""
    config = get_config(sources=[mongodb.config, kafka.config], kafka_enable_dlq=True)

    async with (
        prepare_aem_pack_registry(config=config) as aem_pack_registry,
        prepare_config_adapters(config=config) as config_adapters,
        MongoKafkaDaoPublisherFactory.construct(config=config) as dao_pub_factory,
        ConfiguredMongoClient(config=config) as mongo_client,
    ):
        daos = DAOs(
            aem_pack_dao=await get_aem_pack_dao(
                dao_publisher_factory=dao_pub_factory,
                topic=config.derived_aem_pack_topic,
            ),
            model_dao=await get_persisted_model_dao(dao_factory=mongodb.dao_factory),
            route_dao=await get_route_dao(dao_factory=mongodb.dao_factory),
            workflow_dao=await get_workflow_dao(dao_factory=mongodb.dao_factory),
        )
        yield JointFixture(
            aem_pack_registry=cast(AEMPackRegistry, aem_pack_registry),
            config=config,
            daos=daos,
            incoming_aem_pack_collection=mongo_client[config.db_name][
                INCOMING_AEM_PACK_COLLECTION
            ],
            kafka=kafka,
            loader=config_adapters.loader,
            writer=config_adapters.writer,
        )
