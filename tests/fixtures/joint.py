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

"""A fixture that bundles service components and test dependencies into one class."""

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest_asyncio
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbFixture
from pydantic_settings import BaseSettings
from pymongo.asynchronous.collection import AsyncCollection

from ets.config import Config
from ets.constants import INCOMING_AEM_PACK_COLLECTION
from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.models import AEMPack, PersistedConfig
from ets.inject import prepare_wiring
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_writer import ConfigWriterPort
from ets.ports.outbound.dao import AEMPackDao, ModelDao, RouteDao, WorkflowDao
from tests.fixtures.examples import BASE_DIR, load_aem_pack_config

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
    """Wrapper class to hold all DAOs needed for testing."""

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

    async def seed_config(
        self, config_yaml_path: Path, publish_models: set[str] | None = None
    ) -> PersistedConfig:
        """Derive a config from a YAML fixture and write it to the DB.

        Returns the PersistedConfig matching what ``load_config_from_db`` returns.
        """
        config = load_aem_pack_config(config_yaml_path, publish_models=publish_models)
        for model in config.models:
            await self.daos.model_dao.insert(model)
        for route in config.routes:
            await self.daos.route_dao.insert(route)
        for workflow in config.workflows:
            await self.daos.workflow_dao.insert(workflow)
        return config

    async def seeded_registry(
        self, config_yaml_path: Path, publish_models: set[str] | None = None
    ) -> AEMPackRegistry:
        """Seed the DB with a config and return the registry ready to process packs."""
        await self.seed_config(config_yaml_path, publish_models=publish_models)
        return self.aem_pack_registry

    async def derived_packs(self, pid: str) -> list[AEMPack]:
        """All derived/published AEMPacks for a given originating pid."""
        return [
            pack async for pack in self.daos.aem_pack_dao.find_all(mapping={"pid": pid})
        ]

    async def incoming_doc(self, aem_id: UUID) -> dict[str, Any] | None:
        """Raw incoming-AEMPack document by id, or None if absent."""
        return await self.incoming_aem_pack_collection.find_one({"_id": aem_id})


@pytest_asyncio.fixture(scope="function")
async def joint_fixture(
    mongodb: MongoDbFixture, kafka: KafkaFixture
) -> AsyncGenerator[JointFixture]:
    """Embed all integration-test dependencies into a single fixture.

    Delegates the entire wiring to ``prepare_wiring`` — one Mongo client and
    one Kafka publisher cover the registry, DAOs, loader, writer, and the
    collection accessed directly by queue/claim assertions.
    """
    config = get_config(sources=[mongodb.config, kafka.config], kafka_enable_dlq=True)
    async with prepare_wiring(config=config) as wiring:
        yield JointFixture(
            aem_pack_registry=wiring.aem_pack_registry,
            config=config,
            daos=DAOs(
                aem_pack_dao=wiring.aem_pack_dao,
                model_dao=wiring.model_dao,
                route_dao=wiring.route_dao,
                workflow_dao=wiring.workflow_dao,
            ),
            incoming_aem_pack_collection=wiring.mongo_client[config.db_name][
                INCOMING_AEM_PACK_COLLECTION
            ],
            kafka=kafka,
            loader=wiring.loader,
            writer=wiring.writer,
        )
