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

"""Joint fixture for tests."""

from collections.abc import AsyncGenerator
from dataclasses import dataclass

import httpx
import pytest_asyncio
from ghga_service_commons.api.testing import AsyncTestClient
from hexkit.providers.akafka.testutils import KafkaFixture
from hexkit.providers.mongodb.testutils import MongoDbFixture

from ets.adapters.outbound.dao import get_data_dao, get_workflow_dao
from ets.config import Config
from ets.core.models import DataDto, DerivedEM, WorkflowDto
from ets.inject import prepare_rest_app
from ets.ports.outbound.dao import DataDaoPort, WorkflowDaoPort
from tests.fixtures.config import get_config

TEST_PAYLOAD = DerivedEM(
    data="datapack: 4.0.0", dummy_field="dummy", my_id="workflow-123"
)

WORKFLOW_COLLECTION = WorkflowDto(
    workflow_id="workflow-123", workflow="workflow representation"
)

DATA_COLLECTION = DataDto(
    data="datapack: 4.0.0", model="model-123", workflow_id="workflow-123"
)


@dataclass
class JointFixture:
    """Holds configured container"""

    config: Config
    kafka: KafkaFixture
    mongodb: MongoDbFixture
    rest_client: httpx.AsyncClient
    workflow_dao: WorkflowDaoPort
    data_dao: DataDaoPort


@pytest_asyncio.fixture
async def joint_fixture(
    kafka: KafkaFixture, mongodb: MongoDbFixture
) -> AsyncGenerator[JointFixture]:
    """Setup container with updated config"""
    config = get_config(sources=[kafka.config, mongodb.config])

    async with (
        prepare_rest_app(config=config) as app,
        AsyncTestClient(app=app) as rest_client,
    ):
        workflow_dao = await get_workflow_dao(dao_factory=mongodb.dao_factory)
        data_dao = await get_data_dao(dao_factory=mongodb.dao_factory)
        yield JointFixture(
            config=config,
            kafka=kafka,
            mongodb=mongodb,
            rest_client=rest_client,
            workflow_dao=workflow_dao,
            data_dao=data_dao,
        )
