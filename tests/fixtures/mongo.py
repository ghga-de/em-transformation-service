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

"""Shared MongoDB fixtures for unit tests of single-collection adapters."""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from hexkit.providers.mongodb import ConfiguredMongoClient
from hexkit.providers.mongodb.testutils import MongoDbFixture
from pymongo.asynchronous.collection import AsyncCollection


@pytest_asyncio.fixture
async def mongo_collection(
    request: pytest.FixtureRequest, mongodb: MongoDbFixture
) -> AsyncGenerator[AsyncCollection]:
    """Yield a single MongoDB collection by name, passed via `indirect`.

    `request.param` holds the collection name.
    """
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        yield client[mongodb.config.db_name][request.param]
