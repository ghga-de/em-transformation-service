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

"""Tests for the config version tracker."""

import pytest
from hexkit.providers.mongodb import ConfiguredMongoClient
from hexkit.providers.mongodb.testutils import MongoDbFixture

from ets.adapters.outbound.config_version import ConfigVersioner
from ets.constants import CONFIG_VERSION_COLLECTION


def _make_version_tracker(collection) -> ConfigVersioner:
    return ConfigVersioner(collection=collection)


@pytest.mark.asyncio()
async def test_get_version_returns_zero_when_empty(mongodb: MongoDbFixture):
    """get_version returns 0 when no version document exists."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_VERSION_COLLECTION]
        tracker = _make_version_tracker(collection)

        assert await tracker.get_version() == 0


@pytest.mark.asyncio()
async def test_increment_from_zero(mongodb: MongoDbFixture):
    """First increment creates document with version 1."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_VERSION_COLLECTION]
        tracker = _make_version_tracker(collection)

        result = await tracker.increment_version()
        assert result == 1
        assert await tracker.get_version() == 1


@pytest.mark.asyncio()
async def test_increment_is_monotonic(mongodb: MongoDbFixture):
    """Multiple increments produce strictly increasing values."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_VERSION_COLLECTION]
        tracker = _make_version_tracker(collection)

        for expected in range(1, 6):
            result = await tracker.increment_version()
            assert result == expected


@pytest.mark.asyncio()
async def test_multiple_trackers_share_version(mongodb: MongoDbFixture):
    """Two tracker instances pointing at the same collection see the same version."""
    async with ConfiguredMongoClient(config=mongodb.config) as client:
        collection = client[mongodb.config.db_name][CONFIG_VERSION_COLLECTION]
        tracker_a = _make_version_tracker(collection)
        tracker_b = _make_version_tracker(collection)

        await tracker_a.increment_version()
        await tracker_a.increment_version()

        assert await tracker_b.get_version() == 2

        await tracker_b.increment_version()
        assert await tracker_a.get_version() == 3
