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
from pymongo.asynchronous.collection import AsyncCollection

from ets.adapters.outbound.config_version import ConfigVersioner
from ets.constants import CONFIG_VERSION_COLLECTION

pytestmark = [
    pytest.mark.asyncio(),
    pytest.mark.parametrize(
        "mongo_collection", [CONFIG_VERSION_COLLECTION], indirect=True
    ),
]


async def test_get_version_returns_zero_when_empty(mongo_collection: AsyncCollection):
    """get_version returns 0 when no version document exists."""
    assert await ConfigVersioner(collection=mongo_collection).get_version() == 0


async def test_increment_from_zero(mongo_collection: AsyncCollection):
    """First increment creates the document with version 1."""
    tracker = ConfigVersioner(collection=mongo_collection)
    assert await tracker.increment_version() == 1
    assert await tracker.get_version() == 1


async def test_increment_is_monotonic(mongo_collection: AsyncCollection):
    """Multiple increments produce strictly increasing values."""
    tracker = ConfigVersioner(collection=mongo_collection)
    for expected in range(1, 6):
        assert await tracker.increment_version() == expected


async def test_multiple_trackers_share_version(mongo_collection: AsyncCollection):
    """Two tracker instances pointing at the same collection see the same version."""
    tracker_a = ConfigVersioner(collection=mongo_collection)
    tracker_b = ConfigVersioner(collection=mongo_collection)

    await tracker_a.increment_version()
    await tracker_a.increment_version()
    assert await tracker_b.get_version() == 2

    await tracker_b.increment_version()
    assert await tracker_a.get_version() == 3
