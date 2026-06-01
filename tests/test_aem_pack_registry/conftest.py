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

"""Shared fixtures for aem_pack_registry tests."""

import pytest_asyncio

from ets.core.aem_pack_registry import AEMPackRegistry
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture


@pytest_asyncio.fixture
async def registry(joint_fixture: JointFixture) -> AEMPackRegistry:
    """Seed the DB with the single_route config and return the AEM pack registry."""
    return await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["single_route"]
    )
