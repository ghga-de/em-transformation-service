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

"""Tests for error conditions and edge cases."""

import pytest

from ets.core.aem_pack_registry import AEMPackRegistry
from tests.fixtures.aem_pack_registry import (
    make_ingress_pack,
    populate_db_config,
    process_pack,
    queue_and_claim,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture


@pytest.mark.asyncio
class TestErrorHandling:
    """Tests for error conditions and edge cases."""

    async def test_nonexistent_model_raises_error_in_pipeline(
        self, joint_fixture: JointFixture
    ):
        """Processing an AEMPack for a model not in the config raises ValueError."""
        config = await populate_db_config(
            daos=joint_fixture.daos,
            config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        ingress = make_ingress_pack("NonExistent")

        claimed = await queue_and_claim(
            registry=registry,
            pack=ingress,
            service_instance_id=joint_fixture.config.service_instance_id,
        )
        with pytest.raises(ValueError, match="No model with name NonExistent"):
            await process_pack(registry=registry, incoming=claimed, config=config)
