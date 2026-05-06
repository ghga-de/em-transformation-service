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

from typing import cast
from unittest.mock import AsyncMock

import pytest

from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.config_manager import ConfigManager
from tests.fixtures.aem_pack_registry import (
    make_ingress_pack,
    populate_db_config,
    queue_and_claim,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio


async def test_nonexistent_model_raises_error_in_pipeline(
    joint_fixture: JointFixture, monkeypatch: pytest.MonkeyPatch
):
    """Ensure processing an AEMPack for a model not in the config raises ValueError."""
    config = await populate_db_config(
        daos=joint_fixture.daos,
        config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
    )
    registry: AEMPackRegistry = joint_fixture.aem_pack_registry
    ingress = make_ingress_pack("NonExistent")

    # Bypass queue_unprocessed's model_name + schema validation so the pack reaches the claim step
    permissive_config = config.model_copy(
        update={
            "models": [
                *config.models,
                config.models[0].model_copy(update={"name": "NonExistent"}),
            ]
        }
    )
    config_manager = cast(ConfigManager, registry._config_manager)
    monkeypatch.setattr(
        config_manager._config_loader,
        "load_config_from_db",
        AsyncMock(return_value=permissive_config),
    )
    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )
    # Inject the original (non-permissive) config so processing sees NonExistent as missing
    config_manager._current_config = config
    with pytest.raises(ValueError, match="No model with name NonExistent"):
        await registry._process_next_aem_pack(
            incoming_aem=unprocessed,
            correlation_id=unprocessed.correlation_id,
        )
