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

"""Tests for processing under config inconsistencies and mid-flight changes."""

import logging
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from ets.adapters.outbound.config_loader import ConfigLoaderAdapter
from ets.core.config_updater import ConfigUpdater
from ets.core.models import PersistedConfig
from tests.fixtures.aem_pack import (
    make_ingress_pack,
    queue_and_claim,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


async def test_unreachable_pack_deleted_after_route_removal(
    joint_fixture: JointFixture,
    caplog: pytest.LogCaptureFixture,
):
    """Ensure removing a route causes previously derived packs to be deleted on re-processing."""
    config = await joint_fixture.seed_config(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    registry = joint_fixture.aem_pack_registry
    aem_id = uuid4()
    pid = str(uuid4())

    # Derive all models first
    ingress = make_ingress_pack(model_name="IngressModel", aem_id=aem_id, pid=pid)
    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )
    await registry._process_next_aem_pack(
        incoming_aem=unprocessed,
        correlation_id=unprocessed.correlation_id,
    )

    derived = await joint_fixture.derived_packs(pid)
    assert len(derived) == 3
    deleted_pack_id = next(
        pack.id for pack in derived if pack.model_name == "DerivedModel3"
    )

    # Remove route DerivedModel2→DerivedModel3 from config
    new_config = PersistedConfig(
        models=[model for model in config.models if model.name != "DerivedModel3"],
        routes=[
            route
            for route in config.routes
            if route.output_model_name != "DerivedModel3"
        ],
        workflows=config.workflows,
    )

    # Re-process same ingress
    ingress = make_ingress_pack(model_name="IngressModel", aem_id=aem_id, pid=pid)
    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )
    # Inject the modified config directly — update_config won't overwrite it
    # because the DB version hasn't changed.
    config_updater = registry._config_updater
    assert isinstance(config_updater, ConfigUpdater)
    config_updater._current_config = new_config
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="ets.core.aem_pack_registry"):
        await registry._process_next_aem_pack(
            incoming_aem=unprocessed,
            correlation_id=unprocessed.correlation_id,
        )

    # DerivedModel3 deleted (unreachable), DerivedModel1 and DerivedModel2 remain
    derived = await joint_fixture.derived_packs(pid)
    assert len(derived) == 2
    assert {pack.model_name for pack in derived} == {
        "DerivedModel1",
        "DerivedModel2",
    }

    # "no longer exists" warning logged for removed model
    assert any(
        "no longer exists in the config" in record.message
        and str(deleted_pack_id) in record.message
        for record in caplog.records
    )


async def test_orphaned_pack_cleaned_up_when_model_still_exists(
    joint_fixture: JointFixture,
    caplog: pytest.LogCaptureFixture,
):
    """When a route is removed but the model remains in the config, the previously
    derived pack is deleted with a 'no longer reachable' warning.
    """
    config = await joint_fixture.seed_config(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    registry = joint_fixture.aem_pack_registry
    aem_id = uuid4()
    pid = str(uuid4())

    # Initial processing: derive all 3 packs
    ingress = make_ingress_pack(model_name="IngressModel", aem_id=aem_id, pid=pid)
    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )
    await registry._process_next_aem_pack(
        incoming_aem=unprocessed,
        correlation_id=unprocessed.correlation_id,
    )

    derived = await joint_fixture.derived_packs(pid)
    assert len(derived) == 3
    orphaned_pack = next(pack for pack in derived if pack.model_name == "DerivedModel3")

    # Remove route DerivedModel2 -> DerivedModel3 but keep DerivedModel3 as a model
    # Contrived example, as the corresponding model would be removed in normal processing
    new_config = PersistedConfig(
        models=config.models,
        routes=[
            route
            for route in config.routes
            if route.output_model_name != "DerivedModel3"
        ],
        workflows=config.workflows,
    )

    # Re-process same ingress with modified config
    ingress = make_ingress_pack(model_name="IngressModel", aem_id=aem_id, pid=pid)
    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )
    config_updater = registry._config_updater
    assert isinstance(config_updater, ConfigUpdater)
    config_updater._current_config = new_config
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        await registry._process_next_aem_pack(
            incoming_aem=unprocessed,
            correlation_id=unprocessed.correlation_id,
        )

    # DerivedModel3 pack deleted, DerivedModel1 and DerivedModel2 remain
    derived = await joint_fixture.derived_packs(pid)
    assert len(derived) == 2
    assert {pack.model_name for pack in derived} == {
        "DerivedModel1",
        "DerivedModel2",
    }

    # Correct warning logged (not the "no longer exists" variant)
    assert any(
        "no longer reachable from its previous original ID" in record.message
        and str(orphaned_pack.id) in record.message
        for record in caplog.records
    )


async def test_pack_freed_when_config_changes_mid_processing(
    joint_fixture: JointFixture,
    caplog: pytest.LogCaptureFixture,
):
    """Ensure an in-flight AEMPack is freed for reprocessing when the graph config changes."""
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["single_route"], publish_models={"DerivedModel1"}
    )
    aem_id = uuid4()
    pid = str(uuid4())

    ingress = make_ingress_pack(model_name="IngressModel", aem_id=aem_id, pid=pid)
    claimed = await queue_and_claim(registry=registry, pack=ingress)

    # Simulate a config change occurring mid-processing by bumping the version
    # the versioner reports on its second call.
    config_updater = registry._config_updater
    assert isinstance(config_updater, ConfigUpdater)
    await config_updater.update_config()
    version = config_updater.known_version
    with (
        patch.object(
            config_updater._versioner,
            "get_version",
            AsyncMock(side_effect=[version, version + 1]),
        ),
        caplog.at_level(logging.INFO, logger="ets.core.aem_pack_registry"),
    ):
        await registry._process_next_aem_pack(
            incoming_aem=claimed,
            correlation_id=claimed.correlation_id,
        )

    # No derived packs should have been published
    derived = await joint_fixture.derived_packs(pid)
    assert len(derived) == 0

    # The pack must be available to claim again (freed, not marked processed)
    reclaimed = await registry._incoming_aem_pack_queue.claim_next()
    assert reclaimed is not None
    assert reclaimed.id == aem_id

    assert any(
        "Graph config changed" in record.message and str(aem_id) in record.message
        for record in caplog.records
    )


async def test_nonexistent_model_raises_error_in_pipeline(
    joint_fixture: JointFixture, monkeypatch: pytest.MonkeyPatch
):
    """Ensure processing an AEMPack for a model not in the config raises ValueError."""
    config = await joint_fixture.seed_config(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"]
    )
    registry = joint_fixture.aem_pack_registry
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
    monkeypatch.setattr(
        ConfigLoaderAdapter,
        "load_config_from_db",
        AsyncMock(return_value=permissive_config),
    )
    unprocessed = await queue_and_claim(
        registry=registry,
        pack=ingress,
    )
    # Inject the original (non-permissive) config so processing sees NonExistent as missing
    config_updater = registry._config_updater
    assert isinstance(config_updater, ConfigUpdater)
    config_updater._current_config = config
    with pytest.raises(ValueError, match="No model with name NonExistent"):
        await registry._process_next_aem_pack(
            incoming_aem=unprocessed,
            correlation_id=unprocessed.correlation_id,
        )
