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

"""Tests for behavior when the config changes between processing runs."""

from uuid import uuid4

import pytest

from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.models import PersistedConfig
from tests.fixtures.aem_pack_registry import (
    collect_derived_packs,
    make_ingress_pack,
    populate_db_config,
    process_pack,
    queue_and_claim,
)
from tests.fixtures.examples import VALID_MODEL_DERIVATION_CONFIGS
from tests.fixtures.joint import JointFixture


@pytest.mark.asyncio
class TestConfigChanges:
    """Tests for behavior when the config changes between processing runs."""

    async def test_unreachable_pack_deleted_after_route_removal(
        self, joint_fixture: JointFixture
    ):
        """Removing a route causes previously derived packs to be deleted on re-processing."""
        config = await populate_db_config(
            joint_fixture.daos,
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            publish_models={"B", "C"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        aem_id = uuid4()

        # First processing: B and C derived
        ingress = make_ingress_pack("A", aem_id=aem_id)
        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived_v1 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, aem_id
        )
        assert len(derived_v1) == 2

        # Remove route B→C from config (C becomes unreachable)
        route_bc = next(r for r in config.routes if r.output_model_name == "C")
        new_config = PersistedConfig(
            models=[m for m in config.models if m.name != "C"],
            routes=[r for r in config.routes if r.name != route_bc.name],
            workflows=config.workflows,
        )

        # Re-process same ingress
        ingress_v2 = make_ingress_pack("A", aem_id=aem_id)
        claimed_v2 = await queue_and_claim(
            registry, ingress_v2, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed_v2, config=new_config)

        # C deleted (unreachable), B remains
        derived_v2 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, aem_id
        )
        assert len(derived_v2) == 1
        assert derived_v2[0].model_name == "B"
