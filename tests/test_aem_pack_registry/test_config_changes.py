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
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture


@pytest.mark.asyncio
class TestConfigChanges:
    """Tests for behavior when the config changes between processing runs."""

    async def test_unreachable_pack_deleted_after_route_removal(
        self, joint_fixture: JointFixture
    ):
        """Removing a route causes previously derived packs to be deleted on re-processing."""
        config = await populate_db_config(
            daos=joint_fixture.daos,
            config_yaml_path=AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
            publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        aem_id = uuid4()

        # Derive all models first
        ingress = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
        unprocessed = await queue_and_claim(
            registry=registry,
            pack=ingress,
            service_instance_id=joint_fixture.config.service_instance_id,
        )
        await process_pack(registry=registry, incoming=unprocessed, config=config)

        derived = await collect_derived_packs(
            aem_pack_dao=joint_fixture.daos.aem_pack_dao, original_id=aem_id
        )
        assert len(derived) == 3

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
        ingress_v2 = make_ingress_pack(model_name="IngressModel", aem_id=aem_id)
        claimed_v2 = await queue_and_claim(
            registry=registry,
            pack=ingress_v2,
            service_instance_id=joint_fixture.config.service_instance_id,
        )
        await process_pack(registry=registry, incoming=claimed_v2, config=new_config)

        # DerivedModel3 deleted (unreachable), DerivedModel1 and DerivedModel2 remain
        derived_v2 = await collect_derived_packs(
            aem_pack_dao=joint_fixture.daos.aem_pack_dao, original_id=aem_id
        )
        assert len(derived_v2) == 2
        assert {pack.model_name for pack in derived_v2} == {
            "DerivedModel1",
            "DerivedModel2",
        }
