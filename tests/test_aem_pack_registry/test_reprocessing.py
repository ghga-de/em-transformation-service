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

"""Tests for re-processing the same ingress pack and ID reuse behavior."""

from uuid import uuid4

import pytest

from ets.core.aem_pack_registry import AEMPackRegistry
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
class TestReprocessing:
    """Tests for re-processing the same ingress pack and ID reuse behavior."""

    async def test_reuses_derived_pack_ids(self, joint_fixture: JointFixture):
        """Re-processing the same ingress pack reuses existing derived pack UUIDs."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
            publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        aem_id = uuid4()

        # First processing
        ingress_v1 = make_ingress_pack("IngressModel", aem_id=aem_id)
        claimed_v1 = await queue_and_claim(
            registry, ingress_v1, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed_v1, config=config)

        derived_v1 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, aem_id
        )
        ids_v1 = {pack.model_name: pack.id for pack in derived_v1}

        # Second processing
        ingress_v2 = make_ingress_pack("IngressModel", aem_id=aem_id)
        claimed_v2 = await queue_and_claim(
            registry, ingress_v2, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed_v2, config=config)

        derived_v2 = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, aem_id
        )
        ids_v2 = {pack.model_name: pack.id for pack in derived_v2}

        # IDs reused across runs
        assert ids_v1["DerivedModel1"] == ids_v2["DerivedModel1"]
        assert ids_v1["DerivedModel2"] == ids_v2["DerivedModel2"]
        assert ids_v1["DerivedModel3"] == ids_v2["DerivedModel3"]

        # Annotations unchanged
        for pack in derived_v2:
            assert pack.annotation == {}

    async def test_first_processing_generates_fresh_ids(
        self, joint_fixture: JointFixture
    ):
        """First processing generates unique UUIDs for all derived packs."""
        config = await populate_db_config(
            joint_fixture.daos,
            AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
            publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
        )
        registry: AEMPackRegistry = joint_fixture.aem_pack_registry
        ingress = make_ingress_pack("IngressModel")

        claimed = await queue_and_claim(
            registry, ingress, joint_fixture.config.service_instance_id
        )
        await process_pack(registry, incoming=claimed, config=config)

        derived = await collect_derived_packs(
            joint_fixture.daos.aem_pack_dao, ingress.id
        )
        all_ids = {pack.id for pack in derived}
        all_ids.add(ingress.id)
        assert (
            len(all_ids) == 4
        )  # ingress ID + DerivedModel1 ID + DerivedModel2 ID + DerivedModel3 ID, all distinct
