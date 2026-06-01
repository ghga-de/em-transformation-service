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

from tests.fixtures.aem_pack import make_ingress_pack, process_pack
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio()


async def test_reuses_derived_pack_ids(joint_fixture: JointFixture):
    """Ensure re-processing the same ingress pack reuses existing derived pack UUIDs."""
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    aem_id = uuid4()
    pid = str(uuid4())

    # First processing
    await process_pack(
        registry, make_ingress_pack(model_name="IngressModel", aem_id=aem_id, pid=pid)
    )
    derived_pack_names = {
        pack.model_name: pack.id for pack in await joint_fixture.derived_packs(pid)
    }

    # Second processing
    await process_pack(
        registry, make_ingress_pack(model_name="IngressModel", aem_id=aem_id, pid=pid)
    )
    derived = await joint_fixture.derived_packs(pid)
    re_derived_pack_names = {pack.model_name: pack.id for pack in derived}

    # Ensure IDs are reused across runs
    assert derived_pack_names["DerivedModel1"] == re_derived_pack_names["DerivedModel1"]
    assert derived_pack_names["DerivedModel2"] == re_derived_pack_names["DerivedModel2"]
    assert derived_pack_names["DerivedModel3"] == re_derived_pack_names["DerivedModel3"]

    # Annotations unchanged
    for pack in derived:
        assert pack.annotation == {}


async def test_first_processing_generates_fresh_ids(joint_fixture: JointFixture):
    """Ensure processing generates unique UUIDs for all derived packs."""
    registry = await joint_fixture.seeded_registry(
        AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
        publish_models={"DerivedModel1", "DerivedModel2", "DerivedModel3"},
    )
    ingress = make_ingress_pack("IngressModel")

    await process_pack(registry, ingress)

    derived = await joint_fixture.derived_packs(ingress.pid)
    all_ids = {pack.id for pack in derived}
    all_ids.add(ingress.id)
    assert len(all_ids) == 4
