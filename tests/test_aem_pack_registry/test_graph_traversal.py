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

"""Tests for _traverse_graph with various graph topologies."""

from uuid import uuid4

import pytest
from pydantic import UUID4

from ets.core.models import AEMPack, PersistedConfig
from tests.fixtures.aem_pack_registry import TEST_DATAPACK, load_aem_pack_config
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "aem_pack_config, dirty_names",
    [
        (AEM_PACK_REGISTRY_CONFIGS["single_route"], {"DerivedModel1"}),
        (
            AEM_PACK_REGISTRY_CONFIGS["single_route"],
            {"IngressModel", "DerivedModel1"},
        ),
        (
            AEM_PACK_REGISTRY_CONFIGS["forking_routes"],
            {"DerivedModel1", "DerivedModel2"},
        ),
        (
            AEM_PACK_REGISTRY_CONFIGS["chained_routes"],
            {"DerivedModel1", "DerivedModel3"},
        ),
    ],
    ids=[
        "single_route",
        "single_route_with_ingress",
        "forking_routes",
        "chained_routes",
    ],
    indirect=["aem_pack_config"],
)
async def test_clears_dirty_map(
    joint_fixture: JointFixture,
    aem_pack_config: PersistedConfig,
    dirty_names: set[str],
):
    """Ensure traversal with various graph topologies clears all non-dangling dirty map entries and no packs with publish=False are published."""
    ingress = next(model for model in aem_pack_config.models if model.is_ingress)
    incoming = AEMPack(
        id=uuid4(),
        model_name=ingress.name,
        original_id=None,
        data=TEST_DATAPACK,
        annotation={},
    )
    dirty_map: dict[str, UUID4] = {name: uuid4() for name in dirty_names}

    aem_packs_to_publish, remaining_dirty = (
        joint_fixture.aem_pack_registry._traverse_graph(
            incoming=incoming,
            dirty_map=dirty_map,
            transformed_map={incoming.model_name: incoming},
            config=aem_pack_config,
        )
    )

    for name in dirty_names:
        assert name not in remaining_dirty
    assert {pack.model_name for pack in aem_packs_to_publish} == {
        model.name for model in aem_pack_config.models if model.publish
    }


async def test_respects_topological_order(joint_fixture: JointFixture):
    """Test that routes are processed respecting topological order."""
    aem_pack_config = load_aem_pack_config(AEM_PACK_REGISTRY_CONFIGS["forking_routes"])
    models_by_name = {model.name: model for model in aem_pack_config.models}
    # Swap orders so DerivedModel2 (order=1) is processed before DerivedModel1 (order=2)
    models_by_name["DerivedModel2"].order = 1
    models_by_name["DerivedModel1"].order = 2

    ingress = next(model for model in aem_pack_config.models if model.is_ingress)
    incoming = AEMPack(
        id=uuid4(),
        model_name=ingress.name,
        original_id=None,
        data=TEST_DATAPACK,
        annotation={},
    )

    dirty_map: dict[str, UUID4] = {
        "DerivedModel1": uuid4(),
        "DerivedModel2": uuid4(),
    }

    aem_packs_to_publish, remaining_dirty = (
        joint_fixture.aem_pack_registry._traverse_graph(
            incoming=incoming,
            dirty_map=dirty_map,
            transformed_map={incoming.model_name: incoming},
            config=aem_pack_config,
        )
    )

    # Both branches were traversed regardless of order
    assert "DerivedModel1" not in remaining_dirty
    assert "DerivedModel2" not in remaining_dirty
    assert {pack.model_name for pack in aem_packs_to_publish} == {
        model.name for model in aem_pack_config.models if model.publish
    }


async def test_reuses_dirty_map_ids(joint_fixture: JointFixture):
    """Test that existing IDs from the dirty map are reused for derived packs."""
    aem_pack_config = load_aem_pack_config(
        AEM_PACK_REGISTRY_CONFIGS["forking_routes"],
        publish_models={"DerivedModel1", "DerivedModel2"},
    )
    ingress = next(model for model in aem_pack_config.models if model.is_ingress)
    incoming = AEMPack(
        id=uuid4(),
        model_name=ingress.name,
        original_id=None,
        data=TEST_DATAPACK,
        annotation={},
    )

    existing_id_1 = uuid4()
    existing_id_2 = uuid4()
    dirty_map: dict[str, UUID4] = {
        "DerivedModel1": existing_id_1,
        "DerivedModel2": existing_id_2,
    }

    published, _ = joint_fixture.aem_pack_registry._traverse_graph(
        incoming=incoming,
        dirty_map=dirty_map,
        transformed_map={incoming.model_name: incoming},
        config=aem_pack_config,
    )

    published_ids = {pack.id for pack in published}
    assert existing_id_1 in published_ids
    assert existing_id_2 in published_ids


async def test_generates_new_id_when_no_dirty_entry(joint_fixture: JointFixture):
    """Test that a new UUID is generated when there is no dirty map entry."""
    aem_pack_config = load_aem_pack_config(
        AEM_PACK_REGISTRY_CONFIGS["single_route"],
        publish_models={"DerivedModel1"},
    )
    ingress = next(model for model in aem_pack_config.models if model.is_ingress)
    incoming = AEMPack(
        id=uuid4(),
        model_name=ingress.name,
        original_id=None,
        data=TEST_DATAPACK,
        annotation={},
    )

    published, _ = joint_fixture.aem_pack_registry._traverse_graph(
        incoming=incoming,
        dirty_map={},
        transformed_map={incoming.model_name: incoming},
        config=aem_pack_config,
    )

    assert len(published) == 1
    derived = next(iter(published))
    # sanity check
    assert derived.id != incoming.id


@pytest.mark.parametrize(
    "aem_pack_config, ingress_name",
    [
        (
            (
                AEM_PACK_REGISTRY_CONFIGS["bottleneck"],
                {"DerivedModel1", "DerivedModel2"},
            ),
            "IngressModel1",
        ),
        (
            (
                AEM_PACK_REGISTRY_CONFIGS["bottleneck"],
                {"DerivedModel1", "DerivedModel2"},
            ),
            "IngressModel2",
        ),
    ],
    ids=["bottleneck_from_IngressModel1", "bottleneck_from_IngressModel2"],
    indirect=["aem_pack_config"],
)
async def test_bottleneck_topology(
    joint_fixture: JointFixture,
    aem_pack_config: PersistedConfig,
    ingress_name: str,
):
    """Ensure traversal from each ingress through a bottleneck node publishes downstream."""
    incoming = AEMPack(
        id=uuid4(),
        model_name=ingress_name,
        original_id=None,
        data=TEST_DATAPACK,
        annotation={},
    )
    existing_bottleneck = uuid4()
    existing_d1 = uuid4()
    existing_d2 = uuid4()
    dirty_map: dict[str, UUID4] = {
        "BottleneckModel": existing_bottleneck,
        "DerivedModel1": existing_d1,
        "DerivedModel2": existing_d2,
    }

    published, remaining_dirty = joint_fixture.aem_pack_registry._traverse_graph(
        incoming=incoming,
        dirty_map=dirty_map,
        transformed_map={incoming.model_name: incoming},
        config=aem_pack_config,
    )

    # All downstream dirty entries cleared
    assert "BottleneckModel" not in remaining_dirty
    assert "DerivedModel1" not in remaining_dirty
    assert "DerivedModel2" not in remaining_dirty

    assert {pack.model_name for pack in published} == {
        "DerivedModel1",
        "DerivedModel2",
    }

    # Existing dirty map IDs reused
    published_ids = {pack.id for pack in published}
    assert existing_d1 in published_ids
    assert existing_d2 in published_ids
