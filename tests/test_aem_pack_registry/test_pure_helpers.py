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

"""Tests for the pure in-memory methods of AEMPackRegistry.

These tests run against a ``mock_registry`` (collaborators are mocks) and do
not need MongoDB or Kafka. Covered methods:

* ``_create_aem_pack`` and ``_apply_workflow_to_data`` — wrapper helpers
* ``_traverse_graph`` — the routing/publishing core, exercised with a range of
  graph topologies
"""

from uuid import uuid4

import pytest
from pydantic import UUID4
from schemapack.spec.datapack import DataPack

from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.models import AEMPack, PersistedConfig
from tests.fixtures.aem_pack import (
    EXPECTED_AEM_ID,
    TEST_DATAPACK,
    load_aem_pack_config,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS


def _ingress_for(config: PersistedConfig, name: str | None = None) -> AEMPack:
    """Build an ingress AEMPack (defaults to the configured ingress model)."""
    model_name = name or next(model.name for model in config.models if model.is_ingress)
    return AEMPack(
        id=uuid4(),
        model_name=model_name,
        pid="test-pid",
        data=TEST_DATAPACK,
        annotation={},
    )


# --- _create_aem_pack / _apply_workflow_to_data ------------------------------


@pytest.mark.parametrize(
    "aem_id,expected_aem_id",
    [(None, False), (EXPECTED_AEM_ID, True)],
    ids=["auto_id", "specified_id"],
)
def test_create_aem_pack(
    mock_registry: AEMPackRegistry, aem_id: UUID4 | None, expected_aem_id: bool
):
    """Creating an AEMPack wrapper, with and without a pre-specified ID."""
    aem_pack = mock_registry._create_aem_pack(
        aem_id=aem_id,
        model_name="TestModel",
        pid="test-pid",
        data=TEST_DATAPACK,
        annotation={},
    )

    assert aem_pack.id is not None
    assert aem_pack.model_name == "TestModel"
    assert aem_pack.pid == "test-pid"
    assert aem_pack.data == TEST_DATAPACK
    assert aem_pack.annotation == {}
    if expected_aem_id:
        assert aem_pack.id == EXPECTED_AEM_ID


def test_apply_workflow_to_data(mock_registry: AEMPackRegistry):
    """Applying a workflow transforms only the schema, not the resource data."""
    config = load_aem_pack_config(AEM_PACK_REGISTRY_CONFIGS["single_route"])
    ingress = next(model for model in config.models if model.is_ingress)

    result = mock_registry._apply_workflow_to_data(
        data=TEST_DATAPACK,
        annotation={},
        input_schema=ingress.schema_,
        workflow=config.workflows[0],
    )

    assert isinstance(result, DataPack)
    # rename_id_property only modifies the schema, not the resource data.
    assert result.resources == TEST_DATAPACK.resources


# --- _traverse_graph ---------------------------------------------------------


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
def test_clears_dirty_map(
    mock_registry: AEMPackRegistry,
    aem_pack_config: PersistedConfig,
    dirty_names: set[str],
):
    """Traversal clears all non-dangling dirty map entries and only publishes
    derived packs whose model has ``publish=True``.
    """
    incoming = _ingress_for(aem_pack_config)
    dirty_map: dict[str, UUID4] = {name: uuid4() for name in dirty_names}

    aem_packs_to_publish, remaining_dirty = mock_registry._traverse_graph(
        incoming=incoming,
        dirty_map=dirty_map,
        transformed_map={incoming.model_name: incoming},
        config=aem_pack_config,
    )

    assert dirty_names.isdisjoint(remaining_dirty)
    assert {pack.model_name for pack in aem_packs_to_publish} == {
        model.name for model in aem_pack_config.models if model.publish
    }


def test_respects_topological_order(mock_registry: AEMPackRegistry):
    """Routes are processed in topological order regardless of fixture listing order."""
    config = load_aem_pack_config(AEM_PACK_REGISTRY_CONFIGS["forking_routes"])
    # Swap orders so DerivedModel2 (order=1) is processed before DerivedModel1 (order=2)
    by_name = {model.name: model for model in config.models}
    by_name["DerivedModel2"].order = 1
    by_name["DerivedModel1"].order = 2

    incoming = _ingress_for(config)
    dirty_map: dict[str, UUID4] = {
        "DerivedModel1": uuid4(),
        "DerivedModel2": uuid4(),
    }

    aem_packs_to_publish, remaining_dirty = mock_registry._traverse_graph(
        incoming=incoming,
        dirty_map=dirty_map,
        transformed_map={incoming.model_name: incoming},
        config=config,
    )

    assert {"DerivedModel1", "DerivedModel2"}.isdisjoint(remaining_dirty)
    assert {pack.model_name for pack in aem_packs_to_publish} == {
        model.name for model in config.models if model.publish
    }


@pytest.mark.parametrize(
    "config_name, publish, have_dirty_map, expected_published",
    [
        ("forking_routes", {"DerivedModel1", "DerivedModel2"}, True, 2),
        ("single_route", {"DerivedModel1"}, False, 1),
    ],
    ids=["reuses_dirty_map_ids", "generates_new_id_when_no_dirty_entry"],
)
def test_dirty_map_id_handling(
    mock_registry: AEMPackRegistry,
    config_name: str,
    publish: set[str],
    have_dirty_map: bool,
    expected_published: int,
):
    """With a dirty map, derived packs reuse the supplied UUIDs; without one,
    fresh UUIDs are generated for each derived pack.
    """
    config = load_aem_pack_config(
        AEM_PACK_REGISTRY_CONFIGS[config_name], publish_models=publish
    )
    incoming = _ingress_for(config)
    dirty_map: dict[str, UUID4] = (
        {name: uuid4() for name in publish} if have_dirty_map else {}
    )

    published, _ = mock_registry._traverse_graph(
        incoming=incoming,
        # _traverse_graph pops from this dict; copy so post-call assertions can read it.
        dirty_map=dict(dirty_map),
        transformed_map={incoming.model_name: incoming},
        config=config,
    )

    assert len(published) == expected_published
    published_ids = {pack.id for pack in published}
    if have_dirty_map:
        assert set(dirty_map.values()) <= published_ids
    else:
        assert incoming.id not in published_ids
        assert len(published_ids) == len(published)


@pytest.mark.parametrize(
    "aem_pack_config, ingress_name",
    [
        (
            (
                AEM_PACK_REGISTRY_CONFIGS["bottleneck"],
                {"DerivedModel1", "DerivedModel2"},
            ),
            ingress,
        )
        for ingress in ("IngressModel1", "IngressModel2")
    ],
    ids=["bottleneck_from_IngressModel1", "bottleneck_from_IngressModel2"],
    indirect=["aem_pack_config"],
)
def test_bottleneck_topology(
    mock_registry: AEMPackRegistry,
    aem_pack_config: PersistedConfig,
    ingress_name: str,
):
    """Traversal from each ingress through a bottleneck publishes downstream
    and reuses existing dirty-map IDs for the derived packs.
    """
    incoming = _ingress_for(aem_pack_config, name=ingress_name)
    dirty_map: dict[str, UUID4] = {
        name: uuid4() for name in ("BottleneckModel", "DerivedModel1", "DerivedModel2")
    }

    published, remaining_dirty = mock_registry._traverse_graph(
        incoming=incoming,
        dirty_map=dict(dirty_map),
        transformed_map={incoming.model_name: incoming},
        config=aem_pack_config,
    )

    assert set(dirty_map).isdisjoint(remaining_dirty)
    assert {pack.model_name for pack in published} == {"DerivedModel1", "DerivedModel2"}
    assert {dirty_map["DerivedModel1"], dirty_map["DerivedModel2"]} <= {
        pack.id for pack in published
    }
