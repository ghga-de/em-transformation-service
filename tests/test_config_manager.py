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

"""Tests for the pruning logic in ConfigManager."""

from unittest.mock import MagicMock

import pytest
from metldata.workflow.base import Workflow as MetldataWorkflow

from ets.core.config_manager import ConfigManager
from ets.core.models import OrderedRawModel, Route, ValidatedConfig, Workflow


def make_model(
    name: str, order: int, publish: bool, is_ingress: bool = False
) -> OrderedRawModel:
    """Build a minimal OrderedRawModel for pruning tests."""
    return OrderedRawModel(
        name=name,
        order=order,
        publish=publish,
        is_ingress=is_ingress,
        version=None,
        schema_=None,
    )


def make_route(input_model: str, workflow_name: str, output_model: str) -> Route:
    """Build a Route from its three component names."""
    return Route(
        input_model_name=input_model,
        workflow_name=workflow_name,
        output_model_name=output_model,
    )


_MINIMAL_METLDATA_WORKFLOW = MetldataWorkflow.model_validate(
    {"operations": [{"name": "step", "description": "test step", "args": {}}]}
)


def make_workflow(name: str) -> Workflow:
    """Build a Workflow with a minimal valid metldata workflow."""
    return Workflow(name=name, workflow=_MINIMAL_METLDATA_WORKFLOW)


def make_config(
    models: list[OrderedRawModel],
    routes: list[Route] | None = None,
    workflows: list[Workflow] | None = None,
) -> ValidatedConfig:
    """Build a ValidatedConfig from components, defaulting to empty routes/workflows."""
    return ValidatedConfig(
        models=models,
        routes=routes or [],
        workflows=workflows or [],
    )


@pytest.fixture
def manager() -> ConfigManager:
    """ConfigManager with mock ports (unused by pruning methods)."""
    return ConfigManager(validator=MagicMock(), comparator=MagicMock())  # type: ignore[arg-type]


def model_names(config: ValidatedConfig) -> set[str]:
    return {m.name for m in config.models}


def route_names(config: ValidatedConfig) -> set[str]:
    return {r.name for r in config.routes}


def workflow_names(config: ValidatedConfig) -> set[str]:
    return {w.name for w in config.workflows}


def test_all_published_nothing_pruned(manager: ConfigManager):
    """All published models are retained."""
    config = make_config([
        make_model("I", order=0, publish=True, is_ingress=True),
        make_model("A", order=1, publish=True),
        make_model("B", order=2, publish=True),
    ])
    result = manager._prune_unpublished_leaves(config)
    assert model_names(result) == {"I", "A", "B"}


def test_single_trailing_unpublished_leaf_is_pruned(manager: ConfigManager):
    """A single unpublished leaf at the end of a subgraph is removed."""
    config = make_config([
        make_model("I", order=0, publish=True, is_ingress=True),
        make_model("A", order=1, publish=True),
        make_model("B", order=2, publish=False),
    ])
    result = manager._prune_unpublished_leaves(config)
    assert model_names(result) == {"I", "A"}


def test_multiple_trailing_unpublished_leaves_are_pruned(manager: ConfigManager):
    """Multiple unpublished trailing models are all removed."""
    config = make_config([
        make_model("I", order=0, publish=True, is_ingress=True),
        make_model("A", order=1, publish=True),
        make_model("B", order=2, publish=False),
        make_model("C", order=3, publish=False),
    ])
    result = manager._prune_unpublished_leaves(config)
    assert model_names(result) == {"I", "A"}


def test_unpublished_non_trailing_model_is_kept(manager: ConfigManager):
    """An unpublished model that is not the last leaf is not pruned."""
    config = make_config([
        make_model("I", order=0, publish=True, is_ingress=True),
        make_model("A", order=1, publish=False),
        make_model("B", order=2, publish=True),
    ])
    result = manager._prune_unpublished_leaves(config)
    assert model_names(result) == {"I", "A", "B"}


def test_all_unpublished_subgraph_prunes_ingress(manager: ConfigManager):
    """An ingress with no published descendants is itself pruned."""
    config = make_config([
        make_model("I", order=0, publish=False, is_ingress=True),
        make_model("A", order=1, publish=False),
        make_model("B", order=2, publish=False),
    ])
    result = manager._prune_unpublished_leaves(config)
    assert model_names(result) == set()


def test_published_ingress_subgraph_not_pruned(manager: ConfigManager):
    """Ingress is retained when at least one leaf in its subgraph is published."""
    config = make_config([
        make_model("I", order=0, publish=False, is_ingress=True),
        make_model("A", order=1, publish=True),
        make_model("B", order=2, publish=False),
    ])
    result = manager._prune_unpublished_leaves(config)
    assert "I" in model_names(result)
    assert "A" in model_names(result)
    assert "B" not in model_names(result)


def test_only_unpublished_subgraph_is_pruned(manager: ConfigManager):
    """Two subgraphs: only the one without a published leaf is pruned."""
    config = make_config([
        # subgraph 1 – has a published leaf
        make_model("I1", order=0, publish=False, is_ingress=True),
        make_model("A",  order=1, publish=True),
        # subgraph 2 – no published leaf
        make_model("I2", order=2, publish=False, is_ingress=True),
        make_model("B",  order=3, publish=False),
    ])
    result = manager._prune_unpublished_leaves(config)
    assert model_names(result) == {"I1", "A"}


def test_both_subgraphs_trailing_unpublished_pruned(manager: ConfigManager):
    """Two subgraphs each with an unpublished trailing leaf: both leaves pruned."""
    config = make_config([
        make_model("I1", order=0, publish=False, is_ingress=True),
        make_model("A",  order=1, publish=True),
        make_model("B",  order=2, publish=False),
        make_model("I2", order=3, publish=False, is_ingress=True),
        make_model("C",  order=4, publish=True),
        make_model("D",  order=5, publish=False),
    ])
    result = manager._prune_unpublished_leaves(config)
    assert model_names(result) == {"I1", "A", "I2", "C"}


def test_route_referencing_pruned_model_is_removed(manager: ConfigManager):
    """A route whose output model is pruned is also removed."""
    config = make_config(
        models=[
            make_model("I", order=0, publish=True, is_ingress=True),
            make_model("A", order=1, publish=True),
            make_model("B", order=2, publish=False),
        ],
        routes=[
            make_route("I", "wf", "A"),
            make_route("A", "wf", "B"),
        ],
        workflows=[make_workflow("wf")],
    )
    result = manager._prune_unpublished_leaves(config)
    assert route_names(result) == {"I:wf:A"}


def test_route_with_pruned_input_model_is_removed(manager: ConfigManager):
    """A route whose input model is pruned is also removed."""
    config = make_config(
        models=[
            make_model("I", order=0, publish=True, is_ingress=True),
            make_model("A", order=1, publish=False),
            make_model("B", order=2, publish=False),
        ],
        routes=[
            make_route("I", "wf", "A"),
            make_route("A", "wf", "B"),
        ],
        workflows=[make_workflow("wf")],
    )
    result = manager._prune_unpublished_leaves(config)
    assert route_names(result) == set()

def test_orphaned_workflow_is_pruned(manager: ConfigManager):
    """A workflow referenced only by removed routes is pruned."""
    config = make_config(
        models=[
            make_model("I", order=0, publish=True, is_ingress=True),
            make_model("A", order=1, publish=True),
            make_model("B", order=2, publish=False),
        ],
        routes=[
            make_route("I", "wf_keep", "A"),
            make_route("A", "wf_drop", "B"),
        ],
        workflows=[make_workflow("wf_keep"), make_workflow("wf_drop")],
    )
    result = manager._prune_unpublished_leaves(config)
    assert workflow_names(result) == {"wf_keep"}


def test_shared_workflow_not_pruned(manager: ConfigManager):
    """A workflow referenced by both a pruned and a surviving route is kept."""
    config = make_config(
        models=[
            make_model("I1", order=0, publish=True, is_ingress=True),
            make_model("A",  order=1, publish=True),
            make_model("I2", order=2, publish=True, is_ingress=True),
            make_model("B",  order=3, publish=False),
        ],
        routes=[
            make_route("I1", "shared_wf", "A"),   # survives
            make_route("I2", "shared_wf", "B"),   # pruned (B unpublished)
        ],
        workflows=[make_workflow("shared_wf")],
    )
    result = manager._prune_unpublished_leaves(config)
    assert "shared_wf" in workflow_names(result)


def test_empty_config_unchanged(manager: ConfigManager):
    """An empty config passes through without error."""
    config = make_config(models=[])
    result = manager._prune_unpublished_leaves(config)
    assert model_names(result) == set()
    assert route_names(result) == set()
    assert workflow_names(result) == set()
