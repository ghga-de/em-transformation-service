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

from dataclasses import dataclass, field

import pytest

from ets.core.config_manager import ConfigManager
from ets.core.models import ValidatedConfig
from tests.fixtures.config_manager import (
    manager,  # noqa: F401
    pruning_fixture,  # noqa: F401
)
from tests.fixtures.examples import PRUNING_CASES


@dataclass
class PruningResult:
    """Expected state after applying _prune_unpublished_leaves."""

    models: set[str] = field(default_factory=set)
    routes: set[str] = field(default_factory=set)
    workflows: set[str] = field(default_factory=set)


@pytest.mark.parametrize(
    "pruning_fixture, expected",
    [
        (
            PRUNING_CASES["nothing_pruned"],
            PruningResult(
                models={"PublishedSource", "PublishedDerived", "PublishedSource_2"},
                routes={"PublishedSource:workflow:PublishedDerived"},
                workflows={"workflow"},
            ),
        ),
        (
            PRUNING_CASES["everything_pruned"],
            PruningResult(),
        ),
        (
            PRUNING_CASES["two_subgraphs_pruned"],
            PruningResult(models={"PublishedSource"}),
        ),
        (
            PRUNING_CASES["keep_referenced_workflow"],
            PruningResult(
                models={"PublishedSource", "PublishedDerived"},
                routes={"PublishedSource:workflow:PublishedDerived"},
                workflows={"workflow"},
            ),
        ),
        (
            PRUNING_CASES["prune_unreferenced_workflow"],
            PruningResult(models={"PublishedSource"}),
        ),
        (
            PRUNING_CASES["leaf_pruned"],
            PruningResult(
                models={"PublishedSource", "PublishedDerived"},
                routes={"PublishedSource:kept_workflow:PublishedDerived"},
                workflows={"kept_workflow"},
            ),
        ),
        (
            PRUNING_CASES["shared_workflow_not_pruned"],
            PruningResult(
                models={"UnpublishedSource", "PublishedDerived", "PublishedSource"},
                routes={"UnpublishedSource:shared_workflow:PublishedDerived"},
                workflows={"shared_workflow"},
            ),
        ),
    ],
    ids=PRUNING_CASES.keys(),
    indirect=["pruning_fixture"],
)
def test_prune_unpublished_leaves(
    manager: ConfigManager,  # noqa: F811
    pruning_fixture: ValidatedConfig,  # noqa: F811
    expected: PruningResult,
):
    """Confirm _prune_unpublished_leaves retains the correct models, routes, and workflows."""
    result = manager._prune_unpublished_leaves(pruning_fixture)
    assert {m.name for m in result.models} == expected.models
    assert {r.name for r in result.routes} == expected.routes
    assert {w.name for w in result.workflows} == expected.workflows
