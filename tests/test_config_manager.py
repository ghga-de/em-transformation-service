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

import pytest

from ets.core.config_manager import ConfigManager
from ets.core.models import ValidatedConfig
from tests.fixtures.config_manager import (
    PruningExpected,
    manager,  # noqa: F401
    pruning_fixture,  # noqa: F401
)
from tests.fixtures.examples import PRUNING_CASES


@pytest.mark.parametrize(
    "pruning_fixture, expected",
    [
        (
            PRUNING_CASES["all_published_nothing_pruned"],
            PruningExpected(
                models={"I", "A", "B"}, routes={"I:wf:A"}, workflows={"wf"}
            ),
        ),
        (
            PRUNING_CASES["all_unpublished_subgraph_prunes_ingress"],
            PruningExpected(),
        ),
        (
            PRUNING_CASES["published_ingress_with_trailing_unpublished"],
            PruningExpected(models={"I", "A"}),
        ),
        (
            PRUNING_CASES["only_unpublished_subgraph_pruned"],
            PruningExpected(models={"A"}),
        ),
        (
            PRUNING_CASES["route_referencing_pruned_output_removed"],
            PruningExpected(models={"I", "A"}, routes={"I:wf:A"}, workflows={"wf"}),
        ),
        (
            PRUNING_CASES["route_referencing_pruned_input_removed"],
            PruningExpected(models={"I"}),
        ),
        (
            PRUNING_CASES["orphaned_workflow_pruned"],
            PruningExpected(
                models={"I", "A"}, routes={"I:wf_keep:A"}, workflows={"wf_keep"}
            ),
        ),
        (
            PRUNING_CASES["shared_workflow_not_pruned"],
            PruningExpected(
                models={"I1", "A", "I2"},
                routes={"I1:shared_wf:A"},
                workflows={"shared_wf"},
            ),
        ),
    ],
    ids=PRUNING_CASES.keys(),
    indirect=["pruning_fixture"],
)
def test_prune_unpublished_leaves(
    manager: ConfigManager,  # noqa: F811
    pruning_fixture: ValidatedConfig,  # noqa: F811
    expected: PruningExpected,
):
    """Confirm ``_prune_unpublished_leaves`` retains the correct models, routes, and workflows."""
    result = manager._prune_unpublished_leaves(pruning_fixture)
    assert {m.name for m in result.models} == expected.models
    assert {r.name for r in result.routes} == expected.routes
    assert {w.name for w in result.workflows} == expected.workflows
