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
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from yaml import safe_load

from ets.core import config_manager as config_manager_module
from ets.core.config_manager import ConfigManager, ConfigManagerError
from ets.core.config_pruning import prune_unproductive_subgraphs
from ets.core.config_validation import ConfigValidationError
from ets.core.model_derivation import ModelDeriver
from ets.core.models import Model, PersistedConfig, RawConfig, ValidatedConfig
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.config_writer import ConfigWriterPort
from tests.fixtures.config_manager import pruning_fixture  # noqa: F401
from tests.fixtures.examples import PRUNING_CASES, VALID_CONFIGS


@dataclass
class PruningResult:
    """Expected state after applying prune_unproductive_subgraphs."""

    models: set[str] = field(default_factory=set)
    routes: set[str] = field(default_factory=set)
    workflows: set[str] = field(default_factory=set)


@pytest.mark.parametrize(
    "pruning_fixture, expected",
    [
        (
            PRUNING_CASES["bifurcating_subgraph"],
            PruningResult(
                models={"PublishedSource", "PublishedBranch1", "PublishedBranch2"},
                routes={
                    "PublishedSource:workflow:PublishedBranch1",
                    "PublishedSource:workflow:PublishedBranch2",
                },
                workflows={"workflow"},
            ),
        ),
        (
            PRUNING_CASES["converging_subgraph"],
            PruningResult(
                models={
                    "UnpublishedSource1",
                    "UnpublishedSource2",
                    "PublishedSource",
                    "PublishedDerived",
                },
                routes={
                    "UnpublishedSource1:workflow:PublishedDerived",
                    "UnpublishedSource2:workflow:PublishedDerived",
                    "PublishedSource:workflow:PublishedDerived",
                },
                workflows={"workflow"},
            ),
        ),
        (
            PRUNING_CASES["bottleneck_subgraph"],
            PruningResult(
                models={
                    "UnpublishedSource",
                    "PublishedSource",
                    "Bottleneck",
                    "Published1",
                    "Published2",
                },
                routes={
                    "UnpublishedSource:workflow:Bottleneck",
                    "PublishedSource:workflow:Bottleneck",
                    "Bottleneck:workflow:Published1",
                    "Bottleneck:workflow:Published2",
                },
                workflows={"workflow"},
            ),
        ),
        (
            PRUNING_CASES["nothing_pruned"],
            PruningResult(
                models={"PublishedSource", "PublishedDerived", "PublishedSource_2"},
                routes={"PublishedSource:workflow:PublishedDerived"},
                workflows={"workflow"},
            ),
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
        (
            PRUNING_CASES["emim_not_pruned"],
            PruningResult(
                models={"UnpublishedSource", "PublishedDerived", "UnpublishedEMIM"},
                routes={"UnpublishedSource:workflow:PublishedDerived"},
                workflows={"workflow"},
            ),
        ),
    ],
    ids=[
        "bifurcating_subgraph",
        "converging_subgraph",
        "bottleneck_subgraph",
        "nothing_pruned",
        "keep_referenced_workflow",
        "leaf_pruned",
        "shared_workflow_not_pruned",
        "emim_not_pruned",
    ],
    indirect=["pruning_fixture"],
)
def test_prune_unproductive_subgraphs(
    pruning_fixture: ValidatedConfig,  # noqa: F811
    expected: PruningResult,
):
    """Confirm prune_unproductive_subgraphs retains the correct models, routes, and workflows."""
    result = prune_unproductive_subgraphs(pruning_fixture)
    assert {m.name for m in result.models} == expected.models
    assert {r.name for r in result.routes} == expected.routes
    assert {w.name for w in result.workflows} == expected.workflows


@pytest.mark.parametrize(
    "pruning_fixture",
    [
        PRUNING_CASES["everything_pruned"],
        PRUNING_CASES["two_subgraphs_pruned"],
        PRUNING_CASES["prune_unreferenced_workflow"],
    ],
    ids=["everything_pruned", "two_subgraphs_pruned", "prune_unreferenced_workflow"],
    indirect=["pruning_fixture"],
)
def test_prune_unproductive_subgraphs_raises(
    pruning_fixture: ValidatedConfig,  # noqa: F811
):
    """Confirm prune_unproductive_subgraphs raises when pruning leaves results in any empty config field."""
    with pytest.raises(ConfigValidationError):
        prune_unproductive_subgraphs(pruning_fixture)


def _make_manager(
    *,
    raw_config: RawConfig,
    persisted_config: PersistedConfig,
    model_deriver: ModelDeriver,
) -> tuple[ConfigManager, MagicMock, AsyncMock]:
    loader = MagicMock(spec=ConfigLoaderPort)
    loader.load_config_from_file.return_value = raw_config
    loader.load_config_from_db = AsyncMock(return_value=persisted_config)

    writer = MagicMock(spec=ConfigWriterPort)
    writer.write_config = AsyncMock()

    manager = ConfigManager(
        loader=loader,
        model_deriver=model_deriver,
        writer=writer,
        versioner=MagicMock(spec=ConfigVersionerPort),
    )
    return manager, loader, writer


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "compare_returns_raw, validation_raises",
    [(True, False), (True, True), (False, False)],
    ids=["new_valid_config", "validation_fallback", "unchanged_config"],
)
async def test_resolve_and_persist(
    compare_returns_raw: bool,
    validation_raises: bool,
    monkeypatch: pytest.MonkeyPatch,
):
    """Confirm resolve_and_persist handles the happy paths and validation fallback."""
    with VALID_CONFIGS["basic_config"].open() as fh:
        raw_config = RawConfig.model_validate(safe_load(fh))
    with PRUNING_CASES["nothing_pruned"].open() as fh:
        validated_config = ValidatedConfig.model_validate(safe_load(fh)["config"])

    persisted = MagicMock(spec=PersistedConfig)
    persisted.models = [MagicMock()]
    persisted.routes = [MagicMock()]
    persisted.workflows = [MagicMock()]

    compare_configs = MagicMock(
        return_value=raw_config if compare_returns_raw else persisted
    )
    monkeypatch.setattr(config_manager_module, "compare_configs", compare_configs)

    validate = MagicMock(
        side_effect=ConfigValidationError("invalid") if validation_raises else None,
        return_value=validated_config,
    )
    monkeypatch.setattr(config_manager_module, "validate", validate)

    derived_models = [MagicMock(spec=Model)]
    model_deriver = MagicMock(spec=ModelDeriver)
    model_deriver.derive_models.return_value = derived_models

    manager, loader, writer = _make_manager(
        raw_config=raw_config,
        persisted_config=persisted,
        model_deriver=model_deriver,
    )

    await manager.resolve_and_persist(Path("/fake/config.yaml"))

    loader.load_config_from_file.assert_called_once()
    loader.load_config_from_db.assert_awaited_once()
    compare_configs.assert_called_once_with(raw_config, persisted)

    if compare_returns_raw and not validation_raises:
        validate.assert_called_once_with(raw_config)
        model_deriver.derive_models.assert_called_once()
        writer.write_config.assert_awaited_once()
        written = writer.write_config.await_args.args[0]
        assert isinstance(written, PersistedConfig)
        assert written.models == derived_models
    else:
        # validation_fallback and unchanged_config both leave the DB alone:
        # write_config would bump the version and trigger spurious reprocessing.
        model_deriver.derive_models.assert_not_called()
        writer.write_config.assert_not_awaited()


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "models, routes, workflows",
    [
        ([], [], []),
        ([MagicMock()], [], []),
        ([], [MagicMock()], []),
        ([], [], [MagicMock()]),
        ([MagicMock()], [MagicMock()], []),
        ([MagicMock()], [], [MagicMock()]),
        ([], [MagicMock()], [MagicMock()]),
    ],
    ids=[
        "all_empty",
        "only_models",
        "only_routes",
        "only_workflows",
        "missing_workflows",
        "missing_routes",
        "missing_models",
    ],
)
async def test_resolve_and_persist_stops_when_no_persisted_config(
    models, routes, workflows, monkeypatch: pytest.MonkeyPatch
):
    """When validation fails and no valid config is persisted, raise ConfigManagerError."""
    with VALID_CONFIGS["basic_config"].open() as fh:
        raw_config = RawConfig.model_validate(safe_load(fh))

    incomplete_persisted = PersistedConfig(models=[], routes=[], workflows=[])
    incomplete_persisted.models = models
    incomplete_persisted.routes = routes
    incomplete_persisted.workflows = workflows

    monkeypatch.setattr(
        config_manager_module, "compare_configs", MagicMock(return_value=raw_config)
    )
    monkeypatch.setattr(
        config_manager_module,
        "validate",
        MagicMock(side_effect=ConfigValidationError("invalid")),
    )

    model_deriver = MagicMock(spec=ModelDeriver)

    manager, _, writer = _make_manager(
        raw_config=raw_config,
        persisted_config=incomplete_persisted,
        model_deriver=model_deriver,
    )

    with pytest.raises(ConfigManagerError, match="no previous valid config"):
        await manager.resolve_and_persist(Path("/fake/config.yaml"))
    writer.write_config.assert_not_awaited()
