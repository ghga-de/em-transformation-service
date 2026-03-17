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

"""Fixtures for config manager pruning tests."""

from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from yaml import safe_load

from ets.core.config_manager import ConfigManager
from ets.core.models import ValidatedConfig


@dataclass
class PruningExpected:
    """Expected state after applying ``_prune_unpublished_leaves``."""

    models: set[str] = field(default_factory=set)
    routes: set[str] = field(default_factory=set)
    workflows: set[str] = field(default_factory=set)


@pytest.fixture
def manager() -> ConfigManager:
    """``ConfigManager`` with mock ports (unused by pruning methods)."""
    return ConfigManager(validator=MagicMock(), comparator=MagicMock())  # type: ignore[arg-type]


@pytest.fixture
def pruning_fixture(request: pytest.FixtureRequest) -> ValidatedConfig:
    """Load a ``ValidatedConfig`` from the YAML path passed via ``indirect``."""
    path: Path = request.param
    with path.open("r") as fh:
        data = safe_load(fh)
    return ValidatedConfig.model_validate(data["config"])
