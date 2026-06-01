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

"""Fixtures for config updater pruning tests."""

from collections.abc import Generator
from pathlib import Path

import pytest
from yaml import safe_load

from ets.core.models import RawConfig, ValidatedConfig


@pytest.fixture
def pruning_fixture(request: pytest.FixtureRequest) -> Generator[ValidatedConfig]:
    """Load a ValidatedConfig from the YAML path passed via indirect (needs to be set on the test case)."""
    path: Path = request.param
    with path.open("r") as fh:
        data = safe_load(fh)
    yield ValidatedConfig.model_validate(data["config"])


@pytest.fixture
def raw_config_fixture(request: pytest.FixtureRequest) -> Generator[RawConfig]:
    """Load a RawConfig from the YAML path passed via indirect (needs to be set on the test case)."""
    path: Path = request.param
    with path.open("r") as fh:
        data = safe_load(fh)
    yield RawConfig.model_validate(data)
