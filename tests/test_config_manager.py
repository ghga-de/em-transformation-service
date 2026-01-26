# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Tests for config manager and comparison functions."""

from contextlib import nullcontext
from pathlib import Path

import pytest
from pydantic import ValidationError

from ets.core.models import ComparisonResultChanged
from tests.fixtures.joint import JointFixture
from tests.fixtures.utils import BASE_DIR

CONFIG_DIR = BASE_DIR / "input_configs"

BASIC_TEST_CONFIG_PATH = CONFIG_DIR / "basic_test_config.yaml"
EXTENDED_TEST_CONFIG_PATH = CONFIG_DIR / "test_config.yaml"
INVALID_TEST_CONFIG_PATH = CONFIG_DIR / "invalid_config.yaml"


@pytest.mark.parametrize(
    "config_path,should_pass",
    [
        (BASIC_TEST_CONFIG_PATH, True),
        (EXTENDED_TEST_CONFIG_PATH, True),
        (INVALID_TEST_CONFIG_PATH, False),
    ],
)
async def test_loading_configs(
    config_path: Path, should_pass: bool, joint_fixture: JointFixture
) -> None:
    """TODO"""
    config_manager = joint_fixture.config_manager
    
    with nullcontext() if should_pass else pytest.raises(ValidationError):
        result = await config_manager.check_config_is_different()
    if should_pass:
        assert isinstance(result, ComparisonResultChanged)