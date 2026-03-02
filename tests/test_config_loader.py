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

"""Test transformation config loading."""

from pathlib import Path

import pytest

from ets.ports.outbound.config_loader import ConfigurationLoaderError
from tests.fixtures.examples import (
    INVALID_ON_LOAD_CONFIGS,
    INVALID_ON_VALIDATION_CONFIGS,
    VALID_CONFIGS,
)
from tests.fixtures.joint import JointFixture

# As long as there is structural integrity of the workflow config,
# it will be valid on loading
VALID_ON_LOAD_CONFIGS = INVALID_ON_VALIDATION_CONFIGS | VALID_CONFIGS


@pytest.mark.parametrize(
    "path",
    VALID_ON_LOAD_CONFIGS.values(),
    ids=VALID_ON_LOAD_CONFIGS.keys(),
)
def test_load_config_happy(path: Path, joint_fixture: JointFixture):
    """Test loading RawConfig from a transformation config file."""
    loader = joint_fixture.loader
    raw_config = loader.load_config_from_file(path)
    assert raw_config.models
    assert raw_config.routes
    assert raw_config.workflows


@pytest.mark.parametrize(
    "path",
    INVALID_ON_LOAD_CONFIGS.values(),
    ids=INVALID_ON_LOAD_CONFIGS.keys(),
)
def test_error_on_loading(path: Path, joint_fixture: JointFixture):
    """Check structural errors in the transformation config triggers ConfigurationLoaderError."""
    with pytest.raises(ConfigurationLoaderError):
        joint_fixture.loader.load_config_from_file(path)
