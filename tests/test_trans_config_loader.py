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

import json
from pathlib import Path

import pytest

from ets.core.trans_config_loader import ConfigurationLoaderError, TransConfigFileLoader
from tests.fixtures.utils import BASE_DIR

CONFIG_DIR = BASE_DIR / "input_configs" / "manager"

INVALID_CONFIG_DIR = CONFIG_DIR / "invalid"
VALID_CONFIG_DIR = CONFIG_DIR / "valid"

BASIC_TEST_CONFIG_PATH = VALID_CONFIG_DIR / "basic_config.yaml"
EXTENDED_TEST_CONFIG_PATH = VALID_CONFIG_DIR / "large_config.yaml"
INVALID_TEST_CONFIG_PATH = INVALID_CONFIG_DIR / "without_routes.yaml"

MOCK_JSON_PATH = BASE_DIR / "mock.schemapack.json"

with MOCK_JSON_PATH.open("r") as file:
    MOCK_SCHEMA = json.load(file)

loader = TransConfigFileLoader()


@pytest.mark.parametrize(
    "config_path,should_pass",
    [
        (BASIC_TEST_CONFIG_PATH, True),
        (EXTENDED_TEST_CONFIG_PATH, True),
        (INVALID_TEST_CONFIG_PATH, False),
    ],
)
def test_load_config(config_path: Path, should_pass: bool) -> None:
    """Test loading RawConfig from a transformation config file."""
    if should_pass:
        raw_config = loader.load_config_from_file(config_path)
        assert raw_config.models
        assert raw_config.routes
        assert raw_config.workflows
    else:
        with pytest.raises(ConfigurationLoaderError):
            loader.load_config_from_file(config_path)
