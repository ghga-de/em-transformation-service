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

"""Tests for config validator."""

from pathlib import Path

import pytest
from pydantic import ValidationError
from yaml import safe_load

from ets.core.config_validator import ConfigValidator
from ets.core.models import RawConfig
from ets.ports.inbound.config_validator import ConfigValidationError
from tests.fixtures.utils import BASE_DIR

CONFIG_DIR = BASE_DIR / "input_configs"

VALID_BASELINE_CONFIGS = CONFIG_DIR / "manager" / "valid"
INVALID_VALIDATOR_CONFIGS = CONFIG_DIR / "validator" / "invalid"


def _load_config(path: Path) -> RawConfig:
    """Load a config file and convert to RawConfig."""
    with path.open("r") as file:
        config_dict = safe_load(file)
    # RawConfig.models is list[InternalModel]; the field validator on InternalModel
    # automatically deserializes dict schema_ values to SchemaPack objects.
    return RawConfig.model_validate(config_dict)


class TestConfigValidator:
    """Test suite for ConfigValidator."""

    def setup_method(self):
        """Set up test validator."""
        self.validator = ConfigValidator()

    @pytest.mark.parametrize(
        "config_path",
        [INVALID_VALIDATOR_CONFIGS / "invalid_model_schema.yaml"],
    )
    def test_incomplete_schema(self, config_path):
        """Check incorrectly specified SchemaPack raises ValidationError."""
        with pytest.raises(ValidationError):
            _load_config(config_path)

    @pytest.mark.parametrize(
        "config_path",
        [
            config
            for config in sorted(INVALID_VALIDATOR_CONFIGS.iterdir())
            if config.name != "invalid_model_schema.yaml"
        ],
    )
    def test_invalid_config(self, config_path):
        """Check invalid configs raise ConfigValidationError."""
        changed_config = _load_config(config_path)
        with pytest.raises(ConfigValidationError):
            self.validator.validate(changed_config)

    @pytest.mark.parametrize(
        "config_path",
        sorted(VALID_BASELINE_CONFIGS.iterdir()),
    )
    def test_valid_config(self, config_path):
        """Check valid config passes validation without errors."""
        changed_config = _load_config(config_path)
        self.validator.validate(changed_config)
