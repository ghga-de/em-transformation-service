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
from yaml import safe_load

from ets.core.config_validator import ConfigValidator
from ets.core.models import ComparisonResultChanged, RawConfig
from ets.ports.inbound.config_validator import ConfigValidationError
from tests.fixtures.utils import BASE_DIR

CONFIG_DIR = BASE_DIR / "input_configs" / "validation"
INPUT_CONFIGS_DIR = BASE_DIR / "input_configs"

# Valid config for baseline
BASIC_VALID_CONFIG_PATH = INPUT_CONFIGS_DIR / "basic_test_config.yaml"
VALID_CONFIG_PATH = INPUT_CONFIGS_DIR / "test_config.yaml"

# Invalid configs for testing each validation rule
INVALID_ROUTE_INPUT_MODEL_PATH = CONFIG_DIR / "invalid_route_input_model.yaml"
INVALID_ROUTE_INPUT_NOT_INGRESS_PATH = (
    CONFIG_DIR / "invalid_route_input_not_ingress.yaml"
)
INVALID_ROUTE_WORKFLOW_PATH = CONFIG_DIR / "invalid_route_workflow.yaml"
INVALID_ROUTE_OUTPUT_MODEL_PATH = CONFIG_DIR / "invalid_route_output_model.yaml"
INVALID_ROUTE_OUTPUT_IS_INGRESS_PATH = (
    CONFIG_DIR / "invalid_route_output_is_ingress.yaml"
)
INVALID_MODEL_SCHEMA_PATH = CONFIG_DIR / "invalid_model_schema.yaml"
INVALID_WORKFLOW_UNKNOWN_TRANSFORMATION_PATH = (
    CONFIG_DIR / "invalid_workflow_unknown_transformation.yaml"
)
INVALID_WORKFLOW_CONFIG_TYPE_PATH = CONFIG_DIR / "invalid_workflow_config_type.yaml"


def _load_config(path: Path) -> ComparisonResultChanged:
    """Load a config file and convert to ComparisonResultChanged."""
    with path.open("r") as file:
        config_dict = safe_load(file)
    raw_config = RawConfig.model_validate(config_dict)
    return ComparisonResultChanged(
        models=raw_config.models,
        routes=raw_config.routes,
        workflows=raw_config.workflows,
    )


class TestConfigValidator:
    """Test suite for ConfigValidator."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = ConfigValidator()

    @pytest.mark.parametrize(
        "config_path,error_match",
        [
            (INVALID_ROUTE_INPUT_MODEL_PATH, "non-existent input model"),
            (INVALID_ROUTE_WORKFLOW_PATH, "non-existent workflow"),
            (INVALID_ROUTE_OUTPUT_MODEL_PATH, "non-existent output model"),
            (INVALID_ROUTE_OUTPUT_IS_INGRESS_PATH, "must not be an ingress model"),
            (INVALID_MODEL_SCHEMA_PATH, "Invalid schema"),
            (INVALID_WORKFLOW_UNKNOWN_TRANSFORMATION_PATH, "Unknown transformation"),
            (
                INVALID_WORKFLOW_CONFIG_TYPE_PATH,
                "Invalid configuration for transformation",
            ),
        ],
    )
    def test_invalid_config(self, config_path, error_match):
        """Invalid configs raise ConfigValidationError with appropriate message."""
        changed_config = _load_config(config_path)
        with pytest.raises(ConfigValidationError, match=error_match):
            self.validator.validate(changed_config)

    @pytest.mark.parametrize(
        "config_path",
        [
            VALID_CONFIG_PATH,
            BASIC_VALID_CONFIG_PATH,
        ],
    )
    def test_valid_config(self, config_path):
        """Valid config passes validation without errors."""
        changed_config = _load_config(config_path)
        self.validator.validate(changed_config)
