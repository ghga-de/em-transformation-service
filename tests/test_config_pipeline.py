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

"""End-to-end tests for the config load → validate pipeline."""

from pathlib import Path

import pytest

from ets.adapters.outbound.config_loader import ConfigLoaderAdapter
from ets.core.config_validator import ConfigValidator
from ets.ports.inbound.config_validator import ConfigValidationError
from ets.ports.outbound.config_loader import ConfigurationLoaderError
from tests.fixtures.config_examples import (
    INVALID_ON_LOAD_CONFIGS,
    INVALID_ON_VALIDATION_CONFIGS,
    VALID_CONFIGS,
)

# Sanity check: invalid_on_validation configs must not collide with valid ones.
_overlapping_keys = INVALID_ON_VALIDATION_CONFIGS.keys() & VALID_CONFIGS.keys()
if _overlapping_keys:
    raise ValueError(
        "Duplicate config IDs across INVALID_ON_VALIDATION_CONFIGS and VALID_CONFIGS: "
        f"{sorted(_overlapping_keys)}. Invalid configs must be prefixed with the "
        "name of the invalid component, e.g. 'invalid_model_...'"
    )


@pytest.fixture(scope="module")
def validator() -> ConfigValidator:
    """Stateless validator instance shared across pipeline tests."""
    return ConfigValidator()


@pytest.mark.parametrize("path", VALID_CONFIGS.values(), ids=VALID_CONFIGS.keys())
def test_valid_config_loads_and_validates(
    path: Path, loader: ConfigLoaderAdapter, validator: ConfigValidator
):
    """Valid configs pass both the loader and the validator."""
    raw_config = loader.load_config_from_file(path)
    assert raw_config.models
    assert raw_config.routes
    assert raw_config.workflows

    validator.validate(raw_config)


@pytest.mark.parametrize(
    "path", INVALID_ON_LOAD_CONFIGS.values(), ids=INVALID_ON_LOAD_CONFIGS.keys()
)
def test_invalid_on_load_raises(path: Path, loader: ConfigLoaderAdapter):
    """Structurally invalid configs raise during loading."""
    with pytest.raises(ConfigurationLoaderError):
        loader.load_config_from_file(path)


@pytest.mark.parametrize(
    "path",
    INVALID_ON_VALIDATION_CONFIGS.values(),
    ids=INVALID_ON_VALIDATION_CONFIGS.keys(),
)
def test_invalid_on_validation_loads_then_raises(
    path: Path, loader: ConfigLoaderAdapter, validator: ConfigValidator
):
    """Configs with semantic errors load successfully but fail validation."""
    raw_config = loader.load_config_from_file(path)
    with pytest.raises(ConfigValidationError):
        validator.validate(raw_config)
