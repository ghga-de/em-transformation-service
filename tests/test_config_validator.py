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

from ets.adapters.outbound.config_loader import ConfigLoaderAdapter
from ets.core.config_validation import ConfigValidationError, validate
from tests.fixtures.examples import (
    INVALID_ON_VALIDATION_CONFIGS,
    VALID_CONFIGS,
)


@pytest.mark.parametrize(
    "path",
    INVALID_ON_VALIDATION_CONFIGS.values(),
    ids=INVALID_ON_VALIDATION_CONFIGS.keys(),
)
def test_invalid_config(path: Path, loader: ConfigLoaderAdapter):
    """Check invalid configs raise ConfigValidationError."""
    changed_config = loader.load_config_from_file(path)
    with pytest.raises(ConfigValidationError):
        validate(changed_config)


@pytest.mark.parametrize(
    "path",
    VALID_CONFIGS.values(),
    ids=VALID_CONFIGS.keys(),
)
def test_valid_config(path: Path, loader: ConfigLoaderAdapter):
    """Check valid config passes validation without errors."""
    changed_config = loader.load_config_from_file(path)
    validate(changed_config)
