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

"""Fixtures for example transformation configs and mock schema."""

import json
from pathlib import Path

from yaml import safe_load

from ets.core.models import ValidatedConfig
from tests.fixtures.utils import BASE_DIR

CONFIG_DIR = BASE_DIR / "example_configs"

VALID_CONFIG_DIR = CONFIG_DIR / "valid"
INVALID_CONFIG_DIR = CONFIG_DIR / "invalid_on_validation"
INVALID_ON_LOAD_CONFIG_DIR = CONFIG_DIR / "invalid_on_loading"
MODEL_DERIVATION_DIR = CONFIG_DIR / "model_derivation"
PRUNING_DIR = CONFIG_DIR / "pruning"

MOCK_JSON_PATH = BASE_DIR / "mock.schemapack.json"


def list_examples_in_dir(dir: Path) -> dict[str, Path]:
    """List all example files in the given dir.

    Returns:
        A dict of {example_name: path}.
    """
    examples = {path.stem: path for path in dir.iterdir() if path.is_file()}

    return dict(sorted(examples.items()))


def read_mock_schema(path: Path) -> dict:
    """Read the mock schema from the json file."""
    with path.open("r") as file:
        return json.load(file)


def load_model_derivation_config(path: Path) -> ValidatedConfig:
    """Load a ``ValidatedConfig`` for model-derivation tests from a YAML file.

    Args:
        path: Path to the YAML fixture file.

    Returns:
        A ``ValidatedConfig`` instance populated from the file.
    """
    with path.open("r") as fh:
        return ValidatedConfig.model_validate(safe_load(fh))


VALID_CONFIGS = list_examples_in_dir(VALID_CONFIG_DIR)
INVALID_ON_VALIDATION_CONFIGS = list_examples_in_dir(INVALID_CONFIG_DIR)
INVALID_ON_LOAD_CONFIGS = list_examples_in_dir(INVALID_ON_LOAD_CONFIG_DIR)

VALID_MODEL_DERIVATION_CONFIGS = list_examples_in_dir(MODEL_DERIVATION_DIR / "valid")
INVALID_MODEL_DERIVATION_CONFIGS = list_examples_in_dir(
    MODEL_DERIVATION_DIR / "invalid"
)

PRUNING_CASES = list_examples_in_dir(PRUNING_DIR)

MOCK_SCHEMA = read_mock_schema(MOCK_JSON_PATH)
