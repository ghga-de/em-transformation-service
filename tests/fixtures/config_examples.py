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

"""Example configs: paths, loading helpers, schemas, and parametrize fixtures."""

import json
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from schemapack.spec.schemapack import SchemaPack
from yaml import safe_load

from ets.core.model_derivation import ModelDeriver
from ets.core.models import ValidatedConfig

BASE_DIR = Path(__file__).parent.resolve()
CONFIG_DIR = BASE_DIR / "example_configs"

PIPELINE_DIR = CONFIG_DIR / "pipeline"
VALID_CONFIG_DIR = PIPELINE_DIR / "valid"
INVALID_CONFIG_DIR = PIPELINE_DIR / "invalid_on_validation"
INVALID_ON_LOAD_CONFIG_DIR = PIPELINE_DIR / "invalid_on_loading"
MODEL_DERIVATION_DIR = CONFIG_DIR / "model_derivation"
PRUNING_DIR = CONFIG_DIR / "pruning"
AEM_PACK_REGISTRY_DIR = CONFIG_DIR / "aem_pack_registry"

MOCK_JSON_PATH = BASE_DIR / "mock.schemapack.json"


def list_examples_in_dir(dir: Path) -> dict[str, Path]:
    """Return ``{stem: path}`` for files in ``dir``, sorted by stem."""
    return dict(sorted((path.stem, path) for path in dir.iterdir() if path.is_file()))


def read_mock_schema(path: Path) -> dict:
    """Read the mock schema from a JSON file."""
    with path.open("r") as file:
        return json.load(file)


def load_validated_config(path: Path) -> ValidatedConfig:
    """Load a ``ValidatedConfig`` from a YAML fixture file."""
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
AEM_PACK_REGISTRY_CONFIGS = list_examples_in_dir(AEM_PACK_REGISTRY_DIR)

MOCK_SCHEMA = read_mock_schema(MOCK_JSON_PATH)


_FILE_CONTENT = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "additionalProperties": False,
    "properties": {
        "checksum": {"type": "string"},
        "filename": {"type": "string"},
        "format": {"type": "string"},
        "size": {"type": "integer"},
    },
    "required": ["filename", "format", "checksum", "size"],
    "type": "object",
}

FILE_SCHEMA = SchemaPack.model_validate(
    {
        "schemapack": "4.0.0",
        "classes": {
            "File": {
                "id": {"propertyName": "alias"},
                "content": _FILE_CONTENT,
            }
        },
    }
)

FILE_RENAMED_ID_SCHEMA = SchemaPack.model_validate(
    {
        "schemapack": "4.0.0",
        "classes": {
            "File": {
                "id": {"propertyName": "file_id"},
                "content": _FILE_CONTENT,
            }
        },
    }
)

RENAMED_ID_WITH_BACKUP_SCHEMA = SchemaPack.model_validate(
    {
        "schemapack": "4.0.0",
        "classes": {
            "File": {
                "id": {"propertyName": "file_id"},
                "content": _FILE_CONTENT,
            },
            "FileBackup": {
                "id": {"propertyName": "file_id"},
                "content": _FILE_CONTENT,
            },
        },
    }
)


@dataclass
class ModelDerivationFixture:
    """Holds a loaded ValidatedConfig and the corresponding ModelDeriver."""

    config: ValidatedConfig
    deriver: ModelDeriver


@pytest.fixture
def model_derivation_fixture(
    request: pytest.FixtureRequest,
) -> Generator[ModelDerivationFixture]:
    """Build a ``ModelDerivationFixture`` from the YAML path passed via ``indirect``."""
    config = load_validated_config(request.param)
    yield ModelDerivationFixture(config=config, deriver=ModelDeriver(config=config))


@pytest.fixture
def mock_apply_workflow(
    model_derivation_fixture: ModelDerivationFixture,
) -> Generator[MagicMock]:
    """Patch ``_apply_workflow`` on the deriver and yield the mock."""
    with patch.object(model_derivation_fixture.deriver, "_apply_workflow") as mock:
        yield mock


@pytest.fixture
def pruning_fixture(request: pytest.FixtureRequest) -> Generator[ValidatedConfig]:
    """Load a ``ValidatedConfig`` from the YAML's ``config:`` section, via ``indirect``."""
    with request.param.open("r") as fh:
        data = safe_load(fh)
    yield ValidatedConfig.model_validate(data["config"])
