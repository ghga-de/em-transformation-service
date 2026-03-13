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

"""Fixtures for model derivation tests."""

from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from schemapack.spec.schemapack import SchemaPack

from ets.core.model_derivation import ModelDeriver
from ets.core.models import ValidatedConfig
from tests.fixtures.examples import load_model_derivation_config

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
    """Holds a loaded ``ValidatedConfig`` and the corresponding ``ModelDeriver``."""

    config: ValidatedConfig
    deriver: ModelDeriver


@pytest.fixture
def model_derivation_fixture(
    request: pytest.FixtureRequest,
) -> Generator[ModelDerivationFixture]:
    """Build a ``ModelDerivationFixture`` from the path passed via ``indirect``."""
    path: Path = request.param
    config = load_model_derivation_config(path)
    deriver = ModelDeriver(config=config)
    yield ModelDerivationFixture(config=config, deriver=deriver)


@pytest.fixture
def mock_apply_workflow(
    model_derivation_fixture: ModelDerivationFixture,
) -> Generator[MagicMock]:
    """Patch ``_apply_workflow`` on the deriver and yield the mock.

    Tests configure the mock via ``.return_value`` or ``.side_effect``
    before calling ``derive_models``.
    """
    with patch.object(model_derivation_fixture.deriver, "_apply_workflow") as mock:
        yield mock
