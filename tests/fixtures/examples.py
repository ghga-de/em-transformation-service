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

import contextlib
import json
import os
from collections.abc import Generator, Iterator, Mapping
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from types import MappingProxyType
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel
from schemapack.spec.schemapack import SchemaPack
from yaml import safe_load

from ets.core import model_derivation
from ets.core.models import PersistedConfig, RawConfig, ValidatedConfig

BASE_DIR = Path(__file__).parent.resolve()
CONFIG_DIR = BASE_DIR / "example_configs"
MOCK_JSON_PATH = BASE_DIR / "mock.schemapack.json"


@cache
def _examples_in(subpath: str) -> Mapping[str, Path]:
    """Return `{stem: path}` for files under `CONFIG_DIR / subpath`, sorted by stem.

    The result is a read-only `MappingProxyType` because `@cache` shares one instance
    across callers and mutation would silently corrupt the cache.
    """
    return MappingProxyType(
        dict(
            sorted((p.stem, p) for p in (CONFIG_DIR / subpath).iterdir() if p.is_file())
        )
    )


@contextlib.contextmanager
def _cwd(path: Path) -> Iterator[None]:
    """Chdir to `path` for the block; restore on exit.

    Required so schemapack's content-schema validator resolves `content: ../foo.json`
    references relative to the YAML fixture's own directory, not the test runner's cwd.
    """
    original = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original)


def _load[ModelT: BaseModel](
    path: Path, model: type[ModelT], *, section: str | None = None
) -> ModelT:
    """Load and validate `model` from a YAML fixture."""
    with path.open("r") as fh:
        data = safe_load(fh)
    if section is not None:
        data = data[section]
    with _cwd(path.parent):
        return model.model_validate(data)


def load_validated_config(path: Path) -> ValidatedConfig:
    """Load a `ValidatedConfig` from a YAML fixture file."""
    return _load(path, ValidatedConfig)


def load_raw_config(path: Path) -> RawConfig:
    """Load a `RawConfig` from a YAML fixture file."""
    return _load(path, RawConfig)


def load_pruning_config(path: Path) -> ValidatedConfig:
    """Load a `ValidatedConfig` from a pruning-case YAML's `config` section."""
    return _load(path, ValidatedConfig, section="config")


def load_aem_pack_config(
    path: Path,
    publish_models: set[str] | None = None,
) -> PersistedConfig:
    """Load a YAML config, derive schemas, and return a PersistedConfig.

    `publish_models` sets `publish=True` on all models named in the collection.
    """
    validated = load_validated_config(path)
    models = model_derivation.derive_models(validated)

    if publish_models:
        models = [
            model.model_copy(update={"publish": True})
            if model.name in publish_models
            else model
            for model in models
        ]

    return PersistedConfig(
        models=models, routes=validated.routes, workflows=validated.workflows
    )


# Raw configs, grouped by their fate in the load -> validate pipeline.
VALID_CONFIGS = _examples_in("baseline_valid")
INVALID_ON_LOAD_CONFIGS = _examples_in("invalid_on_loading")
INVALID_ON_VALIDATION_CONFIGS = _examples_in("invalid_on_validation")

# Already-validated configs, grouped by their fate in model derivation.
VALID_MODEL_DERIVATION_CONFIGS = _examples_in("model_derivation/valid")
INVALID_MODEL_DERIVATION_CONFIGS = _examples_in("model_derivation/invalid")

# Validated configs that feed the AEM pack registry pipeline.
AEM_PACK_REGISTRY_CONFIGS = _examples_in("aem_pack_scenarios")

PRUNING_CASES = _examples_in("pruning")

with MOCK_JSON_PATH.open("r") as _fh:
    MOCK_SCHEMA = json.load(_fh)


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


def _file_schema(*, id_property: str, extra_classes: dict | None = None) -> SchemaPack:
    classes = {
        "File": {"id": {"propertyName": id_property}, "content": _FILE_CONTENT},
        **(extra_classes or {}),
    }
    return SchemaPack.model_validate({"schemapack": "4.0.0", "classes": classes})


FILE_SCHEMA = _file_schema(id_property="alias")
FILE_RENAMED_ID_SCHEMA = _file_schema(id_property="file_id")
RENAMED_ID_WITH_BACKUP_SCHEMA = _file_schema(
    id_property="file_id",
    extra_classes={
        "FileBackup": {"id": {"propertyName": "file_id"}, "content": _FILE_CONTENT}
    },
)


@dataclass
class ModelDerivationFixture:
    """Holds a loaded ValidatedConfig."""

    config: ValidatedConfig


@pytest.fixture
def model_derivation_fixture(
    request: pytest.FixtureRequest,
) -> Generator[ModelDerivationFixture]:
    """Build a ModelDerivationFixture from the YAML path passed via ``indirect``."""
    yield ModelDerivationFixture(config=load_validated_config(request.param))


@pytest.fixture
def mock_apply_workflow(
    model_derivation_fixture: ModelDerivationFixture,
) -> Generator[MagicMock]:
    """Patch `_apply_workflow` in the model_derivation module and yield the mock."""
    with patch.object(model_derivation, "_apply_workflow") as mock:
        yield mock


@pytest.fixture
def pruning_fixture(request: pytest.FixtureRequest) -> Generator[ValidatedConfig]:
    """Load a `ValidatedConfig` from the YAML's `config:` section, via `indirect`."""
    yield load_pruning_config(request.param)
