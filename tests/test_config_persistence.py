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

"""DB-backed tests for writing, reading, and comparing persisted configs."""

from pathlib import Path

import pytest
from schemapack import is_equal_schemapack

from emts.adapters.outbound.config_loader import ConfigLoaderAdapter
from emts.core.config_comparison import compare_configs
from emts.core.config_validation import validate
from emts.core.model_derivation import derive_models
from emts.core.models import Model, ModelBase, PersistedConfig, RawConfig
from tests.fixtures.examples import MOCK_SCHEMA, VALID_CONFIGS
from tests.fixtures.joint import JointFixture

BASIC_CONFIG_PATH = VALID_CONFIGS["basic_config"]
EXTENDED_CONFIG_PATH = VALID_CONFIGS["large_config"]


def _model_with_mocked_schema(raw_model: ModelBase, order: int) -> Model:
    """Build a Model from a Raw/persisted entry, filling missing schemas with the mock."""
    model_dict = raw_model.model_dump()
    if not model_dict["schema_"]:
        model_dict["schema_"] = MOCK_SCHEMA
    model_dict["order"] = order
    return Model.model_validate(model_dict)


def _assert_same_config(a: PersistedConfig, b: PersistedConfig) -> None:
    """Compare two PersistedConfigs by sorted entity names, ignoring list order."""
    assert sorted(a.models, key=lambda m: m.name) == sorted(
        b.models, key=lambda m: m.name
    )
    assert sorted(a.routes, key=lambda r: r.name) == sorted(
        b.routes, key=lambda r: r.name
    )
    assert sorted(a.workflows, key=lambda w: w.name) == sorted(
        b.workflows, key=lambda w: w.name
    )


@pytest.fixture
def persisted_config(joint_fixture: JointFixture) -> PersistedConfig:
    """Produce a fully resolved PersistedConfig through the real pipeline."""
    raw_config = joint_fixture.loader.load_config_from_file(BASIC_CONFIG_PATH)
    validated_config = validate(raw_config)
    derived_models = derive_models(validated_config)
    return PersistedConfig(
        models=derived_models,
        routes=validated_config.routes,
        workflows=validated_config.workflows,
    )


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "new_config_path, old_config_path, changed",
    [
        (BASIC_CONFIG_PATH, BASIC_CONFIG_PATH, False),
        (BASIC_CONFIG_PATH, EXTENDED_CONFIG_PATH, True),
    ],
    ids=["basic_vs_basic_unchanged", "basic_vs_extended_changed"],
)
async def test_load_and_compare(
    changed: bool,
    new_config_path: Path,
    old_config_path: Path,
    joint_fixture: JointFixture,
):
    """Ensure loading from YAML and comparing against the persisted state returns the
    expected RawConfig (changed) or PersistedConfig (unchanged) variant.
    """
    loader = joint_fixture.loader

    first_raw = loader.load_config_from_file(old_config_path)
    seed = compare_configs(first_raw, await loader.load_config_from_db())

    await joint_fixture.insert_config(
        PersistedConfig(
            models=[
                _model_with_mocked_schema(raw_model, order)
                for order, raw_model in enumerate(seed.models)
            ],
            routes=seed.routes,
            workflows=seed.workflows,
        )
    )

    second_raw = loader.load_config_from_file(new_config_path)
    result = compare_configs(second_raw, await loader.load_config_from_db())
    assert isinstance(result, RawConfig if changed else PersistedConfig)


def test_compare_is_order_insensitive(loader: ConfigLoaderAdapter):
    """Ensure list ordering does not affect config comparison outcome."""
    raw_config = loader.load_config_from_file(BASIC_CONFIG_PATH)
    persisted_models = [
        _model_with_mocked_schema(rm, order)
        for order, rm in enumerate(raw_config.models)
    ]

    reordered = PersistedConfig(
        models=list(reversed(persisted_models)),
        routes=list(reversed(raw_config.routes)),
        workflows=list(reversed(raw_config.workflows)),
    )
    assert isinstance(compare_configs(raw_config, reordered), PersistedConfig)


@pytest.mark.asyncio()
@pytest.mark.parametrize("write_twice", [False, True], ids=["once", "idempotent_twice"])
async def test_write_config_round_trip(
    joint_fixture: JointFixture,
    persisted_config: PersistedConfig,
    write_twice: bool,
):
    """Ensure write_config upserts entities and the round-trip preserves them. Calling
    twice with the same config must not raise (upsert semantics).
    """
    await joint_fixture.writer.write_config(persisted_config)
    if write_twice:
        await joint_fixture.writer.write_config(persisted_config)

    stored_config = await joint_fixture.loader.load_config_from_db()
    _assert_same_config(stored_config, persisted_config)

    stored_by_name = {m.name: m for m in stored_config.models}
    for model in persisted_config.models:
        assert is_equal_schemapack(model.schema_, stored_by_name[model.name].schema_)
