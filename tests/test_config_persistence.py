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

from ets.adapters.outbound.config_loader import ConfigLoaderAdapter
from ets.core.config_comparator import ConfigComparator
from ets.core.config_validator import ConfigValidator
from ets.core.model_derivation import ModelDeriver
from ets.core.models import Model, PersistedConfig, RawConfig
from tests.fixtures.config_examples import MOCK_SCHEMA, VALID_CONFIGS
from tests.fixtures.joint import JointFixture

BASIC_CONFIG_PATH = VALID_CONFIGS["basic_config"]
EXTENDED_CONFIG_PATH = VALID_CONFIGS["large_config"]


def _model_with_mocked_schema(raw_model, order: int) -> Model:
    """Build a Model from a RawConfig.models entry, filling missing schemas with the mock."""
    model_dict = raw_model.model_dump()
    if not model_dict["schema_"]:
        model_dict["schema_"] = MOCK_SCHEMA
    model_dict["order"] = order
    return Model.model_validate(model_dict)


@pytest.fixture
def persisted_config(joint_fixture: JointFixture) -> PersistedConfig:
    """Produce a fully resolved PersistedConfig through the real pipeline."""
    raw_config = joint_fixture.loader.load_config_from_file(BASIC_CONFIG_PATH)
    validated_config = ConfigValidator().validate(raw_config)
    derived_models = ModelDeriver(config=validated_config).derive_models()
    return PersistedConfig(
        models=derived_models,
        routes=validated_config.routes,
        workflows=validated_config.workflows,
    )


@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "new_config_path,old_config_path,changed",
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
    """Loading from a YAML and comparing against the persisted state returns the
    expected RawConfig (changed) or PersistedConfig (unchanged) variant.
    """
    loader = joint_fixture.loader
    persisted = await loader.load_config_from_db()
    first_raw = loader.load_config_from_file(old_config_path)

    result = ConfigComparator(
        raw_config=first_raw, persisted_config=persisted
    ).compare_configs()

    daos = joint_fixture.daos
    for order, raw_model in enumerate(result.models):
        await daos.model_dao.insert(_model_with_mocked_schema(raw_model, order))
    for route in result.routes:
        await daos.route_dao.insert(route)
    for workflow in result.workflows:
        await daos.workflow_dao.insert(workflow)

    persisted = await loader.load_config_from_db()
    second_raw = loader.load_config_from_file(new_config_path)
    result = ConfigComparator(
        raw_config=second_raw, persisted_config=persisted
    ).compare_configs()

    assert isinstance(result, RawConfig if changed else PersistedConfig)


def test_compare_is_order_insensitive(loader: ConfigLoaderAdapter):
    """Ensure list ordering does not affect config comparison outcome."""
    raw_config = loader.load_config_from_file(BASIC_CONFIG_PATH)

    persisted_models = [
        _model_with_mocked_schema(raw_model, order)
        for order, raw_model in enumerate(raw_config.models)
    ]

    reordered_persisted = PersistedConfig(
        models=list(reversed(persisted_models)),
        routes=list(reversed(raw_config.routes)),
        workflows=list(reversed(raw_config.workflows)),
    )

    result = ConfigComparator(
        raw_config=RawConfig(
            models=raw_config.models,
            routes=raw_config.routes,
            workflows=raw_config.workflows,
        ),
        persisted_config=reordered_persisted,
    ).compare_configs()

    assert isinstance(result, PersistedConfig)


@pytest.mark.asyncio()
async def test_write_config_upserts_to_db(
    joint_fixture: JointFixture, persisted_config: PersistedConfig
):
    """write_config upserts all entities; round-trip via load_config_from_db preserves them."""
    await joint_fixture.writer.write_config(persisted_config)

    stored_config = await joint_fixture.loader.load_config_from_db()

    assert sorted(stored_config.models, key=lambda m: m.name) == sorted(
        persisted_config.models, key=lambda m: m.name
    )
    assert sorted(stored_config.routes, key=lambda r: r.name) == sorted(
        persisted_config.routes, key=lambda r: r.name
    )
    assert sorted(stored_config.workflows, key=lambda w: w.name) == sorted(
        persisted_config.workflows, key=lambda w: w.name
    )

    stored_by_name = {m.name: m for m in stored_config.models}
    for model in persisted_config.models:
        assert is_equal_schemapack(model.schema_, stored_by_name[model.name].schema_)


@pytest.mark.asyncio()
async def test_write_config_upsert_is_idempotent(
    joint_fixture: JointFixture, persisted_config: PersistedConfig
):
    """Calling write_config twice with the same config leaves the DB unchanged."""
    await joint_fixture.writer.write_config(persisted_config)
    await joint_fixture.writer.write_config(persisted_config)  # must not raise

    stored_config = await joint_fixture.loader.load_config_from_db()
    assert sorted(stored_config.models, key=lambda m: m.name) == sorted(
        persisted_config.models, key=lambda m: m.name
    )
    assert sorted(stored_config.routes, key=lambda r: r.name) == sorted(
        persisted_config.routes, key=lambda r: r.name
    )
    assert sorted(stored_config.workflows, key=lambda w: w.name) == sorted(
        persisted_config.workflows, key=lambda w: w.name
    )
