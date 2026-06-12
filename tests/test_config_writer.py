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

"""Tests for the config writer adapter."""

import pytest
from schemapack import is_equal_schemapack

from ets.core.config_validation import validate
from ets.core.model_derivation import derive_models
from ets.core.models import PersistedConfig
from tests.fixtures.examples import VALID_CONFIGS
from tests.fixtures.joint import JointFixture

BASIC_CONFIG_PATH = VALID_CONFIGS["basic_config"]


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
async def test_write_config_upserts_to_db(
    joint_fixture: JointFixture, persisted_config: PersistedConfig
):
    """write_config upserts all models, routes, and workflows to the database.

    The round-trip is verified by reading the config back from the database via
    load_config_from_db and asserting structural equality for every entity.
    """
    loader = joint_fixture.loader
    writer = joint_fixture.writer

    await writer.write_config(persisted_config)

    # Read back and assert round-trip equality
    stored_config = await loader.load_config_from_db()

    assert sorted(stored_config.models, key=lambda m: m.name) == sorted(
        persisted_config.models, key=lambda m: m.name
    )
    assert sorted(stored_config.routes, key=lambda r: r.name) == sorted(
        persisted_config.routes, key=lambda r: r.name
    )
    assert sorted(stored_config.workflows, key=lambda w: w.name) == sorted(
        persisted_config.workflows, key=lambda w: w.name
    )

    # Verify schemas are preserved correctly
    stored_by_name = {m.name: m for m in stored_config.models}
    for model in persisted_config.models:
        stored = stored_by_name[model.name]
        assert is_equal_schemapack(model.schema_, stored.schema_)


@pytest.mark.asyncio()
async def test_write_config_upsert_is_idempotent(
    joint_fixture: JointFixture, persisted_config: PersistedConfig
):
    """Calling write_config twice with the same config leaves the database unchanged.

    Confirms that upsert semantics (not insert) are used — no duplicate key errors
    and the final state matches a single write.
    """
    loader = joint_fixture.loader
    writer = joint_fixture.writer

    await writer.write_config(persisted_config)
    await writer.write_config(persisted_config)  # must not raise

    stored_config = await loader.load_config_from_db()
    assert sorted(stored_config.models, key=lambda m: m.name) == sorted(
        persisted_config.models, key=lambda m: m.name
    )
    assert sorted(stored_config.routes, key=lambda r: r.name) == sorted(
        persisted_config.routes, key=lambda r: r.name
    )
    assert sorted(stored_config.workflows, key=lambda w: w.name) == sorted(
        persisted_config.workflows, key=lambda w: w.name
    )
