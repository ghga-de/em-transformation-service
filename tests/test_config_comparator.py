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

"""Tests for config manager and comparison functions."""

from pathlib import Path

import pytest

from ets.core.config_comparator import ConfigComparator
from ets.core.models import Model, PersistedConfig, RawConfig
from tests.fixtures.examples import MOCK_SCHEMA, VALID_CONFIGS
from tests.fixtures.joint import JointFixture

BASIC_TEST_CONFIG_PATH = VALID_CONFIGS["basic_config"]
EXTENDED_TEST_CONFIG_PATH = VALID_CONFIGS["large_config"]


pytestmark = pytest.mark.asyncio()


@pytest.mark.parametrize(
    "new_config_path,old_config_path,changed",
    [
        (BASIC_TEST_CONFIG_PATH, BASIC_TEST_CONFIG_PATH, False),
        (BASIC_TEST_CONFIG_PATH, EXTENDED_TEST_CONFIG_PATH, True),
    ],
    ids=[
        "basic_vs_basic_unchanged",
        "basic_vs_extended_changed",
    ],
)
async def test_load_and_compare(
    changed: bool,
    new_config_path: Path,
    old_config_path: Path,
    joint_fixture: JointFixture,
):
    """Test loading the config from a yaml file and comparing with previous data
    populated from old_config_path.
    """
    loader = joint_fixture.loader
    persisted_config = await loader.load_config_from_db()
    first_raw_config = loader.load_config_from_file(old_config_path)

    config_manager = ConfigComparator(
        raw_config=first_raw_config, persisted_config=persisted_config
    )
    result = config_manager.compare_configs()

    # Populate DB from config, mocking some fields to conform to DTO
    for order, raw_model in enumerate(result.models):
        # mock order for now, replace once the validation and derivation code is implemented
        # model_dump() serializes schema_ to a JSON-compatible dict via the field_serializer
        model_dict = raw_model.model_dump()
        if not model_dict["schema_"]:
            # mock model derivation by simply inserting a dummy schema
            model_dict["schema_"] = MOCK_SCHEMA
        model_dict["order"] = order

        model = Model.model_validate(model_dict)
        await joint_fixture.daos.model_dao.insert(model)

    for route in result.routes:
        await joint_fixture.daos.route_dao.insert(route)

    for workflow in result.workflows:
        await joint_fixture.daos.workflow_dao.insert(workflow)

    persisted_config = await loader.load_config_from_db()
    second_raw_config = loader.load_config_from_file(new_config_path)
    # config_manager.raw_config = second_raw_config
    config_comparator = ConfigComparator(
        raw_config=second_raw_config, persisted_config=persisted_config
    )
    result = config_manager.compare_configs()
    assert (
        isinstance(result, RawConfig)
        if changed
        else isinstance(result, PersistedConfig)
    )
