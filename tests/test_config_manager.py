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

import json
from contextlib import nullcontext
from pathlib import Path
from typing import cast

import pytest
from pydantic import ValidationError

from ets.core.models import (
    ComparisonResultChanged,
    ComparisonResultUnchanged,
    PersistedModel,
    Route,
)
from tests.fixtures.joint import JointFixture
from tests.fixtures.utils import BASE_DIR

CONFIG_DIR = BASE_DIR / "input_configs"

BASIC_TEST_CONFIG_PATH = CONFIG_DIR / "basic_test_config.yaml"
EXTENDED_TEST_CONFIG_PATH = CONFIG_DIR / "test_config.yaml"
INVALID_TEST_CONFIG_PATH = CONFIG_DIR / "invalid_config.yaml"

MOCK_JSON_PATH = BASE_DIR / "mock_schema.json"

with MOCK_JSON_PATH.open("r") as file:
    MOCK_SCHEMA = json.load(file)

pytestmark = pytest.mark.asyncio()


@pytest.mark.parametrize(
    "config_path,should_pass",
    [
        (BASIC_TEST_CONFIG_PATH, True),
        (EXTENDED_TEST_CONFIG_PATH, True),
        (INVALID_TEST_CONFIG_PATH, False),
    ],
)
async def test_loading_configs(
    config_path: Path, should_pass: bool, joint_fixture: JointFixture
) -> None:
    """Test loading the config from a yaml file and comparing with no previous data persisted."""
    config_manager = joint_fixture.config_manager
    # directly patch instance attribute for now, find a better way once everything is
    # wired correctly
    config_manager.config_path = config_path  # type: ignore
    with nullcontext() if should_pass else pytest.raises(ValidationError):
        result = await config_manager.check_config_is_different()
    if should_pass:
        assert isinstance(result, ComparisonResultChanged)


@pytest.mark.parametrize(
    "new_config_path,old_config_path,changed",
    [
        (BASIC_TEST_CONFIG_PATH, BASIC_TEST_CONFIG_PATH, False),
        (BASIC_TEST_CONFIG_PATH, EXTENDED_TEST_CONFIG_PATH, True),
    ],
)
async def test_load_and_compare(
    changed: bool,
    new_config_path: Path,
    old_config_path: Path,
    joint_fixture: JointFixture,
):
    """Test loading the config from a yaml file and comparing with previous data populated from old_config_path."""
    config_manager = joint_fixture.config_manager
    # directly patch instance attribute for now, find a better way once everything is
    # wired correctly
    config_manager.config_path = old_config_path  # type: ignore
    result = await config_manager.check_config_is_different()

    # Populate DB from config, mocking some fields to conform to DTO
    for order, raw_model in enumerate(result.models):
        # mock order for now, replace once the validation and derivation code is implemented
        schema = raw_model.schema_  # type: ignore[attr-defined] # mypy false positive
        model_dict = raw_model.model_dump(exclude={"schema_"})
        if not schema:
            # mock model derivation by simply inserting a dummy schema
            model_dict["schema_"] = MOCK_SCHEMA
        else:
            # SchemaPack objects need to be serialized with mode='json' to get JSON-compatible types
            model_dict["schema_"] = json.loads(schema.model_dump_json())
        model_dict["order"] = order

        model = PersistedModel.model_validate(model_dict)
        await joint_fixture.daos.model_dao.insert(model)

    for route in result.routes:
        # Should be equivalent after validation
        await joint_fixture.daos.route_dao.insert(cast(Route, route))

    for workflow in result.workflows:
        await joint_fixture.daos.workflow_dao.insert(workflow)

    config_manager.config_path = new_config_path  # type: ignore
    result = await config_manager.check_config_is_different()
    assert (
        isinstance(result, ComparisonResultChanged)
        if changed
        else isinstance(result, ComparisonResultUnchanged)
    )
