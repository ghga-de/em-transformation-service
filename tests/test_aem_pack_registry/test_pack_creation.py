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

"""Tests for AEM pack related utility functions called in during processing."""

from uuid import uuid4

import pytest
from pydantic import UUID4
from schemapack.spec.datapack import DataPack

from tests.fixtures.aem_pack_registry import (
    EXPECTED_AEM_ID,
    TEST_DATAPACK,
    load_aem_pack_config,
)
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "aem_id,expected_aem_id",
    [(None, False), (EXPECTED_AEM_ID, True)],
    ids=["auto_id", "specified_id"],
)
async def test_create_aem_pack(
    joint_fixture: JointFixture, aem_id: UUID4, expected_aem_id: UUID4
):
    """Test creating an AEM pack wrapper, with and without a pre-specified ID."""
    model_name = "TestModel"
    original_id = uuid4()
    annotation: dict = {}

    aem_pack = joint_fixture.aem_pack_registry._create_aem_pack(
        aem_id=aem_id,
        model_name=model_name,
        original_id=original_id,
        data=TEST_DATAPACK,
        annotation=annotation,
    )

    assert aem_pack.id is not None
    assert aem_pack.model_name == model_name
    assert aem_pack.original_id == original_id
    assert aem_pack.data == TEST_DATAPACK
    assert aem_pack.annotation == annotation
    if expected_aem_id:
        assert aem_pack.id == EXPECTED_AEM_ID


async def test_apply_workflow_to_data(joint_fixture: JointFixture):
    """Test applying a workflow to transform data; empty resources are left unchanged."""
    aem_pack_config = load_aem_pack_config(AEM_PACK_REGISTRY_CONFIGS["single_route"])
    workflow = aem_pack_config.workflows[0]
    ingress = next(model for model in aem_pack_config.models if model.is_ingress)

    result_data = joint_fixture.aem_pack_registry._apply_workflow_to_data(
        data=TEST_DATAPACK,
        annotation={},
        input_schema=ingress.schema_,
        workflow=workflow,
    )

    assert isinstance(result_data, DataPack)
    assert "File" in result_data.resources
    # With no resource instances, rename_id_property leaves resources unchanged
    assert result_data.resources == TEST_DATAPACK.resources
