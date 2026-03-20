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

"""Tests for creating and transforming AEM packs."""

from uuid import uuid4

import pytest
from schemapack.spec.datapack import DataPack

from ets.core.models import PersistedConfig
from tests.fixtures.aem_pack_registry import _SPECIFIED_AEM_ID, TEST_DATAPACK_V1
from tests.fixtures.examples import AEM_PACK_REGISTRY_CONFIGS
from tests.fixtures.joint import JointFixture


@pytest.mark.asyncio
class TestPackCreation:
    """Tests for creating and transforming AEM packs."""

    @pytest.mark.parametrize(
        "aem_id,expect_specified",
        [(None, False), (_SPECIFIED_AEM_ID, True)],
        ids=["auto_id", "specified_id"],
    )
    async def test_create_aem_pack(
        self, joint_fixture: JointFixture, aem_id, expect_specified
    ):
        """Test creating an AEM pack wrapper, with and without a pre-specified ID."""
        model_name = "TestModel"
        original_id = uuid4()
        annotation = {"key": "value"}

        aem_pack = joint_fixture.aem_pack_registry._create_aem_pack(
            aem_id=aem_id,
            model_name=model_name,
            original_id=original_id,
            data=TEST_DATAPACK_V1,
            annotation=annotation,
        )

        assert aem_pack.id is not None
        assert aem_pack.model_name == model_name
        assert aem_pack.original_id == original_id
        assert aem_pack.data == TEST_DATAPACK_V1
        assert aem_pack.annotation == annotation
        if expect_specified:
            assert aem_pack.id == _SPECIFIED_AEM_ID

    @pytest.mark.parametrize(
        "aem_pack_config",
        [AEM_PACK_REGISTRY_CONFIGS["single_route"]],
        ids=["single_route"],
        indirect=True,
    )
    async def test_apply_workflow_to_data(
        self,
        joint_fixture: JointFixture,
        aem_pack_config: PersistedConfig,
    ):
        """Test applying a workflow to transform data; empty resources are left unchanged."""
        workflow = aem_pack_config.workflows[0]
        ingress = next(m for m in aem_pack_config.models if m.is_ingress)

        result_data = joint_fixture.aem_pack_registry._apply_workflow_to_data(
            data=TEST_DATAPACK_V1,
            annotation={},
            input_schema=ingress.schema_,
            workflow=workflow,
        )

        assert isinstance(result_data, DataPack)
        assert "File" in result_data.resources
        # With no resource instances, rename_id_property leaves resources unchanged
        assert result_data.resources == TEST_DATAPACK_V1.resources
