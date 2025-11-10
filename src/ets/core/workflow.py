# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""Core business logic for managing workflows."""

from logging import getLogger

from hexkit.protocols.dao import ResourceNotFoundError

from ets.ports.inbound.workflow import WorkflowInboundPort
from ets.ports.outbound.dao import DataDaoPort, WorkflowDaoPort

from .models import DataDto, WorkflowDto

log = getLogger(__name__)


class WorkflowCore(WorkflowInboundPort):
    """A concrete implementation of the WorkflowPort abstract class"""

    def __init__(self, *, workflow_dao: WorkflowDaoPort, data_dao: DataDaoPort):
        """Initialize the WorkflowCore with the required outbound ports."""
        self._workflow_dao = workflow_dao
        self._data_dao = data_dao

    async def get_workflow(self, *, workflow_id: str):
        """Get workflow information"""
        # Dummy implementation for illustration purposes
        return await self._fetch_workflow(workflow_id=workflow_id)

    async def transform_data(self, *, workflow_id: str):
        """Transform data information"""
        # workflow = await self._fetch_workflow(workflow_id=workflow_id)
        data = await self._fetch_data(workflow_id=workflow_id)
        return data

    async def _fetch_workflow(self, *, workflow_id: str):
        """Get workflow by its ID"""
        try:
            workflow = await self._workflow_dao.get_by_id(workflow_id)
            log.info("Fetched workflow: %s", workflow.model_dump())
        except ResourceNotFoundError as err:
            raise ValueError(f"Workflow with ID {workflow_id} not found.") from err
        return workflow.model_dump(mode="json")["workflow"]

    async def _fetch_data(self, *, workflow_id: str):
        """Get data by its workflow_id"""
        try:
            data = await self._data_dao.get_by_id(workflow_id)
            log.info("Fetched workflow: %s", data.model_dump())
        except ResourceNotFoundError as err:
            raise ValueError(f"Data with workflow ID {workflow_id} not found.") from err
        return data.model_dump(mode="json")

    async def whatever(self):
        """Just a dummy method to illustrate further expansion of the core logic."""
        workflow_example = WorkflowDto(
            workflow_id="example_id", workflow="example_workflow"
        )
        await self._workflow_dao.upsert(workflow_example)

    async def whatever_data(self):
        """Just a dummy method to illustrate further expansion of the core logic."""
        data_example = DataDto(
            workflow_id="example_id", data="example_data", model="example_model"
        )
        await self._data_dao.upsert(data_example)
