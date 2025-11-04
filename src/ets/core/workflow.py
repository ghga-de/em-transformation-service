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
from ets.ports.outbound.dao import WorkflowDaoPort

log = getLogger(__name__)


class WorkflowCore(WorkflowInboundPort):
    """A concrete implementation of the WorkflowPort abstract class"""

    def __init__(self, *, workflow_dao: WorkflowDaoPort):
        """Initialize the WorkflowCore with the required outbound ports."""
        self._workflow_dao = workflow_dao

    async def get_workflow_id(self, *, workflow_id: str):
        """Get workflow information"""
        # Dummy implementation for illustration purposes
        return await self._fetch_workflow(workflow_id=workflow_id)

    async def _fetch_workflow(self, *, workflow_id: str):
        """Get workflow by its ID"""
        try:
            workflow = await self._workflow_dao.get_by_id(workflow_id)
            log.info("Fetched workflow: %s", workflow.model_dump())
        except ResourceNotFoundError as err:
            raise ValueError(f"Workflow with ID {workflow_id} not found.") from err
        return workflow
