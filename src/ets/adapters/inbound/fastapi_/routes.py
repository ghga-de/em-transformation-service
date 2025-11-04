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

"""FastAPI routes for workflow management."""

from fastapi import APIRouter, status

from ets.adapters.inbound.fastapi_.dummies import WorkflowDummy

router = APIRouter()


@router.get("/health", summary="Health check endpoint", status_code=status.HTTP_200_OK)
async def health_check():
    """Test endpoint to check if the service is running."""
    return {"status": "healthy"}


@router.post(
    "/workflows",
    summary="Submit a workflow id for execution",
    status_code=status.HTTP_201_CREATED,
)
async def submit_workflow(workflow: WorkflowDummy, workflow_id: str) -> str:
    """Submit a workflow for execution."""
    return await workflow.get_workflow_id(workflow_id=workflow_id)
