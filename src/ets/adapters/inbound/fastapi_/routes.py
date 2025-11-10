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
from ets.core import models

router = APIRouter()


@router.get("/health", summary="Health check endpoint", status_code=status.HTTP_200_OK)
async def health_check():
    """Test endpoint to check if the service is running."""
    return {"status": "healthy"}


@router.get(
    "/workflows/{workflow_id}",
    summary="Get a workflow id for execution",
    status_code=status.HTTP_201_CREATED,
    response_model=models.WorkflowDto,
)
async def get_workflow(workflow: WorkflowDummy, workflow_id: str) -> str:
    """Submit a workflow for execution."""
    return await workflow.get_workflow(workflow_id=workflow_id)


@router.get(
    "/data/{workflow_id}",
    summary="Get data for a specific workflow",
    status_code=status.HTTP_200_OK,
    response_model=models.DataDto,
)
async def get_data(workflow: WorkflowDummy, workflow_id: str) -> str:
    """Submit a workflow for execution."""
    return await workflow.transform_data(workflow_id=workflow_id)


@router.post(
    "/workflows/dummy",
    summary="Just a dummy endpoint to illustrate further expansion of the routes.",
)
async def whatever(workflow: WorkflowDummy) -> None:
    """Just a dummy endpoint to illustrate further expansion of the routes."""
    await workflow.whatever()


@router.post(
    "/data/dummy",
    summary="Just a dummy endpoint to illustrate further expansion of the routes.",
)
async def whatever_data(data: WorkflowDummy) -> None:
    """Just a dummy endpoint to illustrate further expansion of the routes."""
    await data.whatever_data()
