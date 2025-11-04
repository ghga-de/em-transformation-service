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

"""Dependency injection and preparation"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext

from fastapi import FastAPI
from hexkit.providers.mongodb import MongoDbDaoFactory

from ets.adapters.inbound.fastapi_ import dummies
from ets.adapters.inbound.fastapi_.configure import get_configured_app
from ets.adapters.outbound.dao import get_workflow_dao
from ets.config import Config
from ets.core.workflow import WorkflowCore
from ets.ports.inbound.workflow import WorkflowInboundPort


@asynccontextmanager
async def prepare_core(*, config: Config) -> AsyncGenerator[WorkflowInboundPort]:
    """Constructs and initializes all core components and their outbound dependencies."""
    async with MongoDbDaoFactory.construct(config=config) as dao_factory:
        workflow_dao = await get_workflow_dao(dao_factory=dao_factory)
        workflow = WorkflowCore(workflow_dao=workflow_dao)
        yield workflow


def prepare_code_with_overwrite(
    *, config: Config, workflow_overwrite: WorkflowInboundPort | None = None
):
    """Resolve the prepare_core context manager based on config and override (if any)."""
    return (
        nullcontext(workflow_overwrite)
        if workflow_overwrite
        else prepare_core(config=config)
    )


@asynccontextmanager
async def prepare_rest_app(*, config: Config) -> AsyncGenerator[FastAPI]:
    """Construct and initialize an REST API app along with all its dependencies.
    By default, the core dependencies are automatically prepared but you can also
    provide them using the bank_override parameter.
    """
    app = get_configured_app(config=config)
    async with prepare_code_with_overwrite(config=config) as workflow:
        app.dependency_overrides[dummies.workflow_dummy] = lambda: workflow
        yield app
