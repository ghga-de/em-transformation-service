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
"""TODO"""

import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar
from xmlrpc.client import boolean

from pydantic import BaseModel
from yaml import safe_load

from ets.core.models import Model, RawConfig, RawModel, Route, Workflow
from ets.ports.outbound.dao import ModelDao, RouteDao, WorkflowDao

ConfigField = TypeVar("ConfigField", bound=BaseModel)


@dataclass
class ComparisonResult:
    """TODO"""

    changed: boolean
    models: list[RawModel]
    routes: list[Route]
    workflows: list[Workflow]


class ConfigManager:
    """TODO"""

    def __init__(
        self,
        config_path: Path,
        model_dao: ModelDao,
        route_dao: RouteDao,
        workflow_dao: WorkflowDao,
    ):
        """TODO"""
        self.config_path = config_path
        self.model_dao = model_dao
        self.route_dao = route_dao
        self.workflow_dao = workflow_dao
        self.comparison_result: ComparisonResult | None = None

    async def is_new_config_different(self):
        """TODO"""
        with contextlib.suppress(ValueError):
            await self.compare_configs()

        return self.comparison_result

    async def compare_configs(self):
        """TODO"""
        new_models, new_routes, new_workflows = self.parse_config_from_file()
        old_models, old_routes, old_workflows = await self.get_persisted_config()

        self.comparison_result = ComparisonResult(
            changed=True, models=new_models, routes=new_routes, workflows=new_workflows
        )

        compare_models(new_models, old_models)
        compare_entities(new_routes, old_routes)
        compare_entities(new_workflows, old_workflows)

        self.comparison_result.changed = False

    def parse_config_from_file(self):
        """TODO"""
        with self.config_path.open("r") as config_file:
            new_config = safe_load(config_file)

        raw_config = RawConfig.model_validate(new_config)

        models = sorted(raw_config.models, key=lambda model: model.name)
        # Validator should take care of None names, so all should be populated
        routes = sorted(raw_config.routes, key=lambda route: route.name)  # type: ignore
        workflows = sorted(raw_config.workflows, key=lambda workflow: workflow.name)

        return models, routes, workflows

    async def get_persisted_config(self):
        """TODO"""
        models = [model async for model in self.model_dao.find_all(mapping={})]
        routes = [route async for route in self.route_dao.find_all(mapping={})]
        workflows = [
            workflow async for workflow in self.workflow_dao.find_all(mapping={})
        ]

        models = sorted(models, key=lambda model: model.name)
        # Validator should take care of None names, so all should be populated
        routes = sorted(routes, key=lambda route: route.name)  # type: ignore
        workflows = sorted(workflows, key=lambda workflow: workflow.name)

        return models, routes, workflows


def compare_entities(new: list[ConfigField], old: list[ConfigField]):
    """TODO"""
    if len(new) != len(old):
        raise ValueError("")
    for n, o in zip(new, old, strict=True):
        if n != o:
            raise ValueError("")


def compare_models(new: list[RawModel], old: list[Model]):
    """TODO"""
    if len(new) != len(old):
        raise ValueError("")
    for new_model, old_model in zip(new, old, strict=True):
        if (
            new_model.schema_ != None
            or new_model.name != old_model.name
            or new_model.description != old_model.description
            or new_model.publish != old_model.publish
        ):
            raise ValueError()

        if True == new_model.is_ingress == old_model.is_ingress and (
            new_model.version != old_model.version
            or new_model.schema_ != old_model.schema_
        ):
            raise ValueError()
