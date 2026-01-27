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
"""Contains functionality to load and compare service config."""

from pathlib import Path
from typing import TypeVar

from yaml import safe_load

from ets.core.models import (
    ComparisonResultChanged,
    ComparisonResultUnchanged,
    ConfigFields,
    Model,
    RawConfig,
    RawModel,
    Route,
    RouteDTO,
    Workflow,
)
from ets.ports.inbound.config_manager import ComparisonMismatchError, ConfigManagerPort
from ets.ports.outbound.dao import ModelDao, RouteDao, WorkflowDao

ConfigField = TypeVar("ConfigField", bound=Route | RouteDTO | Workflow)


class ConfigManager(ConfigManagerPort):
    """Manages loading old and new config and comparing them."""

    def __init__(
        self,
        config_path: Path,
        model_dao: ModelDao,
        route_dao: RouteDao,
        workflow_dao: WorkflowDao,
    ):
        self.config_path = config_path
        self.model_dao = model_dao
        self.route_dao = route_dao
        self.workflow_dao = workflow_dao
        self.config_fields = ConfigFields()

    async def check_config_is_different(
        self,
    ) -> ComparisonResultChanged | ComparisonResultUnchanged:
        """Check if both configs are equal.

        Returns new config fields.
        """
        try:
            await self._compare_configs()
        except ComparisonMismatchError:
            return ComparisonResultChanged(
                models=self.config_fields.new_models,
                routes=self.config_fields.routes,
                workflows=self.config_fields.workflows,
            )

        return ComparisonResultUnchanged(
            models=self.config_fields.old_models,
            routes=self.config_fields.routes,
            workflows=self.config_fields.workflows,
        )

    async def _compare_configs(self):
        """Fetch and compare config fields."""
        # runs on startup, so should be ok to just let it crash if fetching information fails.
        new_models, new_routes, new_workflows = self._parse_config_from_file()
        old_models, old_routes, old_workflows = await self._get_persisted_config()

        self.config_fields = ConfigFields(
            new_models=new_models,
            old_models=old_models,
            routes=new_routes,
            workflows=new_workflows,
        )

        _compare_models(new_models, old_models)
        _compare_entities(new_routes, old_routes)
        _compare_entities(new_workflows, old_workflows)

    def _parse_config_from_file(self):
        """Parse config fields from yaml file and sort them by name."""
        with self.config_path.open("r") as config_file:
            new_config = safe_load(config_file)

        raw_config = RawConfig.model_validate(new_config)

        models = sorted(raw_config.models, key=lambda model: model.name)
        # Validator should take care of None names, so all should be populated
        routes = sorted(raw_config.routes, key=lambda route: route.name)  # type: ignore
        workflows = sorted(raw_config.workflows, key=lambda workflow: workflow.name)

        return models, routes, workflows

    async def _get_persisted_config(self):
        """Fetch config fields from persistence layer and sort them by name."""
        models = [model async for model in self.model_dao.find_all(mapping={})]
        routes = [route async for route in self.route_dao.find_all(mapping={})]
        workflows = [
            workflow async for workflow in self.workflow_dao.find_all(mapping={})
        ]

        models = sorted(models, key=lambda model: model.name)
        # Validator should take care of None names in routes, so all should be populated
        routes = sorted(routes, key=lambda route: route.name)  # type: ignore
        workflows = sorted(workflows, key=lambda workflow: workflow.name)

        return models, routes, workflows


def _compare_entities(new: list[ConfigField], old: list[ConfigField]):
    """Comparison logic for routes and workflows.

    Assumes both lists are sorted by name.
    """
    if len(new) != len(old):
        raise ComparisonMismatchError("Different amount of config entities.")
    for n, o in zip(new, old, strict=True):
        if n != o:
            raise ComparisonMismatchError("Mismatching config entity.")


def _compare_models(new: list[RawModel], old: list[Model]):
    """Custom comparison logic for both model types.

    Assumes both lists are sorted by name.
    """
    if len(new) != len(old):
        raise ComparisonMismatchError("Different amount of models configs.")
    for new_model, old_model in zip(new, old, strict=True):
        if not (
            new_model.name == old_model.name
            and new_model.description == old_model.description
            and new_model.publish == old_model.publish
        ):
            raise ComparisonMismatchError("Mismatching fields on a model.")

        if True == new_model.is_ingress == old_model.is_ingress:
            if (
                new_model.version != old_model.version
                or new_model.schema_ != old_model.schema_
            ):
                raise ComparisonMismatchError("Mismatching fields on an EMIM model.")
        elif new_model.schema_:
            raise ComparisonMismatchError("Schemapack provided for a non EMIM model.")
