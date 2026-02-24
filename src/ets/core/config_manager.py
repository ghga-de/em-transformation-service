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

"""Contains functionality to compare transformation config."""

import logging

from schemapack import is_equal_schemapack

from ets.core.models import (
    ComparisonResultChanged,
    ComparisonResultUnchanged,
    Model,
    RawConfig,
    RawModel,
    Route,
    Workflow,
)
from ets.ports.inbound.config_manager import ComparisonMismatchError, ConfigManagerPort
from ets.ports.outbound.dao import ModelDao, RouteDao, WorkflowDao

log = logging.getLogger(__name__)


class ConfigManager(ConfigManagerPort):
    """Loads the config file, fetches persisted config, and compares them to detect changes."""

    def __init__(
        self,
        raw_config: RawConfig,
        model_dao: ModelDao,
        route_dao: RouteDao,
        workflow_dao: WorkflowDao,
    ):
        self.raw_config = raw_config
        self.model_dao = model_dao
        self.route_dao = route_dao
        self.workflow_dao = workflow_dao

    async def compare_configs(
        self,
    ) -> ComparisonResultChanged | ComparisonResultUnchanged:
        """Compare new config with the persisted one.

        Returns:
            ComparisonResultChanged: when the configs differ, containing the new models, routes, and workflows.
            ComparisonResultUnchanged: when the configs are equal, containing the persisted models, routes, and workflows.
        """
        old_models, old_routes, old_workflows = await self._get_persisted_config()

        new_models = self.raw_config.models
        new_routes = self.raw_config.routes
        new_workflows = self.raw_config.workflows

        try:
            log.info("Comparing models.")
            _compare_models(new_models, old_models)
            log.info("Comparing routes.")
            _compare_entities(new_routes, old_routes)
            log.info("Comparing workflows.")
            _compare_entities(new_workflows, old_workflows)
        except ComparisonMismatchError as error:
            log.info(
                f"Changes detected between configs, using new config.\nDetails:{error}"
            )
            return ComparisonResultChanged(
                models=new_models, routes=new_routes, workflows=new_workflows
            )

        log.info("No changes detected between configs, continuing with old config.")
        return ComparisonResultUnchanged(
            models=old_models, routes=old_routes, workflows=old_workflows
        )

    async def _get_persisted_config(self):
        """Fetch config fields from persistence layer and sort them by name."""
        log.info("Fetching old config from persistence layer.")
        models = [model async for model in self.model_dao.find_all(mapping={})]
        routes = [route async for route in self.route_dao.find_all(mapping={})]
        workflows = [
            workflow async for workflow in self.workflow_dao.find_all(mapping={})
        ]

        models = sorted(models, key=lambda model: model.name)
        routes = sorted(routes, key=lambda route: route.name)
        workflows = sorted(workflows, key=lambda workflow: workflow.name)

        return models, routes, workflows


def _compare_entities[ConfigField: Route | Workflow](
    new: list[ConfigField], old: list[ConfigField]
):
    """Comparison logic for routes and workflows.

    Assumes both lists are sorted by name.
    """
    if len(new) != len(old):
        raise ComparisonMismatchError("Different amount of config entities.")
    for n, o in zip(new, old, strict=True):
        if n != o:
            raise ComparisonMismatchError(f"Mismatching config entity: {n.name}.")


def _compare_models(new: list[RawModel], old: list[Model]):
    """Custom comparison logic for new (file) models vs persisted models.

    Assumes both lists are sorted by name.
    """
    if len(new) != len(old):
        raise ComparisonMismatchError("Different amount of model configs.")

    for new_model, old_model in zip(new, old, strict=True):
        # compare model attributes except the schema_s
        if not (
            new_model.name == old_model.name
            and new_model.description == old_model.description
            and new_model.publish == old_model.publish
            and new_model.version == old_model.version
            and new_model.is_ingress == old_model.is_ingress
        ):
            raise ComparisonMismatchError(
                f"Mismatching fields on model {new_model.name}."
            )
        # compare model schema_s

        # The old config models do not have any empty schema
        # The new config models have empty schemas if is_ingress is not True.
        # We should not compare derived models with empty schemas in the new config
        # if both are not is_ingress=true
        new_schema = new_model.schema_
        old_schema = old_model.schema_

        if new_schema and not is_equal_schemapack(old_schema, new_schema):
            raise ComparisonMismatchError(
                f"Mismatching schema on EMIM model {new_model.name}."
            )
