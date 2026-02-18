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
"""Contains functionality to load and compare service config."""

import logging
from pathlib import Path

from pydantic import ValidationError
from schemapack import is_equal_schemapack
from yaml import safe_load

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
from ets.ports.outbound.dao import PersistedModelDao, RouteDao, WorkflowDao

log = logging.getLogger(__name__)


class ConfigManager(ConfigManagerPort):
    """Loads the config file, fetches persisted config, and compares them to detect changes."""

    def __init__(
        self,
        config_path: Path,
        model_dao: PersistedModelDao,
        route_dao: RouteDao,
        workflow_dao: WorkflowDao,
    ):
        self.config_path = config_path
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
        parsed = self._parse_config_from_file()

        if parsed is None:
            log.info("SchemaPack parsing failed, continuing with old config.")
            return ComparisonResultUnchanged(
                models=old_models, routes=old_routes, workflows=old_workflows
            )

        new_models, new_routes, new_workflows = parsed

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
        except ValueError as error:
            log.error(error)
            log.info(
                "Invalid is_ingress/schema_ combination, falling back to old config."
            )
            return ComparisonResultUnchanged(
                models=old_models, routes=old_routes, workflows=old_workflows
            )

        log.info("No changes detected between configs, continuing with old config.")
        return ComparisonResultUnchanged(
            models=old_models, routes=old_routes, workflows=old_workflows
        )

    async def _get_persisted_config(self):
        """Fetch config fields from persistence layer and sort them by name."""
        log.info("Loading old config from persistence layer.")
        models = [model async for model in self.model_dao.find_all(mapping={})]
        routes = [route async for route in self.route_dao.find_all(mapping={})]
        workflows = [
            workflow async for workflow in self.workflow_dao.find_all(mapping={})
        ]

        models = sorted(models, key=lambda model: model.name)
        routes = sorted(routes, key=lambda route: route.name)
        workflows = sorted(workflows, key=lambda workflow: workflow.name)

        return models, routes, workflows

    def _parse_config_from_file(
        self,
    ) -> tuple[list[RawModel], list[Route], list[Workflow]] | None:
        """Parse config fields from yaml file and sort them by name.

        Returns ``None`` if SchemaPack validation fails, signalling that the
        caller should fall back to the already-persisted configuration.
        """
        log.info("Loading new config from file.")
        with self.config_path.open("r") as config_file:
            new_config = safe_load(config_file)

        try:
            raw_config = RawConfig.model_validate(new_config)
        except ValidationError as exc:
            schema_errors = [
                err for err in exc.errors() if "schema_" in err.get("loc", ())
            ]
            if len(schema_errors) != len(exc.errors()):
                # Structural config errors should propagate, not be silently swallowed
                raise
            log.error(
                "Could not parse SchemaPack information. Falling back to old config.",
            )
            return None

        routes = sorted(raw_config.routes, key=lambda route: route.name)
        workflows = sorted(raw_config.workflows, key=lambda workflow: workflow.name)
        models = sorted(raw_config.models, key=lambda m: m.name)

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
        if not (
            new_model.name == old_model.name
            and new_model.description == old_model.description
            and new_model.publish == old_model.publish
        ):
            raise ComparisonMismatchError(
                f"Mismatching fields on model {new_model.name}."
            )

        if True == new_model.is_ingress == old_model.is_ingress:
            if not new_model.schema_:
                raise ValueError(f"Missing SchemaPack on EMIM model {new_model.name}.")
            if new_model.version != old_model.version or not is_equal_schemapack(
                old_model.schema_, new_model.schema_
            ):
                raise ComparisonMismatchError(
                    f"Mismatching fields on EMIM model {new_model.name}."
                )
        elif new_model.schema_:
            raise ValueError(
                f"SchemaPack provided for non EMIM model {new_model.name}."
            )
