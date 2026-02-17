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
from schemapack.spec.schemapack import SchemaPack
from yaml import safe_load

from ets.core.models import (
    ComparisonResultChanged,
    ComparisonResultUnchanged,
    ConfigFields,
    InternalModel,
    PersistedModel,
    RawConfig,
    RawModel,
    RawRoute,
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
        self.use_persisted_config = False
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
        except ComparisonMismatchError as error:
            log.info(
                f"Changes detected between configs, using new config.\nDetails:{error}"
            )
            return ComparisonResultChanged(
                models=self.config_fields.new_models,
                routes=self.config_fields.routes,
                workflows=self.config_fields.workflows,
            )

        log.info("No changes detected between configs, continuing with old config.")
        return ComparisonResultUnchanged(
            models=self.config_fields.old_models,
            routes=self.config_fields.routes,
            workflows=self.config_fields.workflows,
        )

    async def _compare_configs(self):
        """Fetch and compare config fields."""
        # runs on startup, so should be ok to just let it crash if fetching information fails.
        old_models, old_routes, old_workflows = await self._get_persisted_config()
        new_models, new_routes, new_workflows = self._parse_config_from_file()

        self.config_fields = ConfigFields(
            new_models=new_models,
            old_models=old_models,
            routes=new_routes,
            workflows=new_workflows,
        )

        # Short circuit if SchemaPack parsing failed and just use old config in that case
        if self.use_persisted_config:
            return

        log.info("Comparing models.")
        try:
            _compare_models(new_models, old_models)
        except ValueError as error:
            # Short circuit on invalid is_ingress/schema_ combinations and use the old config
            log.error(error)
            log.info("Falling back to using old config.")
            return
        log.info("Comparing routes.")
        _compare_entities(new_routes, old_routes)
        log.info("Comparing workflows.")
        _compare_entities(new_workflows, old_workflows)

    async def _get_persisted_config(self):
        """Fetch config fields from persistence layer and sort them by name."""
        log.info("Loading old config from persistence layer.")
        persisted_models = []
        async for persisted_model in self.model_dao.find_all(mapping={}):
            # Convert DTO model with serialized schema to internal representation using
            # an actual schemapack object
            schemapack = SchemaPack.model_validate(persisted_model.schema_)
            model = InternalModel(
                **persisted_model.model_dump(exclude={"schema_"}), schema_=schemapack
            )
            persisted_models.append(model)

        persisted_routes = [
            route async for route in self.route_dao.find_all(mapping={})
        ]
        workflows = [
            workflow async for workflow in self.workflow_dao.find_all(mapping={})
        ]

        routes = []
        for persisted_route in persisted_routes:
            route_dict = persisted_route.model_dump()
            routes.append(RawRoute.model_validate(route_dict))

        models = sorted(persisted_models, key=lambda model: model.name)
        # Validator should take care of None names in routes, so all should be populated
        routes = sorted(routes, key=lambda route: route.name)  # type: ignore
        workflows = sorted(workflows, key=lambda workflow: workflow.name)

        return models, routes, workflows

    def _parse_config_from_file(self):
        """Parse config fields from yaml file and sort them by name."""
        log.info("Loading new config from file.")
        with self.config_path.open("r") as config_file:
            new_config = safe_load(config_file)

        # okay to crash here if the configuration is invalid
        raw_config = RawConfig.model_validate(new_config)

        # Validator should take care of None names, so all should be populated
        routes = sorted(raw_config.routes, key=lambda route: route.name)  # type: ignore
        workflows = sorted(raw_config.workflows, key=lambda workflow: workflow.name)

        log.info("Deserializing model SchemaPack information.")
        models = []
        for raw_model in sorted(raw_config.models, key=lambda model: model.name):
            # Convert config model with serialized schema to internal representation using
            # an actual schemapack object, where applicable
            schemapack = None
            if raw_model.schema_:
                try:
                    schemapack = SchemaPack.model_validate(raw_model.schema_)
                except ValidationError:
                    log.error(
                        "Could not parse SchemaPack information for %s. Continuing with existing, persisted data instead.",
                        raw_model.name,
                    )
                    self.use_persisted_config = True
                    break
            model = InternalModel(
                **raw_model.model_dump(exclude={"schema_"}), schema_=schemapack
            )
            models.append(model)

        return models, routes, workflows


def _compare_entities[ConfigField: Route | RawRoute | Workflow](
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


def _compare_models(new: list[RawModel], old: list[PersistedModel]):
    """Custom comparison logic for both model types.

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
            old_schema = SchemaPack.model_validate(old_model.schema_)
            new_schema = SchemaPack.model_validate(new_model.schema_)
            if new_model.version != old_model.version or not is_equal_schemapack(
                old_schema, new_schema
            ):
                raise ComparisonMismatchError(
                    f"Mismatching fields on EMIM model {new_model.name}."
                )
        elif new_model.schema_:
            raise ValueError(
                f"SchemaPack provided for non EMIM model {new_model.name}."
            )
