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

from schemapack import is_equivalent_schemapack
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
            log.critical(error)
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

        models = []
        for raw_model in sorted(raw_config.models, key=lambda model: model.name):
            # Convert config model with serialized schema to internal representation using
            # an actual schemapack object, where applicable
            schemapack = None
            if raw_model.schema_:
                schemapack = SchemaPack.model_validate(raw_model.schema_)
            model = InternalModel(
                **raw_model.model_dump(exclude={"schema_"}), schema_=schemapack
            )
            models.append(model)

        # Validator should take care of None names, so all should be populated
        routes = sorted(raw_config.routes, key=lambda route: route.name)  # type: ignore
        workflows = sorted(raw_config.workflows, key=lambda workflow: workflow.name)

        return models, routes, workflows

    async def _get_persisted_config(self):
        """Fetch config fields from persistence layer and sort them by name."""
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
            raise ComparisonMismatchError("Mismatching config entity.")


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
            if new_model.version != old_model.version or not is_equivalent_schemapack(
                old_schema, new_schema
            ):
                raise ComparisonMismatchError(
                    f"Mismatching fields on EMIM model {new_model.name}."
                )
        elif new_model.schema_:
            raise ComparisonMismatchError(
                f"SchemaPack provided for non EMIM model {new_model.name}."
            )
