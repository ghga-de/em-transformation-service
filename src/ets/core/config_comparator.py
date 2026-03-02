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

"""Contains functionality to compare transformation configs."""

import logging

from schemapack import is_equal_schemapack

from ets.core.models import (
    Model,
    PersistedConfig,
    RawConfig,
    RawModel,
    Route,
    Workflow,
)
from ets.ports.inbound.config_comparator import (
    ComparisonMismatchError,
    ConfigComparatorPort,
)

log = logging.getLogger(__name__)


class ConfigComparator(ConfigComparatorPort):
    """Loads the config file, fetches persisted config, and compares them to detect changes."""

    def __init__(self, raw_config: RawConfig, persisted_config: PersistedConfig):
        self.raw_config = raw_config
        self.persisted_config = persisted_config

    def compare_configs(
        self,
    ) -> PersistedConfig | RawConfig:
        """Compare new config with the persisted one.

        Returns:
            RawConfig: when the configs differ, containing the new models, routes, and workflows.
            PersistedConfig: when the configs are equal, containing the persisted models, routes, and workflows.
        """
        old_models = self.persisted_config.models
        old_routes = self.persisted_config.routes
        old_workflows = self.persisted_config.workflows

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
            return self.raw_config

        log.info("No changes detected between configs, continuing with old config.")
        return self.persisted_config


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
        # compare model attributes except the schemapacks
        if (
            new_model.name != old_model.name
            or new_model.description != old_model.description
            or new_model.publish != old_model.publish
            or new_model.version != old_model.version
            or new_model.is_ingress != old_model.is_ingress
        ):
            raise ComparisonMismatchError(
                f"Mismatching fields on model {new_model.name}."
            )
        # Validation after loading ensures that only two invariants exist here:
        # 1) is_ingress == True and new_schema
        # 2) is_ingress == False and new_schema is None
        # Only the first case needs comparison
        new_schema = new_model.schema_
        old_schema = old_model.schema_

        if new_schema and not is_equal_schemapack(old_schema, new_schema):
            raise ComparisonMismatchError(
                f"Mismatching schema on EMIM model {new_model.name}."
            )
