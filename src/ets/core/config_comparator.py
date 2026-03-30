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
from functools import cached_property

from schemapack import is_equal_schemapack

from ets.core.models import (
    PersistedConfig,
    RawConfig,
)
from ets.ports.inbound.config_comparator import (
    ComparisonMismatchError,
    ConfigComparatorPort,
)

log = logging.getLogger(__name__)


class ConfigComparator(ConfigComparatorPort):
    """Compares new config with the persisted one to detect changes."""

    def __init__(self, raw_config: RawConfig, persisted_config: PersistedConfig):
        self._raw_config = RawConfig(
            models=sorted(raw_config.models, key=lambda m: m.name),
            routes=sorted(raw_config.routes, key=lambda r: r.name),
            workflows=sorted(raw_config.workflows, key=lambda w: w.name),
        )
        self._persisted_config = persisted_config

    @cached_property
    def persisted_config(self) -> PersistedConfig:
        """Return the persisted config with sorted collections."""
        return PersistedConfig(
            models=sorted(self._persisted_config.models, key=lambda m: m.name),
            routes=sorted(self._persisted_config.routes, key=lambda r: r.name),
            workflows=sorted(self._persisted_config.workflows, key=lambda w: w.name),
        )

    def compare_configs(self) -> PersistedConfig | RawConfig:
        """Compare new config with the persisted one.

        Returns:
            RawConfig: when the configs differ, containing the new models, routes, and workflows.
            PersistedConfig: when the configs are equal, containing the persisted models, routes, and workflows.
        """
        try:
            log.info("Comparing models.")
            self._compare_models()
            log.info("Comparing routes.")
            self._compare_routes()
            log.info("Comparing workflows.")
            self._compare_workflows()
        except ComparisonMismatchError as error:
            log.info(
                f"Changes detected between configs, using new config.\nDetails:{error}"
            )
            return self._raw_config

        log.info("No changes detected between configs, continuing with old config.")
        return self.persisted_config

    def _compare_models(self):
        new = self._raw_config.models
        old = self.persisted_config.models
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

    def _compare_routes(self):
        new = self._raw_config.routes
        old = self.persisted_config.routes
        if len(new) != len(old):
            raise ComparisonMismatchError("Different amount of config entities.")
        for n, o in zip(new, old, strict=True):
            if n != o:
                raise ComparisonMismatchError(f"Mismatching config entity: {n.name}.")

    def _compare_workflows(self):
        new = self._raw_config.workflows
        old = self.persisted_config.workflows
        if len(new) != len(old):
            raise ComparisonMismatchError("Different amount of config entities.")
        for n, o in zip(new, old, strict=True):
            if n != o:
                raise ComparisonMismatchError(f"Mismatching config entity: {n.name}.")
