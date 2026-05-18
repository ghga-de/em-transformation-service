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
    PersistedConfig,
    RawConfig,
)

log = logging.getLogger(__name__)


class ComparisonMismatchError(RuntimeError):
    """Custom error type raised on any mismatch between the existing and new config."""


class ConfigComparator:
    """Compares new config with the persisted one to detect changes."""

    def compare_configs(
        self, raw_config: RawConfig, persisted_config: PersistedConfig
    ) -> PersistedConfig | RawConfig:
        """Compare new config with the persisted one.

        Returns:
            RawConfig: when the configs differ, containing the new models, routes, and workflows.
            PersistedConfig: when the configs are equal, containing the persisted models, routes, and workflows.
        """
        sorted_raw = raw_config.model_copy(
            update={
                "models": sorted(raw_config.models, key=lambda m: m.name),
                "routes": sorted(raw_config.routes, key=lambda r: r.name),
                "workflows": sorted(raw_config.workflows, key=lambda w: w.name),
            }
        )
        sorted_persisted = persisted_config.model_copy(
            update={
                "models": sorted(persisted_config.models, key=lambda m: m.name),
                "routes": sorted(persisted_config.routes, key=lambda r: r.name),
                "workflows": sorted(persisted_config.workflows, key=lambda w: w.name),
            }
        )
        try:
            log.info("Comparing models.")
            self._compare_models(raw=sorted_raw, persisted=sorted_persisted)
            log.info("Comparing routes.")
            self._compare_routes(raw=sorted_raw, persisted=sorted_persisted)
            log.info("Comparing workflows.")
            self._compare_workflows(raw=sorted_raw, persisted=sorted_persisted)
        except ComparisonMismatchError as error:
            log.info(
                f"Changes detected between configs, using new config.\nDetails:{error}"
            )
            return sorted_raw

        log.info("No changes detected between configs, continuing with old config.")
        return sorted_persisted

    def _compare_models(self, *, raw: RawConfig, persisted: PersistedConfig) -> None:
        new = raw.models
        old = persisted.models
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

    def _compare_routes(self, *, raw: RawConfig, persisted: PersistedConfig) -> None:
        new = raw.routes
        old = persisted.routes
        if len(new) != len(old):
            raise ComparisonMismatchError("Different amount of routes.")
        for n, o in zip(new, old, strict=True):
            if n != o:
                raise ComparisonMismatchError(f"Mismatching route: {n.name}.")

    def _compare_workflows(self, *, raw: RawConfig, persisted: PersistedConfig) -> None:
        new = raw.workflows
        old = persisted.workflows
        if len(new) != len(old):
            raise ComparisonMismatchError("Different amount of workflows.")
        for n, o in zip(new, old, strict=True):
            if n != o:
                raise ComparisonMismatchError(f"Mismatching workflow: {n.name}.")
