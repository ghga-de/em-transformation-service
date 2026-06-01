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

"""Outbound adapter for loading and parsing the transformation config file."""

import logging
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from yaml import safe_load

from ets.core.models import PersistedConfig, RawConfig
from ets.ports.outbound.config_loader import (
    ConfigLoaderPort,
    ConfigurationLoaderError,
)
from ets.ports.outbound.dao import ModelDao, RouteDao, WorkflowDao

log = logging.getLogger(__name__)


class ConfigLoaderAdapter(ConfigLoaderPort):
    """Adapter for loading transformation config from YAML and DB."""

    def __init__(
        self, *, model_dao: ModelDao, route_dao: RouteDao, workflow_dao: WorkflowDao
    ):
        self._model_dao = model_dao
        self._route_dao = route_dao
        self._workflow_dao = workflow_dao

    def _read_yaml(self, config_path: Path) -> dict[str, Any]:
        """Read a new config from file and return it as a dict."""
        with config_path.open("r") as config_file:
            new_config = safe_load(config_file)
        return new_config

    def _load_config(self, config: dict[str, Any]) -> RawConfig:
        """Load and return a config dict. This step takes care of the SchemaPack spec validation."""
        try:
            return RawConfig.model_validate(config)
        except ValidationError as exc:
            schema_errors = [
                err for err in exc.errors() if "schema_" in err.get("loc", ())
            ]
            if len(schema_errors) != len(exc.errors()):
                # Structural config errors should propagate, not be silently swallowed
                raise ConfigurationLoaderError(
                    f"Config validation failed due to structural issues in the config: {exc}"
                ) from exc
            raise ConfigurationLoaderError(
                "Config validation failed due to schemas included in the config not being"
                f" compatible with SchemaPack spec: {exc}"
            ) from exc

    def load_config_from_file(self, config_path: Path) -> RawConfig:
        """Load a config from a yaml file."""
        log.info("Fetching new config from file.")
        config_dict = self._read_yaml(config_path)
        raw_config = self._load_config(config_dict)
        return raw_config

    async def load_config_from_db(self) -> PersistedConfig:
        """Fetch config fields from persistence layer and sort them by name."""
        log.info("Fetching config from persistence layer.")
        models = [model async for model in self._model_dao.find_all(mapping={})]
        routes = [route async for route in self._route_dao.find_all(mapping={})]
        workflows = [
            workflow async for workflow in self._workflow_dao.find_all(mapping={})
        ]

        return PersistedConfig(models=models, routes=routes, workflows=workflows)
