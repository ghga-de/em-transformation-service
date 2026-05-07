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

"""Manages transformation config related operations."""

import logging

from ets.core.config_pruning import prune_unproductive_subgraphs
from ets.core.models import PersistedConfig, RawConfig, ValidatedConfig
from ets.ports.inbound.config_comparator import ConfigComparatorPort
from ets.ports.inbound.config_manager import ConfigManagerError, ConfigManagerPort
from ets.ports.inbound.config_validator import (
    ConfigValidationError,
    ConfigValidatorPort,
)
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_version import ConfigVersionerPort

log = logging.getLogger(__name__)


class ConfigManager(ConfigManagerPort):
    """Manages loading, comparison, validation and selection of an active config."""

    def __init__(
        self,
        *,
        config_loader: ConfigLoaderPort,
        config_versioner: ConfigVersionerPort,
        validator: ConfigValidatorPort,
        comparator: ConfigComparatorPort,
    ):
        self._config_loader = config_loader
        self._config_versioner = config_versioner
        self.validator = validator
        self.comparator = comparator
        self._known_version: int = 0
        self._current_config: PersistedConfig | None = None

    @property
    def current_config(self) -> PersistedConfig:
        """Return the currently loaded active config."""
        if self._current_config is None:
            raise ConfigManagerError(
                "Current config has not been loaded yet. Call update_config() first."
            )
        return self._current_config

    @property
    def known_version(self) -> int:
        """Return the version of the currently loaded active config."""
        return self._known_version

    async def update_config(self):
        """Return the active config and its version, reloading from DB if the version changed."""
        current_version = await self._config_versioner.get_version()
        if self._current_config is None:
            log.info("Loading initial config (version %d).", current_version)
            self._current_config = await self._config_loader.load_config_from_db()
            self._known_version = current_version
        elif self._known_version < current_version:
            log.info(
                "Config version changed (%d -> %d), reloading.",
                self._known_version,
                current_version,
            )
            self._current_config = await self._config_loader.load_config_from_db()
            self._known_version = current_version
        elif self._known_version > current_version:
            inconsistent_version = ConfigManagerError(
                f"Encountered inconsistent current config version: {current_version}."
                f" Worker config version: {self._known_version}"
            )
            log.critical(inconsistent_version)
            raise inconsistent_version

    def resolve_transformation_config(self) -> PersistedConfig | ValidatedConfig:
        """Resolve the given transformation config.

        This includes:
        - Comparing raw config with the persisted config
        - If they are the same, return the persisted config
        - If they differ, validate the raw config, prune unproductive subgraphs and return it
        - If validation fails:
          - If a valid persisted config exists, log a warning and fall back to it
          - If no valid persisted config exists, raise ConfigManagerError and stop the service

        Raises:
            ConfigManagerError: If the new config fails validation and no previous
                valid config exists in the database.
        """
        match self.comparator.compare_configs():
            case RawConfig() as raw_config:
                # validate new config
                try:
                    config = self.validator.validate(raw_config)
                    return prune_unproductive_subgraphs(config)
                except ConfigValidationError as error:
                    persisted = self.comparator.persisted_config
                    if not (
                        persisted.models and persisted.routes and persisted.workflows
                    ):
                        msg = (
                            "New config failed to validate and no previous"
                            " valid config exists in the database."
                            " Stopping the service."
                        )
                        log.critical(msg, exc_info=error)
                        raise ConfigManagerError(msg) from error
                    log.warning(
                        "New config failed to validate, using existing, persisted config instead."
                    )
                    return persisted
            case PersistedConfig() as persisted_config:
                return persisted_config
