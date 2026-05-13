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
from pathlib import Path

from ets.core.config_pruning import prune_unproductive_subgraphs
from ets.core.models import PersistedConfig, RawConfig
from ets.ports.inbound.config_comparator import ConfigComparatorPort
from ets.ports.inbound.config_manager import ConfigManagerError, ConfigManagerPort
from ets.ports.inbound.config_validator import (
    ConfigValidationError,
    ConfigValidatorPort,
)
from ets.ports.inbound.model_derivation import ModelDeriverPort
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.config_writer import ConfigWriterPort

log = logging.getLogger(__name__)


class ConfigManager(ConfigManagerPort):
    """Manages loading, comparison, validation and selection of an active config."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        comparator: ConfigComparatorPort,
        loader: ConfigLoaderPort,
        versioner: ConfigVersionerPort,
        model_deriver: ModelDeriverPort,
        validator: ConfigValidatorPort,
        writer: ConfigWriterPort,
    ):
        self._comparator = comparator
        self._versioner = versioner
        self._loader = loader
        self._model_deriver = model_deriver
        self._writer = writer
        self._validator = validator
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
        current_version = await self._versioner.get_version()
        if self._current_config is None:
            log.info("Loading initial config (version %d).", current_version)
            self._current_config = await self._loader.load_config_from_db()
            self._known_version = current_version
        elif self._known_version < current_version:
            log.info(
                "Config version changed (%d -> %d), reloading.",
                self._known_version,
                current_version,
            )
            self._current_config = await self._loader.load_config_from_db()
            self._known_version = current_version
        elif self._known_version > current_version:
            inconsistent_version = ConfigManagerError(
                f"Encountered inconsistent current config version: {current_version}."
                f" Worker config version: {self._known_version}"
            )
            log.critical(inconsistent_version)
            raise inconsistent_version

    async def resolve_and_persist(self, input_config_path: Path) -> None:
        """Load, resolve, and persist the transformation config.

        - Loads the raw config from disk and the persisted one from the database.
        - Compares them; if equal, persists the persisted config back (no-op upsert).
        - If they differ, validates the raw config, prunes unproductive subgraphs,
          derives output schemas, and persists the result.
        - If validation of a new config fails, falls back to the persisted config
          (and persists it). If no valid persisted config exists, raises
          ConfigManagerError and stops the service.

        Raises:
            ConfigManagerError: If the new config fails validation and no previous
                valid config exists in the database.
        """
        raw_config = self._loader.load_config_from_file(input_config_path)
        persisted_config = await self._loader.load_config_from_db()
        
        await self._resolve(
            raw_config=raw_config, persisted_config=persisted_config
        )

    async def _resolve(self, *, raw_config: RawConfig, persisted_config: PersistedConfig):
        """Compare, validate/prune and derive schemas as needed."""
        match self._comparator.compare_configs(raw_config, persisted_config):
            case RawConfig():
                try:
                    validated = self._validator.validate(raw_config)
                    pruned = prune_unproductive_subgraphs(validated)
                except ConfigValidationError as error:
                    if not (
                        persisted_config.models
                        and persisted_config.routes
                        and persisted_config.workflows
                    ):
                        msg = (
                            "New config failed to validate and no previous"
                            " valid config exists in the database."
                            " Stopping the service."
                        )
                        log.critical(msg)
                        raise ConfigManagerError(msg) from error

                    log.warning(
                        "New config failed to validate, using existing, persisted config instead."
                    )
                    return persisted_config
                derived_models = self._model_deriver.derive_models(pruned)
                resolved = PersistedConfig(
                    models=derived_models,
                    routes=pruned.routes,
                    workflows=pruned.workflows,
                )
                await self._writer.write_config(resolved)
            case PersistedConfig(): # keep around, so the match is exhaustive
                return
