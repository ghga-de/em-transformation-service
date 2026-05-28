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

from ets.core.config_comparison import compare_configs
from ets.core.config_pruning import prune_unproductive_subgraphs
from ets.core.config_validation import ConfigValidationError, validate
from ets.core.model_derivation import ModelDerivationError, ModelDeriver
from ets.core.models import PersistedConfig, RawConfig
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.config_writer import ConfigWriterPort

log = logging.getLogger(__name__)


class ConfigManagerError(RuntimeError):
    """Raised when an unexpected error happens while handling a transformation configuration."""


class ConfigManager:
    """Manages loading, comparison, validation and selection of an active config."""

    def __init__(
        self,
        *,
        loader: ConfigLoaderPort,
        versioner: ConfigVersionerPort,
        model_deriver: ModelDeriver,
        writer: ConfigWriterPort,
    ):
        self._versioner = versioner
        self._loader = loader
        self._model_deriver = model_deriver
        self._writer = writer
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
        """Check if the active config is stale and reload from DB if the version changed."""
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

    async def resolve_and_persist(self, input_config_path: Path) -> bool:
        """Load, resolve, and persist the transformation config.

        - Loads the raw config from disk and the persisted one from the database.
        - Compares them; if equal, leaves the DB untouched.
        - If they differ, validates the raw config, prunes unproductive subgraphs,
          derives output schemas, and persists the result (bumping the version).
        - If validation of a new config fails, falls back to the persisted config
          (without rewriting it). If no valid persisted config exists, raises
          ConfigManagerError and stops the service.

        Raises:
            ConfigManagerError: If the new config fails validation and no previous
                valid config exists in the database.
        Returns:
            True, if the persisted config is outdated and has been replaced with a new one
            False in all other cases
        """
        raw_config = self._loader.load_config_from_file(input_config_path)
        persisted_config = await self._loader.load_config_from_db()

        return await self._resolve_has_config_changed(
            raw_config=raw_config, persisted_config=persisted_config
        )

    async def _resolve_has_config_changed(
        self, *, raw_config: RawConfig, persisted_config: PersistedConfig
    ) -> bool:
        """Compare, validate/prune, derive schemas and persist the config as needed.
        Returns:
            True, if the persisted config is outdated and has been replaced with a new one
            False in all other cases
        """
        result = compare_configs(raw_config, persisted_config)
        if not isinstance(result, RawConfig):
            return False

        try:
            validated = validate(raw_config)
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
            return False

        try:
            derived_models = self._model_deriver.derive_models(pruned)
        except ModelDerivationError:
            log.warning(
                "Could not derive model schemas for new configuration."
                + "\nFalling back to existing, persisted config instead."
            )
            return False

        resolved = PersistedConfig(
            models=derived_models,
            routes=pruned.routes,
            workflows=pruned.workflows,
        )
        await self._writer.write_config(resolved)
        return True
