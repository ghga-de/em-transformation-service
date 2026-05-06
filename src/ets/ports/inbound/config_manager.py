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

"""Interface for managing transformation config related operations."""

from abc import ABC, abstractmethod

from ets.core.models import PersistedConfig, ValidatedConfig


class ConfigManagerError(RuntimeError):
    """Raised when an unexpected error happens while handling the transformation configurations."""


class ConfigManagerPort(ABC):
    """Port for managing transformation config related operations."""

    @abstractmethod
    async def get_current_config(self) -> tuple[PersistedConfig, int]:
        """Return the active config and its version, reloading from DB if the version changed."""

    @abstractmethod
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
