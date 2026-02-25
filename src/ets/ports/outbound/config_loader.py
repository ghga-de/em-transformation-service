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

"""Module for loading and parsing the transformation config file."""

from abc import ABC, abstractmethod
from pathlib import Path

from ets.core.models import PersistedConfig, RawConfig


class ConfigurationLoaderError(Exception):
    """Raised when loading the configuration fails."""


class ConfigFileLoaderPort(ABC):
    """Loads the transformation config file and parses it into a RawConfig."""

    @abstractmethod
    def load_config_from_file(self, config_path: Path) -> RawConfig:
        """Load a transformation config from a yaml file."""

    @abstractmethod
    async def load_config_from_db(self) -> PersistedConfig:
        """Fetch transformation config fields from persistence layer and sort them by name."""
