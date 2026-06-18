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

"""Outbound port for persisting the resolved transformation configuration."""

from abc import ABC, abstractmethod

from emts.core.models import PersistedConfig


class ConfigWriterPort(ABC):
    """Persists a fully resolved transformation configuration to the database."""

    @abstractmethod
    async def write_config(self, config: PersistedConfig) -> None:
        """Upsert all models, routes, and workflows from the given config.

        Args:
            config: The resolved configuration containing derived models, routes,
                and workflows to persist.
        """
