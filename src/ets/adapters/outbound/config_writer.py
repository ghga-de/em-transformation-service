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

"""Outbound adapter for persisting the transformation configuration to the database."""

import logging

from ets.core.models import PersistedConfig
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.config_writer import ConfigWriterPort
from ets.ports.outbound.dao import ModelDao, RouteDao, WorkflowDao

log = logging.getLogger(__name__)


class ConfigWriterAdapter(ConfigWriterPort):
    """Adapter for upserting transformation config entities to the database."""

    def __init__(
        self,
        *,
        model_dao: ModelDao,
        route_dao: RouteDao,
        workflow_dao: WorkflowDao,
        config_version: ConfigVersionerPort,
    ):
        self._model_dao = model_dao
        self._route_dao = route_dao
        self._workflow_dao = workflow_dao
        self._config_version = config_version

    async def write_config(self, config: PersistedConfig) -> None:
        """Upsert all models, routes, and workflows from the given config.

        Args:
            config: The resolved configuration containing derived models, routes,
                and workflows to persist.
        """
        log.info("Persisting transformation configuration to the database.")
        for model in config.models:
            await self._model_dao.upsert(model)
        for route in config.routes:
            await self._route_dao.upsert(route)
        for workflow in config.workflows:
            await self._workflow_dao.upsert(workflow)
        await self._config_version.increment_version()
        log.info("Transformation configuration persisted successfully.")
