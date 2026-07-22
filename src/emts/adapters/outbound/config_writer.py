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

from pydantic import BaseModel
from pymongo.asynchronous.collection import AsyncCollection

from emts.core.models import PersistedConfig
from emts.ports.outbound.config_version import ConfigVersionerPort
from emts.ports.outbound.config_writer import ConfigWriterPort

log = logging.getLogger(__name__)


def _to_document(entity: BaseModel) -> dict:
    """Convert a config entity into a MongoDB document.

    Mirrors the storage format of the hexkit DAOs (``id_field="name"``) so the
    config loader can keep reading these documents back through its DAOs: the
    ``name`` field becomes the document ``_id``.
    """
    document = entity.model_dump()
    document["_id"] = document.pop("name")
    return document


class ConfigWriterAdapter(ConfigWriterPort):
    """Adapter for replacing transformation config entities in the database."""

    def __init__(
        self,
        *,
        models: AsyncCollection,
        routes: AsyncCollection,
        workflows: AsyncCollection,
        config_versioner: ConfigVersionerPort,
    ):
        self._models = models
        self._routes = routes
        self._workflows = workflows
        self._config_versioner = config_versioner

    async def write_config(self, config: PersistedConfig) -> None:
        """Replace all models, routes, and workflows with the given config.

        Drops every existing entity across all three collections before inserting
        the new ones, so entities absent from the new config do not linger.

        Args:
            config: The resolved configuration containing derived models, routes,
                and workflows to persist.
        """
        log.info("Removing old config from DB ...")
        await self._models.delete_many({})
        await self._routes.delete_many({})
        await self._workflows.delete_many({})
        log.info("Persisting transformation configuration to the database.")
        await self._models.insert_many(_to_document(model) for model in config.models)
        await self._routes.insert_many(_to_document(route) for route in config.routes)
        await self._workflows.insert_many(
            _to_document(workflow) for workflow in config.workflows
        )
        await self._config_versioner.increment_version()
        log.info("Transformation configuration persisted successfully.")
