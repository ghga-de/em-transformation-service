# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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

"""This module contains functionalities for transforming models and data."""

import logging

from pydantic import UUID4

from ets.adapters.inbound.event_schemas import AEMPack
from ets.ports.inbound.aem_pack_registry import (
    AEMPackRegistryPort,
)
from ets.ports.outbound.dao import AEMPackDao, ResourceNotFoundError

log = logging.getLogger(__name__)


class AEMPackRegistry(AEMPackRegistryPort):
    """Core service for managing AEMPack transformations."""

    def __init__(
        self,
        *,
        aem_pack_dao: AEMPackDao,
    ):
        self._aem_pack_dao = aem_pack_dao

    async def upsert_aem_pack(self, aem_pack: AEMPack) -> None:
        """Upsert AEM Pack. Inserts a new AEMPack or updates an existing one.

        Args:
            aem_pack (AEMPack): The AEMPack to process.
        Raises:
            ModelNotFoundError: If the model referenced doesn't exist in configuration.
            DataPackValidationError: If the data doesn't conform to the model schema.
            UpsertionError: If the database operation fails.
        """
        try:
            await self._aem_pack_dao.get_by_id(aem_pack.id)
            log.debug(
                "Found an AEMPack with id '%s', updating entry.",
                aem_pack.id,
            )
        except ResourceNotFoundError:
            log.debug(
                "No existing AEMPack found with id '%s', creating new entry.",
                aem_pack.id,
            )

        await self._aem_pack_dao.upsert(aem_pack)

        log.debug("AEMPack upserted with id '%s'.", aem_pack.id)

    # Placeholder until the actual logic is implemented
    async def delete_aem_pack(self, aem_pack_id: UUID4) -> None:
        """Delete an existing AEMPack.

        Args:
            aem_pack_id (UUID4): The id of the AEMPack to delete.

        Raises:
            AEMPackNotFoundError: If the AEMPack does not exist.
        """
        ...
