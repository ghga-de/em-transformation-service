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

from ets.core.models import AnnotatedEMPack
from ets.ports.inbound.annotated_em_pack_registry import (
    AnnotatedEMPackRegistryPort,
)
from ets.ports.outbound.dao import AnnotatedEMPackDao, ResourceNotFoundError

log = logging.getLogger(__name__)


class AnnotatedEMPackTransformer(AnnotatedEMPackRegistryPort):
    """Core service for managing AnnotatedEMPack transformations."""

    def __init__(
        self,
        *,
        annotated_em_pack_dao: AnnotatedEMPackDao,
    ):
        self._annotated_em_pack_dao = annotated_em_pack_dao

    async def upsert_annotated_em_pack(
        self, annotated_em_pack: AnnotatedEMPack
    ) -> None:
        """Upsert Annotated EM Pack. Inserts a new AnnotatedEMPack or updates an existing one.

        Args:
            annotated_em_pack (AnnotatedEMPack): The AnnotatedEMPack to process.
        Raises:
            ModelNotFoundError: If the model referenced doesn't exist in configuration.
            DataPackValidationError: If the data doesn't conform to the model schema.
            UpsertionError: If the database operation fails.
        """
        try:
            await self._annotated_em_pack_dao.get_by_id(annotated_em_pack.id)
            log.debug(
                "Found an AnnotatedEMPack with id '%s', updating entry.",
                annotated_em_pack.id,
            )
        except ResourceNotFoundError:
            log.debug(
                "No existing AnnotatedEMPack found with id '%s', creating new entry.",
                annotated_em_pack.id,
            )

        await self._annotated_em_pack_dao.upsert(annotated_em_pack)

        log.debug("AnnotatedEMPack upserted with id '%s'.", annotated_em_pack.id)

    # Placeholder until the actual logic is implemented
    async def delete_annotated_em_pack(self, annotated_em_pack_id: UUID4) -> None:
        """Delete Annotated EM Pack. Deletes an existing AnnotatedEMPack.

        Args:
            annotated_em_pack_id (UUID4): The id of the AnnotatedEMPack to delete.

        Raises:
            AnnotatedEMPackNotFoundError: If the AnnotatedEMPack does not exist.
        """
        ...
