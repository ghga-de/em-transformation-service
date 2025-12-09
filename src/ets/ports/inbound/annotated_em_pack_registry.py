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

from abc import ABC, abstractmethod

from pydantic import UUID4

from ets.core.models import AnnotatedEMPack


class ModelNotFoundError(RuntimeError):
    """Raised when a referenced model does not exist in the configuration."""

    def __init__(self, *, model_name: str):
        message = f"Model '{model_name}' not found in configuration."
        super().__init__(message)


class AnnotatedEMPackNotFoundError(RuntimeError):
    """Raised when an AnnotatedEMPack does not exist in the data storage.
    Triggered if deletion is attempted on a non-existing AnnotatedEMPack.
    """

    def __init__(self, *, annotated_em_pack_id: UUID4):
        message = (
            f"AnnotatedEMPack with ID '{annotated_em_pack_id}' not found in storage."
        )
        super().__init__(message)


class DataPackValidationError(RuntimeError):
    """Raised when a DataPack of a AnnotatedEMPack does not conform to its schema."""

    def __init__(self, *, annotated_em_pack_id: UUID4, model_name: str):
        message = (
            f"DataPack validation failed for AnnotatedEMPack '{annotated_em_pack_id}' "
            f"against model '{model_name}'"
        )
        super().__init__(message)


class UpsertionError(RuntimeError):
    """Raised when database upsert operation fails for an AnnotatedEMPack."""

    def __init__(self, *, annotated_em_pack_id: UUID4, details: str | None = None):
        message = f"Failed to upsert AnnotatedEMPack '{annotated_em_pack_id}'"
        if details:
            message += f": {details}"
        super().__init__(message)


class AnnotatedEMPackRegistryPort(ABC):
    """Port for managing AnnotatedEMPack lifecycle and transformation operations.

    This port defines the interface for:
    - Upserting AnnotatedEMPacks (insert or update)
    - Deleting AnnotatedEMPacks
    - Validating AnnotatedEMPack data against schemas TODO
    - Triggering data transformation TODO
    """

    @abstractmethod
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
        ...

    @abstractmethod
    async def delete_annotated_em_pack(self, annotated_em_pack_id: UUID4) -> None:
        """Delete Annotated EM Pack. Deletes an existing AnnotatedEMPack.

        Args:
            annotated_em_pack_id (UUID4): The id of the AnnotatedEMPack to delete.

        Raises:
            AnnotatedEMPackNotFoundError: If the AnnotatedEMPack does not exist.
        """
        ...
