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

"""Interface for managing annotated em pack operations."""

from abc import ABC, abstractmethod

from pydantic import UUID4

from ets.event_schemas import UnprocessedAEMPack


class AEMPackRegistryPort(ABC):
    """Port for managing AEMPack lifecycle and transformation operations.

    This port defines the interface for:
    - Upserting AEMPacks (insert or update)
    - Transforming an original AEMPack into all derived representations
    - Validating AEMPack data against schemas TODO
    """

    class ModelNotFoundError(RuntimeError):
        """Raised when a referenced model does not exist in the configuration."""

        def __init__(self, *, model_name: str):
            message = f"Model '{model_name}' not found in configuration."
            super().__init__(message)

    class DataPackValidationError(RuntimeError):
        """Raised when a DataPack of a AEMPack does not conform to its schema."""

        def __init__(self, *, aem_pack_id: UUID4, model_name: str):
            message = (
                f"DataPack validation failed for AEMPack '{aem_pack_id}' "
                f"against model '{model_name}'"
            )
            super().__init__(message)

    @abstractmethod
    async def queue_unprocessed(self, aem_pack: UnprocessedAEMPack):
        """TODO"""

    @abstractmethod
    async def process_aem_packs(self):
        """Transform an original AEMPack through the full user journey (steps 1-4).

        Steps 1-3 build the dirty map, initialize the transformed map, and traverse
        the DAG in topological order applying each route's workflow.
        Step 4 upserts all transformed AEMPacks whose model has ``publish=True``
        and deletes any stale entries remaining in the dirty map.

        Args:
            original: The incoming original AEMPack to transform.

        Returns:
            A tuple of (transformed_map, dirty_map):
            - transformed_map: mapping from model name to the newly produced AEMPack.
            - dirty_map: mapping from model name to IDs of stale AEMPacks that were
              deleted from the database in step 4.
        """
