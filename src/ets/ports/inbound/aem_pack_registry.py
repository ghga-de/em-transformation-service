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

"""Interface for managing Annotated EM Pack operations."""

from abc import ABC, abstractmethod

from pydantic import UUID4

from ets.core.models import AEMPack


class AEMPackRegistryPort(ABC):
    """Port for managing AEMPack lifecycle and transformation operations.

    This port defines the interface for:
    - Upserting AEMPacks (insert or update)
    - Transforming an original AEMPack into all derived representations
    """

    @abstractmethod
    async def queue_unprocessed(self, aem_pack: AEMPack):
        """Put new AEMPacks from event subscriber into the processing queue."""

    @abstractmethod
    async def process_aem_packs(self):
        """Derives AEMPacks from incoming AEMPacks."""

    @abstractmethod
    async def delete_aem_pack_and_descendants(self, incoming_aem_id: UUID4):
        """Delete an AEMPack and all derived AEMPacks."""
