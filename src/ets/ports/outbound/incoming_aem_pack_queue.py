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

"""Port for the incoming AEMPack processing queue."""

from abc import ABC, abstractmethod

from pydantic import UUID4

from ets.core.models import AEMPack, IncomingAEMPack


class IncomingAEMPackQueuePort(ABC):
    """Port for the incoming AEMPack processing queue.

    Guarantees that each pack is claimed by exactly one processor at a time.
    """

    @abstractmethod
    async def queue(self, aem_pack: AEMPack) -> None:
        """Upsert an AEMPack into the queue."""

    @abstractmethod
    async def claim_next(self) -> IncomingAEMPack | None:
        """Claim the next available AEMPack for processing."""

    @abstractmethod
    async def mark_processed(self, aem_pack_id: UUID4) -> None:
        """Mark an AEMPack as successfully processed."""

    @abstractmethod
    async def mark_for_deletion(self, aem_pack_id: UUID4) -> None:
        """Mark an AEMPack for deletion."""

    @abstractmethod
    async def is_marked_or_deleted(self, aem_pack_id: UUID4) -> bool:
        """Check if an AEMPack is marked for deletion or already deleted."""

    @abstractmethod
    async def delete_marked(self, aem_pack_id: UUID4) -> None:
        """Delete an AEMPack from the queue."""

    @abstractmethod
    async def free(self, aem_pack_id: UUID4) -> None:
        """Release an AEMPack back to the queue without marking it processed."""

    @abstractmethod
    async def mark_all_for_reprocessing(self) -> None:
        """Flag all processed AEMPacks for reprocessing.

        Sets needs_reprocessing=True on every non-tombstoned doc that has already
        been processed, so claim_next will pick them up again.  Intended to be
        called once after a config change, while the config lock is still held.
        """
