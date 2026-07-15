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

from emts.core.models import IncomingAEMPack, VersionedAEMPack


class IncomingAEMPackQueuePort(ABC):
    """Port for the incoming AEMPack processing queue.

    Claims are tracked with a ``claimed_at`` timestamp written by the MongoDB server
    clock. An AEMPack is normally processed by a single instance, but a claim older than
    ``claim_ttl_seconds`` is treated as stale and may be reclaimed by another instance.
    Transient concurrent processing is possible, but the first result is written
    and all other discarded.
    """

    @abstractmethod
    async def create_claim_indexes(self) -> None:
        """Create the secondary indexes backing claim queries."""

    @abstractmethod
    async def get(self, aem_pack_id: UUID4) -> IncomingAEMPack | None:
        """Return the queued AEMPack with the given id, or None if it is not present."""

    @abstractmethod
    async def queue(self, aem_pack: VersionedAEMPack) -> bool:
        """Upsert an AEMPack into the queue if its version is newer than the stored one.

        The incoming version is compared against any document already stored with the
        same id. The document is only (over)written when the incoming version is
        strictly higher. Equal or lower versions are rejected and logged.
        Accepting a newer version also resets ``failed_at``, so a previously failed pack
        is reprocessed under the new version.

        Returns True if the pack was stored, False if it was rejected.
        """

    @abstractmethod
    async def claim_next(self) -> IncomingAEMPack | None:
        """Claim the next available AEMPack for processing.

        Claims are (re)stamped with the server clock (``$$NOW``) to avoid clock skew.
        """

    @abstractmethod
    async def mark_processed(self, aem_pack_id: UUID4, version: int) -> None:
        """Mark an AEMPack as successfully processed."""

    @abstractmethod
    async def extend_all_claims(self, by_seconds: int) -> None:
        """Advance every in-flight ``claimed_at`` by ``by_seconds``.

        Compensates for idle time while the config lock was held, so those claims
        are not reclaimed for involuntary inactivity. Skips processed and unclaimed
        docs. No-op for non-positive duration.
        """

    @abstractmethod
    async def is_superseded_or_processed(
        self, aem_pack_id: UUID4, version: int
    ) -> bool:
        """Matches when the doc is already processed or superseded by a newer version."""

    @abstractmethod
    async def mark_for_deletion(self, aem_pack_id: UUID4) -> None:
        """Mark an AEMPack for deletion."""

    @abstractmethod
    async def is_marked_or_deleted(self, aem_pack_id: UUID4) -> bool:
        """Check if an AEMPack is marked for deletion or already deleted."""

    @abstractmethod
    async def delete_marked(self, aem_pack_id: UUID4) -> None:
        """Delete an AEMPack marked for deletion from the queue."""

    @abstractmethod
    async def free(self, aem_pack_id: UUID4) -> None:
        """Release an AEMPack back to the queue without marking it processed."""

    @abstractmethod
    async def mark_all_for_reprocessing(self) -> None:
        """Flag all processed AEMPacks for reprocessing.

        Failed AEMPacks are included and their ``failed_at`` is cleared: a config
        change may be exactly what fixes the transformation that previously failed.
        """

    @abstractmethod
    async def mark_as_failed(self, aem_pack_id: UUID4, version: int) -> None:
        """Mark an AEMPack as failed, setting ``processed_at`` so it's not claimed again.

        Version-guarded, so it never marks an already processed or newer version of the AEMPack.
        """
