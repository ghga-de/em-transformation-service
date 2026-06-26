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

    A pack is normally processed by a single instance at a time. Claims carry a
    timestamp and a claim older than the configured TTL may be reclaimed, so
    transient concurrent processing is possible. ``processed_at`` is the
    single-writer terminal state that resolves it.
    """

    @abstractmethod
    async def queue(self, aem_pack: VersionedAEMPack) -> bool:
        """Upsert an AEMPack into the queue.

        Returns True if the pack was stored (a strictly newer version), False in all other cases.
        """

    @abstractmethod
    async def claim_next(self) -> IncomingAEMPack | None:
        """Claim the next available AEMPack for processing."""

    @abstractmethod
    async def mark_processed(self, aem_pack_id: UUID4, version: int) -> None:
        """Mark an AEMPack as successfully processed.

        Conditional on ``processed_at`` being unset *and* the stored version still
        equalling ``version``, so only the first worker to finish wins and a worker
        whose version was superseded mid-flight is discarded rather than committing
        stale results.
        """

    @abstractmethod
    async def extend_all_claims(self, by_seconds: int) -> None:
        """Push every in-flight claim's ``claimed_at`` forward by ``by_seconds``.

        Called once by the instance holding the config lock: while the lock is held,
        every other instance is blocked in ``wait_for_lock_release`` and its claimed
        packs age without making progress. Pushing every claim's ``claimed_at`` forward
        by the hold duration rewinds the time already counted against the reclaim
        timeout, so that involuntary idle time does not push live packs over the TTL.
        Already-processed (terminal) and unclaimed packs are left untouched. No-op for
        a non-positive duration.
        """

    @abstractmethod
    async def is_superseded_or_processed(
        self, aem_pack_id: UUID4, version: int
    ) -> bool:
        """Whether a derivation for ``version`` of this pack must be discarded.

        True when the stored pack is no longer unprocessed at exactly ``version`` —
        already in a terminal state, or superseded by a newer version queued while
        ``version`` was being derived. Either way the derived results are stale. A
        non-atomic early-out mirroring the condition ``mark_processed`` enforces.
        """

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
    async def free(self, aem_pack_id: UUID4, version: int) -> None:
        """Release this worker's in-flight claim on ``version`` without marking it processed.

        Version-guarded and conditional on the pack still being unprocessed, so a pack
        superseded by a newer version or already driven to a terminal state mid-flight
        is left untouched rather than having an unrelated claim yanked.
        """

    @abstractmethod
    async def mark_all_for_reprocessing(self) -> None:
        """Flag all processed AEMPacks for reprocessing.

        Sets needs_reprocessing=True on every non-tombstoned doc that has already
        been processed, so claim_next will pick them up again. Failed packs are
        included and their failed_at is cleared, since a config change may fix the
        transformation that failed. Intended to be called once after a config
        change, while the config lock is still held.
        """

    @abstractmethod
    async def mark_as_failed(self, aem_pack_id: UUID4, version: int) -> None:
        """Mark an AEMPack as failed when data derivation raises an exception.
        It is marked as processed for the sake of state management to ensure
        that it is not picked up again for processing. Conditional on ``processed_at``
        being unset *and* the stored version still equalling ``version``, mirroring
        ``mark_processed`` so a stale failure never overwrites a newer version.
        """
