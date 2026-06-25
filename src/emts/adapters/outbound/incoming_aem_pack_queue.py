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

"""MongoDB adapter for the incoming AEMPack processing queue."""

import logging

from hexkit.correlation import get_correlation_id
from pydantic import UUID4
from pymongo import ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError

from emts.constants import (
    CLAIMED_AT_FIELD,
    FAILED_AT_FIELD,
    NEEDS_REPROCESSING_FIELD,
    PROCESSED_AT_FIELD,
    TOMBSTONE_FIELD,
    VERSION_FIELD,
)
from emts.core.models import IncomingAEMPack, VersionedAEMPack
from emts.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort

log = logging.getLogger(__name__)


class IncomingAEMPackQueue(IncomingAEMPackQueuePort):
    """MongoDB adapter for the incoming AEMPack processing queue.

    Claims are tracked with a ``claimed_at`` timestamp written by the MongoDB server
    clock. A pack is normally processed by a single instance, but a claim older than
    ``claim_ttl_seconds`` is treated as stale and may be reclaimed by another instance.
    Transient concurrent processing is therefore possible, but the first result is written
    and all other discarded.
    """

    def __init__(
        self,
        *,
        collection: AsyncCollection,
        claim_ttl_seconds: int,
    ):
        self._collection = collection
        self._claim_ttl_seconds = claim_ttl_seconds

    async def queue(self, aem_pack: VersionedAEMPack) -> bool:
        """Upsert an AEMPack into the queue if its version is newer than the stored one.

        The incoming version is compared against any document already stored with the
        same id. The document is only (over)written when the incoming version is
        strictly higher. Equal or lower versions are rejected and logged. Accepting a newer version also resets
        ``failed_at``, so a previously failed pack is reprocessed under the new version.

        Returns True if the pack was stored, False if it was rejected.
        """
        doc = aem_pack.model_dump(mode="json")
        doc.pop("id")
        doc["correlation_id"] = str(get_correlation_id())
        incoming_version = doc[VERSION_FIELD]

        try:
            await self._collection.find_one_and_update(
                # No match when the stored version is >= incoming: upsert then tries to
                # insert a duplicate _id, which surfaces as DuplicateKeyError (rejection).
                filter={"_id": aem_pack.id, VERSION_FIELD: {"$lt": incoming_version}},
                update=[
                    {
                        "$set": {
                            **doc,
                            # Preserve an in-flight claim so the processing instance can
                            # still complete and mark the doc as done; it is requeued via
                            # needs_reprocessing.
                            CLAIMED_AT_FIELD: {
                                "$cond": {
                                    "if": f"${CLAIMED_AT_FIELD}",
                                    "then": f"${CLAIMED_AT_FIELD}",
                                    "else": None,
                                }
                            },
                            # Reprocessing is only needed when a newer version
                            # overwrites a doc that is already claimed or processed.
                            # ``$ifNull`` coerces a *missing* field to null so a fresh
                            # insert (where these fields do not yet exist) is not
                            # mistaken for an in-flight claim: ``$ne`` treats a missing
                            # field as distinct from null and would otherwise be true.
                            NEEDS_REPROCESSING_FIELD: {
                                "$or": [
                                    {
                                        "$ne": [
                                            {"$ifNull": [f"${CLAIMED_AT_FIELD}", None]},
                                            None,
                                        ]
                                    },
                                    {
                                        "$ne": [
                                            {
                                                "$ifNull": [
                                                    f"${PROCESSED_AT_FIELD}",
                                                    None,
                                                ]
                                            },
                                            None,
                                        ]
                                    },
                                ]
                            },
                            PROCESSED_AT_FIELD: {
                                "$cond": {
                                    "if": f"${PROCESSED_AT_FIELD}",
                                    "then": f"${PROCESSED_AT_FIELD}",
                                    "else": None,
                                }
                            },
                            # A newer version clears any prior failure so the pack is
                            # reprocessed instead of staying parked as failed.
                            FAILED_AT_FIELD: None,
                        }
                    }
                ],
                upsert=True,
            )
        except DuplicateKeyError:
            log.info(
                "Skip queuing AEMPack %s, as a newer version (%s) is already stored.",
                aem_pack.id,
                incoming_version,
            )
            return False
        return True

    async def claim_next(self) -> IncomingAEMPack | None:
        """Claim the next available AEMPack for processing.

        Priority order:
          1. a fresh, never-claimed pack;
          2. a stale pack whose claim has exceeded the TTL (crashed/stalled instance);
          3. an already-processed pack flagged for reprocessing.

        ``claimed_at`` is always (re)stamped with the MongoDB server clock (``$$NOW``)
        so reclaim decisions never depend on individual instance clocks.
        """
        # 1. Fresh, never-claimed pack.
        doc = await self._collection.find_one_and_update(
            filter={
                CLAIMED_AT_FIELD: None,
                PROCESSED_AT_FIELD: None,
                TOMBSTONE_FIELD: {"$ne": True},
            },
            update=[{"$set": {CLAIMED_AT_FIELD: "$$NOW"}}],
            return_document=ReturnDocument.AFTER,
        )

        # 2. Stale pack: claimed but never processed, with a claim older than the TTL.
        #    Both the comparison baseline and the new timestamp come from the server
        #    clock. NOTE: this is an application-level timeout, deliberately NOT a
        #    MongoDB TTL index, which would *delete* the in-flight document.
        if not doc:
            ttl_ms = self._claim_ttl_seconds * 1000
            doc = await self._collection.find_one_and_update(
                filter={
                    PROCESSED_AT_FIELD: None,
                    TOMBSTONE_FIELD: {"$ne": True},
                    CLAIMED_AT_FIELD: {"$ne": None},
                    "$expr": {
                        "$lt": [
                            f"${CLAIMED_AT_FIELD}",
                            {"$subtract": ["$$NOW", ttl_ms]},
                        ]
                    },
                },
                update=[{"$set": {CLAIMED_AT_FIELD: "$$NOW"}}],
                sort=[(CLAIMED_AT_FIELD, 1)],
                return_document=ReturnDocument.AFTER,
            )
            if doc is not None:
                log.warning(
                    "Reclaimed stale AEMPack %s (pid %s); its claim exceeded the %ds "
                    "TTL, so the previous processor likely crashed or stalled.",
                    doc["_id"],
                    doc.get("pid"),
                    self._claim_ttl_seconds,
                )

        # 3. Already-processed pack that received a newer version while in flight.
        if not doc:
            doc = await self._collection.find_one_and_update(
                filter={
                    PROCESSED_AT_FIELD: {"$ne": None},
                    NEEDS_REPROCESSING_FIELD: True,
                    TOMBSTONE_FIELD: {"$ne": True},
                },
                update=[
                    {
                        "$set": {
                            CLAIMED_AT_FIELD: "$$NOW",
                            PROCESSED_AT_FIELD: None,
                            NEEDS_REPROCESSING_FIELD: False,
                        }
                    }
                ],
                sort=[(CLAIMED_AT_FIELD, 1)],
                return_document=ReturnDocument.AFTER,
            )

        if doc is None:
            return None

        doc["id"] = doc.pop("_id")
        return IncomingAEMPack(**doc)

    async def mark_processed(self, aem_pack_id: UUID4, version: int) -> None:
        """Mark an AEMPack as successfully processed.

        Conditional on ``processed_at`` still being unset *and* the stored ``version``
        still matching the one that was processed. This keeps the terminal state both
        single-valued and current: when the same pack is processed concurrently (a
        long-running pack reclaimed as stale while still alive) only the first worker to
        reach this point wins, and a worker whose ``version`` was superseded by a newer
        one queued mid-flight matches nothing and is silently discarded instead of
        committing stale results. The discarded work is recovered via the requeued
        ``needs_reprocessing`` flag, so the workers never livelock.
        """
        await self._collection.update_one(
            filter={
                "_id": aem_pack_id,
                PROCESSED_AT_FIELD: None,
                VERSION_FIELD: version,
            },
            update=[{"$set": {CLAIMED_AT_FIELD: None, PROCESSED_AT_FIELD: "$$NOW"}}],
        )

    async def extend_all_claims(self, by_seconds: int) -> None:
        """Push every in-flight claim's ``claimed_at`` forward by ``by_seconds``.

        Issued once by the config-lock holder: while it held the lock, every other
        instance was blocked in ``wait_for_lock_release``, so each in-flight claim aged
        without making progress. Rewinding all claims by the hold duration keeps those
        still-live packs from being reclaimed for involuntary idle time. A duration is
        added to each server-written timestamp, which is immune to clock skew.
        Already-processed (terminal) and unclaimed packs are left untouched. No-op for
        a non-positive duration.
        """
        if by_seconds <= 0:
            return
        await self._collection.update_many(
            filter={PROCESSED_AT_FIELD: None, CLAIMED_AT_FIELD: {"$ne": None}},
            update=[
                {
                    "$set": {
                        CLAIMED_AT_FIELD: {
                            "$add": [f"${CLAIMED_AT_FIELD}", by_seconds * 1000]
                        }
                    }
                }
            ],
        )

    async def is_superseded_or_processed(
        self, aem_pack_id: UUID4, version: int
    ) -> bool:
        """Whether a derivation for ``version`` of this pack must be discarded.

        True when the stored document is no longer an unprocessed pack at exactly
        ``version``: it has either already reached a terminal state, or a newer version
        arrived while ``version`` was being derived (so the stored content now differs
        from what was processed). In both cases the derived results are stale and must
        not be published. This mirrors, as a non-atomic early-out, the condition that
        ``mark_processed`` enforces atomically.
        """
        return (
            await self._collection.count_documents(
                filter={
                    "_id": aem_pack_id,
                    VERSION_FIELD: version,
                    PROCESSED_AT_FIELD: None,
                },
                limit=1,
            )
            == 0
        )

    async def mark_for_deletion(self, aem_pack_id: UUID4) -> None:
        """Mark the AEMPack for deletion."""
        await self._collection.update_one(
            filter={"_id": aem_pack_id}, update={"$set": {TOMBSTONE_FIELD: True}}
        )

    async def is_marked_or_deleted(self, aem_pack_id: UUID4) -> bool:
        """Check if an AEMPack is marked for deletion or already deleted."""
        doc = await self._collection.find_one(filter={"_id": aem_pack_id})
        return doc is None or bool(doc.get(TOMBSTONE_FIELD))

    async def delete_marked(self, aem_pack_id: UUID4) -> None:
        """Delete an AEMPack marked for deletion from the queue."""
        result = await self._collection.delete_one(
            filter={"_id": aem_pack_id, TOMBSTONE_FIELD: True}
        )
        if result.deleted_count:
            log.info("AEMPack %s successfully deleted from the queue.", aem_pack_id)
        else:
            log.warning(
                "AEMPack %s not found in the queue for deletion, presumed already deleted.",
                aem_pack_id,
            )

    async def free(self, aem_pack_id: UUID4) -> None:
        """Release an AEMPack back to the queue without marking it processed."""
        await self._collection.update_one(
            filter={"_id": aem_pack_id},
            update={
                "$set": {
                    CLAIMED_AT_FIELD: None,
                    NEEDS_REPROCESSING_FIELD: False,
                }
            },
        )

    async def mark_all_for_reprocessing(self) -> None:
        """Flag all processed AEMPacks for reprocessing.

        Failed AEMPacks are included and their ``failed_at`` is cleared: a config
        change may be exactly what fixes the transformation that previously failed,
        so they are retried fresh under the new config.
        """
        await self._collection.update_many(
            filter={PROCESSED_AT_FIELD: {"$ne": None}, TOMBSTONE_FIELD: {"$ne": True}},
            update={"$set": {NEEDS_REPROCESSING_FIELD: True, FAILED_AT_FIELD: None}},
        )

    async def mark_as_failed(self, aem_pack_id: UUID4, version: int) -> None:
        """Mark an AEMPack as failed when data derivation raises an exception.

        It is marked as processed (``processed_at`` set) for the sake of state
        management to ensure that it is not picked up again for processing. Conditional
        on ``processed_at`` being unset *and* the stored ``version`` still matching,
        mirroring ``mark_processed``: a concurrent worker that already reached a terminal
        state, or a newer version queued mid-flight, makes this a no-op so a stale
        failure never overwrites a current version. Derivation is deterministic, so
        concurrent runs of the same version agree on the failure.
        """
        await self._collection.update_one(
            filter={
                "_id": aem_pack_id,
                PROCESSED_AT_FIELD: None,
                VERSION_FIELD: version,
            },
            update={
                "$set": {
                    FAILED_AT_FIELD: "$$NOW",
                    CLAIMED_AT_FIELD: None,
                    PROCESSED_AT_FIELD: "$$NOW",
                }
            },
        )
