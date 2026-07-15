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


def _unprocessed_version_filter(aem_pack_id: UUID4, version: int) -> dict:
    """Select a not yet processed doc at the given version."""
    return {
        "_id": aem_pack_id,
        VERSION_FIELD: version,
        PROCESSED_AT_FIELD: None,
    }


class IncomingAEMPackQueue(IncomingAEMPackQueuePort):
    """MongoDB adapter for the incoming AEMPack processing queue.

    Claims are tracked with a ``claimed_at`` timestamp written by the MongoDB server
    clock. An AEMPack is normally processed by a single instance, but a claim older than
    ``claim_ttl_seconds`` is treated as stale and may be reclaimed by another instance.
    Transient concurrent processing is possible, but the first result is written
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

    async def create_claim_indexes(self) -> None:
        """Create the secondary indexes backing claim queries."""
        # Index for the claim query (fresh and stale claims alike)
        await self._collection.create_index(
            [
                (PROCESSED_AT_FIELD, 1),
                (CLAIMED_AT_FIELD, 1),
                (TOMBSTONE_FIELD, 1),
            ]
        )
        # Index for reprocessing
        await self._collection.create_index(
            [
                (NEEDS_REPROCESSING_FIELD, 1),
                (PROCESSED_AT_FIELD, 1),
                (TOMBSTONE_FIELD, 1),
                (CLAIMED_AT_FIELD, 1),
            ]
        )

    async def get(self, aem_pack_id: UUID4) -> IncomingAEMPack | None:
        """Return the queued AEMPack with the given id, or None if it is not present."""
        doc = await self._collection.find_one({"_id": aem_pack_id})
        if doc is None:
            return None
        doc["id"] = doc.pop("_id")
        return IncomingAEMPack(**doc)

    async def queue(self, aem_pack: VersionedAEMPack) -> bool:
        """Upsert an AEMPack into the queue if its version is newer than the stored one.

        The incoming version is compared against any document already stored with the
        same id. The document is only (over)written when the incoming version is
        strictly higher. Equal or lower versions are rejected and logged.
        Accepting a newer version also resets ``failed_at``, so a previously failed pack
        is reprocessed under the new version.

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
                            # Preserve in-flight claim. The old worker will call free()
                            # when it detects the supersession, releasing the claim.
                            # mark_processed's version guard prevents it from committing.
                            CLAIMED_AT_FIELD: {
                                "$cond": {
                                    "if": f"${CLAIMED_AT_FIELD}",
                                    "then": f"${CLAIMED_AT_FIELD}",
                                    "else": None,
                                }
                            },
                            # Missing and null values should evaluate to false when
                            # comparing using $gt null
                            NEEDS_REPROCESSING_FIELD: {
                                "$or": [
                                    {"$gt": [f"${CLAIMED_AT_FIELD}", None]},
                                    {"$gt": [f"${PROCESSED_AT_FIELD}", None]},
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

        Claims are (re)stamped with the server clock (``$$NOW``) to avoid clock skew.
        Unclaimed packs and stale claims are matched by a single query: ascending
        order on ``claimed_at`` sorts null (never claimed) before any timestamp, so
        fresh packs are claimed first, then stale claims are reclaimed oldest-first.
        """
        ttl_ms = self._claim_ttl_seconds * 1000
        doc = await self._collection.find_one_and_update(
            filter={
                PROCESSED_AT_FIELD: None,
                TOMBSTONE_FIELD: {"$ne": True},
                "$or": [
                    {CLAIMED_AT_FIELD: None},
                    {
                        "$expr": {
                            "$lt": [
                                f"${CLAIMED_AT_FIELD}",
                                {"$subtract": ["$$NOW", ttl_ms]},
                            ]
                        }
                    },
                ],
            },
            update=[{"$set": {CLAIMED_AT_FIELD: "$$NOW"}}],
            sort=[(CLAIMED_AT_FIELD, 1)],
            return_document=ReturnDocument.AFTER,
        )

        # Already-processed pack that received a newer version while in flight.
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
                return_document=ReturnDocument.AFTER,
            )

        if doc is None:
            return None

        doc["id"] = doc.pop("_id")
        return IncomingAEMPack(**doc)

    async def mark_processed(self, aem_pack_id: UUID4, version: int) -> None:
        """Mark an AEMPack as successfully processed."""
        await self._collection.update_one(
            filter=_unprocessed_version_filter(aem_pack_id, version),
            # Version filter matched, so nothing newer is pending.
            update=[
                {
                    "$set": {
                        CLAIMED_AT_FIELD: None,
                        PROCESSED_AT_FIELD: "$$NOW",
                        NEEDS_REPROCESSING_FIELD: False,
                    }
                }
            ],
        )

    async def extend_all_claims(self, by_seconds: int) -> None:
        """Advance every in-flight ``claimed_at`` by ``by_seconds``.

        Compensates for idle time while the config lock was held, so those claims
        are not reclaimed for involuntary inactivity. Skips processed and unclaimed
        docs. No-op for non-positive duration.
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
        """Matches when the doc is already processed or superseded by a newer version."""
        return (
            await self._collection.count_documents(
                filter=_unprocessed_version_filter(aem_pack_id, version),
                limit=1,
            )
            == 0
        )

    async def mark_for_deletion(self, aem_pack_id: UUID4) -> None:
        """Mark an AEMPack for deletion."""
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
            filter={"_id": aem_pack_id, PROCESSED_AT_FIELD: None},
            update={"$set": {CLAIMED_AT_FIELD: None}},
        )

    async def mark_all_for_reprocessing(self) -> None:
        """Flag all processed AEMPacks for reprocessing.

        Failed AEMPacks are included and their ``failed_at`` is cleared: a config
        change may be exactly what fixes the transformation that previously failed.
        """
        await self._collection.update_many(
            filter={PROCESSED_AT_FIELD: {"$ne": None}, TOMBSTONE_FIELD: {"$ne": True}},
            update={
                "$set": {
                    NEEDS_REPROCESSING_FIELD: True,
                    FAILED_AT_FIELD: None,
                }
            },
        )

    async def mark_as_failed(self, aem_pack_id: UUID4, version: int) -> None:
        """Mark an AEMPack as failed, setting ``processed_at`` so it's not claimed again.

        Version-guarded, so it never marks an already processed or never version of the AEMPack.
        """
        await self._collection.update_one(
            filter=_unprocessed_version_filter(aem_pack_id, version),
            update=[
                {
                    "$set": {
                        FAILED_AT_FIELD: "$$NOW",
                        CLAIMED_AT_FIELD: None,
                        PROCESSED_AT_FIELD: "$$NOW",
                        NEEDS_REPROCESSING_FIELD: False,
                    }
                }
            ],
        )
