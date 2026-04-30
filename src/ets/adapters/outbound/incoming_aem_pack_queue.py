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
from hexkit.utils import now_utc_ms_prec
from pydantic import UUID4
from pymongo import ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection

from ets.constants import (
    NEEDS_REPROCESSING_FIELD,
    PROCESSED_AT_FIELD,
    PROCESSOR_FIELD,
    TOMBSTONE_FIELD,
)
from ets.core.models import AEMPack, IncomingAEMPack
from ets.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort

log = logging.getLogger(__name__)


class IncomingAEMPackQueue(IncomingAEMPackQueuePort):
    """MongoDB adapter for the incoming AEMPack processing queue.

    Uses atomic find-and-update operations to guarantee that each pack is
    claimed by exactly one processor at a time.
    """

    def __init__(
        self,
        *,
        collection: AsyncCollection,
        worker_id: str,
    ):
        self._collection = collection
        self._worker_id = worker_id

    async def queue(self, aem_pack: AEMPack) -> None:
        """Upsert an AEMPack into the queue."""
        doc = aem_pack.model_dump(mode="json")
        doc.pop("id")
        doc["correlation_id"] = str(get_correlation_id())

        await self._collection.find_one_and_update(
            filter={"_id": aem_pack.id},
            update=[
                {
                    "$set": {
                        **doc,
                        # Preserve the current processor so the in-flight instance can still
                        # complete and mark the doc as done; it will be requeued via needs_reprocessing.
                        PROCESSOR_FIELD: {
                            "$cond": {
                                "if": f"${PROCESSOR_FIELD}",
                                "then": f"${PROCESSOR_FIELD}",
                                "else": None,
                            }
                        },
                        NEEDS_REPROCESSING_FIELD: {
                            "$or": [
                                {"$ne": [f"${PROCESSOR_FIELD}", None]},
                                {"$ne": [f"${PROCESSED_AT_FIELD}", None]},
                            ]
                        },
                        PROCESSED_AT_FIELD: {
                            "$cond": {
                                "if": f"${PROCESSED_AT_FIELD}",
                                "then": f"${PROCESSED_AT_FIELD}",
                                "else": None,
                            }
                        },
                    }
                }
            ],
            upsert=True,
        )

    async def claim_next(self) -> IncomingAEMPack | None:
        """Claim the next available AEMPack for processing."""
        # Check for packs abandoned by a previous crash of this instance
        doc = await self._collection.find_one(
            {
                PROCESSOR_FIELD: self._worker_id,
                PROCESSED_AT_FIELD: None,
                TOMBSTONE_FIELD: {"$ne": True},
            }
        )
        if not doc:
            # No abandoned packs; try to claim a fresh one
            doc = await self._collection.find_one_and_update(
                filter={
                    PROCESSOR_FIELD: None,
                    PROCESSED_AT_FIELD: None,
                    TOMBSTONE_FIELD: {"$ne": True},
                },
                update={"$set": {PROCESSOR_FIELD: self._worker_id}},
                return_document=ReturnDocument.AFTER,
            )
        if not doc:
            # Check for already-processed packs that received a new version while in flight
            doc = await self._collection.find_one_and_update(
                filter={
                    PROCESSED_AT_FIELD: {"$ne": None},
                    NEEDS_REPROCESSING_FIELD: True,
                    TOMBSTONE_FIELD: {"$ne": True},
                },
                update={
                    "$set": {
                        PROCESSOR_FIELD: self._worker_id,
                        PROCESSED_AT_FIELD: None,
                        NEEDS_REPROCESSING_FIELD: False,
                    }
                },
                return_document=ReturnDocument.AFTER,
            )

        if doc is None:
            return None

        doc["id"] = doc.pop("_id")
        return IncomingAEMPack(**doc)

    async def mark_processed(self, aem_pack_id: UUID4) -> None:
        """Mark an AEMPack as successfully processed."""
        await self._collection.update_one(
            {"_id": aem_pack_id},
            {
                "$set": {
                    PROCESSOR_FIELD: None,
                    PROCESSED_AT_FIELD: now_utc_ms_prec(),
                }
            },
        )

    async def mark_for_deletion(self, aem_pack_id: UUID4) -> None:
        """Mark the AEMPack for deletion."""
        await self._collection.update_one(
            {"_id": aem_pack_id}, {"$set": {TOMBSTONE_FIELD: True}}
        )

    async def is_marked_or_deleted(self, aem_pack_id: UUID4) -> bool:
        """Check if an AEMPack is marked for deletion or already deleted."""
        doc = await self._collection.find_one({"_id": aem_pack_id})
        return doc is None or bool(doc.get(TOMBSTONE_FIELD))

    async def delete_marked(self, aem_pack_id: UUID4) -> None:
        """Delete an AEMPack marked for deletion from the queue."""
        result = await self._collection.delete_one(
            {"_id": aem_pack_id, TOMBSTONE_FIELD: True}
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
            {"_id": aem_pack_id},
            {
                "$set": {
                    PROCESSOR_FIELD: None,
                    NEEDS_REPROCESSING_FIELD: False,
                }
            },
        )
