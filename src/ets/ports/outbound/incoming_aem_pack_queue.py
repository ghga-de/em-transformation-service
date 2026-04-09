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

"""Port for the incoming AEM pack processing queue."""

from abc import ABC, abstractmethod

from pydantic import UUID4

from ets.core.models import AEMPack, IncomingAEMPack


class IncomingAEMPackQueuePort(ABC):
    """Port for the incoming AEM pack processing queue.

    Guarantees that each pack is claimed by exactly one processor at a time.
    """

    @abstractmethod
    async def queue(self, aem_pack: AEMPack, correlation_id: UUID4) -> None:
        """Upsert an AEMPack into the queue."""

    @abstractmethod
    async def claim_next(self) -> IncomingAEMPack | None:
        """Claim the next available AEMPack for processing."""

    @abstractmethod
    async def mark_processed(self, aem_pack_id: UUID4) -> None:
        """Mark an AEMPack as successfully processed."""
