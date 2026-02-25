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

"""KafkaEventSubscriber receiving events."""

from uuid import UUID

from hexkit.protocols.daosub import DaoSubscriberProtocol

from ets.adapters.inbound.event_schemas import AEMPack, AEMPackEventConfig
from ets.ports.inbound.aem_pack_registry import AEMPackRegistryPort


class EventSubTranslatorConfig(AEMPackEventConfig):
    """Config for the event subscriber."""


class EventSubTranslator(DaoSubscriberProtocol):
    """Outbox-style event subscriber that is used to receive an AEMPack."""

    event_topic: str

    dto_model = AEMPack

    def __init__(
        self,
        config: EventSubTranslatorConfig,
        aem_pack_registry: AEMPackRegistryPort,
    ):
        """Initialize with config parameters and core dependencies."""
        self.event_topic = config.aem_pack_upsert_topic

        self._aem_pack_registry = aem_pack_registry
        self._config = config

    async def changed(self, resource_id: str, update: AEMPack) -> None:
        """Consume a change event (created or updated) for the AEMPack"""
        aem_pack = AEMPack(
            id=update.id,
            model_name=update.model_name,
            original_id=update.original_id,
            data=update.data,
            annotation=update.annotation,
        )
        await self._aem_pack_registry.upsert_aem_pack(aem_pack)

    async def deleted(self, resource_id: str) -> None:
        """Consume an event indicating the deletion of an AEMPack"""
        await self._aem_pack_registry.delete_aem_pack(aem_pack_id=UUID(resource_id))
