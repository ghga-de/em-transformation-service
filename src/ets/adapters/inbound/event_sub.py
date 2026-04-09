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

import logging

from hexkit.protocols.daosub import DaoSubscriberProtocol

from ets.adapters.inbound.temporary_event_schemas import (
    AEMPackEventConfig,
    OriginalAEMPack,
)
from ets.ports.inbound.aem_pack_registry import AEMPackRegistryPort

log = logging.getLogger(__name__)


class AEMPackTranslatorConfig(AEMPackEventConfig):
    """Config for the AEMPack event subscriber."""


class EventSubTranslator(DaoSubscriberProtocol):
    """Outbox-style event subscriber that is used to receive an AEMPack."""

    event_topic: str

    dto_model = OriginalAEMPack

    def __init__(
        self,
        config: AEMPackTranslatorConfig,
        aem_pack_registry: AEMPackRegistryPort,
    ):
        """Initialize with config parameters and core dependencies."""
        self.event_topic = config.original_aem_pack_topic
        self._aem_pack_registry = aem_pack_registry
        self._config = config

    async def changed(self, resource_id: str, update: OriginalAEMPack) -> None:
        """Consume a change event (created or updated) for the AEMPack."""
        await self._aem_pack_registry.queue_unprocessed(update)

    async def deleted(self, resource_id: str) -> None:
        """Consume a deletion event for an AEMPack."""
        log.warning(
            "Received deletion event for resource '%s', but deletion is not yet implemented.",
            resource_id,
        )
