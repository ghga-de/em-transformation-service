# Copyright 2021 - 2025 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
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
from pydantic import UUID4, BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings
from schemapack.spec.datapack import DataPack

from ets.core.models import AnnotatedEMPack
from ets.ports.inbound.annotated_em_pack_registry import AnnotatedEMPackRegistryPort


class AnnotatedEMPackEventConfig(BaseSettings):
    """This will go to the event schemas.stateful?."""

    annotated_em_pack_upsert_topic: str = Field(
        ...,
        description="Name of the topic used for events indicating that an original"
        " AnnotatedEMPack is registered for transformation",
    )


class AnnotatedEMPackPayload(BaseModel):
    """This event is triggered when a new annotated em pack is created or an existing one is
    updated.
    This will go to event_schemas.pydantic_
    """

    id: UUID4 = Field(
        ...,
        description="Unique identifier of the EMPack.",
    )
    model_name: str = Field(
        ...,
        description="Unique name of the model the EMPack conforms to.",
    )
    original_id: str | None = Field(
        None,
        description="ID of the original incoming EMPack it was derived from. None if it is an original EMPack",
    )
    data: DataPack = Field(
        ...,
        description="The data conforming to the model.",
    )
    annotation: dict = Field(
        ...,
        description="Additional information from other models held by the service",
    )
    model_config = ConfigDict(title="annotated_em_pack_received")


class EventSubTranslatorConfig(AnnotatedEMPackEventConfig):
    """This will go to the event schemas.
    Config for the event subscriber.
    """


class EventSubTranslator(DaoSubscriberProtocol):
    """Outbox-style event subscriber that is used to receive an AnnotatedEMPack."""

    event_topic: str

    dto_model = AnnotatedEMPackPayload

    def __init__(
        self,
        config: EventSubTranslatorConfig,
        annotated_em_pack_registry: AnnotatedEMPackRegistryPort,
    ):
        """Initialize with config parameters and core dependencies."""
        self.event_topic = config.annotated_em_pack_upsert_topic

        self._annotated_em_pack_registry = annotated_em_pack_registry
        self._config = config

    async def changed(self, resource_id: str, update: AnnotatedEMPackPayload) -> None:
        """Consume a change event (created or updated) for the AnnotatedEMPack"""
        annotated_em_pack = AnnotatedEMPack(
            id=update.id,
            model_name=update.model_name,
            original_id=update.original_id,
            data=update.data,
            annotation=update.annotation,
        )
        await self._annotated_em_pack_registry.upsert_annotated_em_pack(
            annotated_em_pack
        )

    async def deleted(self, resource_id: str) -> None:
        """Consume an event indicating the deletion of an AnnotatedEMPack"""
        await self._annotated_em_pack_registry.delete_annotated_em_pack(
            annotated_em_pack_id=UUID(resource_id)
        )
