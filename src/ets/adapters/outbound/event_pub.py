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

"""Translation between core and EventPublisherProtocol"""

from hexkit.protocols.eventpub import EventPublisherProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from ets.core import models
from ets.ports.outbound.event_pub import EventPubTranslatorPort


class EventPubTranslatorConfig(
    BaseSettings
):  # will be removed after it is in event schemas
    """Configuration for publishing events"""

    my_topic: str = Field(
        default=...
    )  # consumers will filter based on filter and type, e.g. labelling
    my_type: str = Field(default=...)


class EventPubTranslator(EventPubTranslatorPort):
    """Translation between core and EventPublisherProtocol"""

    def __init__(
        self,
        *,
        config: EventPubTranslatorConfig,
        provider: EventPublisherProtocol,
    ) -> None:
        """Configure with provider for the DaoFactoryProtocol"""
        self._provider = provider
        self._config = config

    async def publish_derived_em(
        self,
        *,
        data: str,  # internal model
        _id: str,
    ):
        """Send FileUploadValidationSuccess event to downstream services"""
        payload = models.DerivedEM(data=data, my_id=_id, dummy_field="dummy")
        await self._provider.publish(
            payload=payload.model_dump(),
            type_=self._config.my_type,
            key=payload.my_id,
            topic=self._config.my_topic,
        )
