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

"""Models for KafkaEventSubscriber"""

from pydantic import UUID4, BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings
from schemapack.spec.datapack import DataPack


class AEMPackEventConfig(BaseSettings):
    """This will go to the event schemas.stateful?."""

    aem_pack_upsert_topic: str = Field(
        ...,
        description="Name of the topic used for events indicating that an original"
        " AEMPack is registered for transformation",
    )


class AEMPack(BaseModel):
    """This event is triggered when a new AEMPack is created or an existing one is
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
    original_id: UUID4 | None = Field(
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
    model_config = ConfigDict(title="aem_pack")
