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

"""DAO translators for accessing the database."""

from hexkit.protocols.dao import DaoFactoryProtocol
from hexkit.protocols.daopub import DaoPublisher, DaoPublisherFactoryProtocol
from hexkit.providers.mongodb import MongoDbIndex
from pydantic import Field
from pydantic_settings import BaseSettings

from emts.core import models
from emts.core.models import AEMPack, AEMPackStatusEvent
from emts.ports.outbound.dao import (
    ModelDao,
    RouteDao,
    StatusEventDao,
    WorkflowDao,
)


async def get_persisted_model_dao(*, dao_factory: DaoFactoryProtocol) -> ModelDao:
    """Setup the Persisted Model DAO using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name="models", dto_model=models.Model, id_field="name"
    )


async def get_workflow_dao(*, dao_factory: DaoFactoryProtocol) -> WorkflowDao:
    """Setup the Workflow DAO using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name="workflows", dto_model=models.Workflow, id_field="name"
    )


async def get_route_dao(*, dao_factory: DaoFactoryProtocol) -> RouteDao:
    """Setup the Route DAO using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name="routes", dto_model=models.Route, id_field="name"
    )


class AEMPackDaoConfig(BaseSettings):
    """Config for the AEMPack event publisher adapter."""

    derived_aem_pack_topic: str = Field(
        default=...,
        description="Topic for events informing about derived AEMPacks.",
        examples=["derived-aempacks"],
    )
    aem_pack_processing_status_topic: str = Field(
        default=...,
        description=(
            "Topic for AEMPack processing-lifecycle (status) events, e.g. processing"
            " failures, and later successes."
        ),
        examples=["aempack-processing-status"],
    )


async def get_aem_pack_dao(
    *, dao_publisher_factory: DaoPublisherFactoryProtocol, topic: str
) -> DaoPublisher[AEMPack]:
    """Construct an outbox DAO for AEMPack objects."""
    return await dao_publisher_factory.get_dao(
        name="aem_packs",
        id_field="id",
        dto_model=AEMPack,
        dto_to_event=lambda aem_pack: aem_pack.model_dump(mode="json"),
        event_topic=topic,
        autopublish=True,
        indexes=[MongoDbIndex(fields={"pid": 1, "model_name": 1})],
    )


async def get_status_event_dao(
    *, dao_publisher_factory: DaoPublisherFactoryProtocol, topic: str
) -> StatusEventDao:
    """Construct an outbox DAO for AEMPack processing-status (lifecycle) events."""
    return await dao_publisher_factory.get_dao(
        name="status_events",
        id_field="id",
        dto_model=AEMPackStatusEvent,
        dto_to_event=lambda event: event.model_dump(mode="json"),
        event_topic=topic,
        autopublish=True,
    )
