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

import logging

from hexkit.protocols.dao import DaoFactoryProtocol
from hexkit.protocols.daopub import DaoPublisher, DaoPublisherFactoryProtocol
from pydantic import Field
from pydantic_settings import BaseSettings

from ets.core import models
from ets.core.models import AEMPack
from ets.ports.outbound.dao import (
    AEMPackEventPublisherPort,
    ModelDao,
    RouteDao,
    UnprocessedAEMPackDao,
    WorkflowDao,
)

log = logging.getLogger(__name__)

UNPROCESSED_AEM_PACK_COLLECTION = "unprocessed_aem_packs"


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


async def get_unprocessed_aem_pack_dao(
    *, dao_factory: DaoFactoryProtocol
) -> UnprocessedAEMPackDao:
    """Setup the Unprocessed AEM Pack DAO using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name=UNPROCESSED_AEM_PACK_COLLECTION,
        dto_model=models.IncomingAEMPack,
        id_field="id",
    )


class AEMPackDaoConfig(BaseSettings):
    """Config for the AEMPack event publisher adapter."""

    derived_aem_pack_topic: str = Field(
        default=...,
        description="Topic for events informing about derived AEMs.",
        examples=["derived-aems"],
    )


class AEMPackDaoFactory(AEMPackEventPublisherPort):
    """Adapter translating domain publish calls into Kafka events."""

    def __init__(
        self,
        *,
        config: AEMPackDaoConfig,
        dao_publisher_factory: DaoPublisherFactoryProtocol,
    ):
        self._aem_pack_topic = config.derived_aem_pack_topic
        self._dao_publisher_factory = dao_publisher_factory

    async def get_aem_pack_dao(self) -> DaoPublisher[AEMPack]:
        """Construct an outbox DAO for AEMPack objects."""
        return await self._dao_publisher_factory.get_dao(
            name="aem_packs",
            id_field="id",
            dto_model=AEMPack,
            dto_to_event=lambda aem: aem.model_dump(mode="json"),
            event_topic=self._aem_pack_topic,
            autopublish=True,
        )
