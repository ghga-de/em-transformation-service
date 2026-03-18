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

"""Dependency injection and preparation"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext

from hexkit.providers.akafka import (
    ComboTranslator,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.mongodb import MongoDbDaoFactory
from hexkit.providers.mongokafka import MongoKafkaDaoPublisherFactory

from ets.adapters.inbound.event_sub import EventSubTranslator
from ets.adapters.outbound.config_loader import ConfigLoaderAdapter
from ets.adapters.outbound.dao import (
    AEMPackDaoFactory,
    get_persisted_model_dao,
    get_route_dao,
    get_workflow_dao,
)
from ets.config import Config
from ets.core.aem_pack_registry import AEMPackRegistry
from ets.ports.inbound.aem_pack_registry import AEMPackRegistryPort
from ets.ports.outbound.config_loader import ConfigLoaderPort


@asynccontextmanager
async def prepare_config_loader(*, config: Config) -> AsyncGenerator[ConfigLoaderPort]:
    """Constructs config loader instances that can be used by the central core class.

    Factored out for better testability.
    """
    async with MongoDbDaoFactory.construct(config=config) as dao_factory:
        model_dao = await get_persisted_model_dao(dao_factory=dao_factory)
        route_dao = await get_route_dao(dao_factory=dao_factory)
        workflow_dao = await get_workflow_dao(dao_factory=dao_factory)
        yield ConfigLoaderAdapter(
            model_dao=model_dao,
            route_dao=route_dao,
            workflow_dao=workflow_dao,
        )


@asynccontextmanager
async def prepare_aem_pack_registry(
    *,
    config: Config,
) -> AsyncGenerator[AEMPackRegistryPort]:
    """Constructs and initializes core components and their outbound dependencies."""
    async with (
        MongoDbDaoFactory.construct(config=config) as dao_factory,
        MongoKafkaDaoPublisherFactory.construct(config=config) as dao_pub_factory,
    ):
        aem_pack_dao_factory = AEMPackDaoFactory(
            config=config, dao_publisher_factory=dao_pub_factory
        )
        aem_pack_dao = await aem_pack_dao_factory.get_aem_pack_dao()
        config_loader = ConfigLoaderAdapter(
            model_dao=await get_persisted_model_dao(dao_factory=dao_factory),
            route_dao=await get_route_dao(dao_factory=dao_factory),
            workflow_dao=await get_workflow_dao(dao_factory=dao_factory),
        )

        yield AEMPackRegistry(
            aem_pack_dao=aem_pack_dao,
            config_loader=config_loader,
        )


def prepare_aem_pack_registry_with_override(
    *,
    config: Config,
    core_override: AEMPackRegistryPort | None = None,
):
    """Resolve the prepare_core context manager based on config and override (if any)."""
    return (
        nullcontext(core_override)
        if core_override
        else prepare_aem_pack_registry(config=config)
    )


@asynccontextmanager
async def prepare_event_subscriber(
    *,
    config: Config,
    core_override: AEMPackRegistryPort | None = None,
) -> AsyncGenerator[KafkaEventSubscriber]:
    """Construct and initialize an event subscriber with all its dependencies.
    By default, the core dependencies are automatically prepared but you can also
    provide them using the core_override parameter.
    """
    async with (
        prepare_aem_pack_registry_with_override(
            config=config, core_override=core_override
        ) as aem_pack_registry,
        KafkaEventPublisher.construct(config=config) as dlq_publisher,
    ):
        event_sub_translator = EventSubTranslator(
            config=config, aem_pack_registry=aem_pack_registry
        )
        translator = ComboTranslator(translators=[event_sub_translator])
        async with KafkaEventSubscriber.construct(
            config=config, translator=translator, dlq_publisher=dlq_publisher
        ) as event_subscriber:
            yield event_subscriber
