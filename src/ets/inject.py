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
from dataclasses import dataclass

from hexkit.providers.akafka import (
    ComboTranslator,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.mongodb import ConfiguredMongoClient, MongoDbDaoFactory
from hexkit.providers.mongokafka import MongoKafkaDaoPublisherFactory
from pymongo import AsyncMongoClient

from ets.adapters.inbound.event_sub import EventSubTranslator
from ets.adapters.outbound.config_loader import ConfigLoaderAdapter
from ets.adapters.outbound.config_lock import ConfigLockAdapter
from ets.adapters.outbound.config_version import ConfigVersioner
from ets.adapters.outbound.config_writer import ConfigWriterAdapter
from ets.adapters.outbound.dao import (
    get_aem_pack_dao,
    get_persisted_model_dao,
    get_route_dao,
    get_workflow_dao,
)
from ets.adapters.outbound.incoming_aem_pack_queue import IncomingAEMPackQueue
from ets.config import Config
from ets.constants import (
    CONFIG_LOCK_COLLECTION,
    CONFIG_VERSION_COLLECTION,
    INCOMING_AEM_PACK_COLLECTION,
)
from ets.core.aem_pack_registry import AEMPackRegistry
from ets.core.config_manager import ConfigManager
from ets.core.config_updater import ConfigUpdater
from ets.core.model_derivation import ModelDeriver
from ets.ports.inbound.aem_pack_registry import AEMPackRegistryPort
from ets.ports.inbound.config_manager import ConfigManagerPort
from ets.ports.inbound.config_updater import ConfigUpdaterPort
from ets.ports.outbound.config_lock import ConfigLockPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort


@dataclass
class _ConfigStack:
    """Config-related entities reused by higher level preparation steps."""

    config_manager: ConfigManagerPort
    config_lock: ConfigLockPort
    versioner: ConfigVersionerPort
    incoming_aem_pack_queue: IncomingAEMPackQueuePort


@asynccontextmanager
async def _prepare_config_stack(
    *,
    config: Config,
    mongo_client: AsyncMongoClient | None = None,
    aem_pack_queue_override: IncomingAEMPackQueuePort | None = None,
) -> AsyncGenerator[_ConfigStack]:
    """Wire all config-related entities using a single shared Mongo client.

    If `mongo_client` is provided, the caller retains ownership of its lifetime;
    otherwise a fresh client is opened for this scope.
    """
    client_ctx = (
        nullcontext(mongo_client)
        if mongo_client
        else ConfiguredMongoClient(config=config)
    )
    async with client_ctx as client:
        dao_factory = MongoDbDaoFactory(config=config, client=client)
        model_dao = await get_persisted_model_dao(dao_factory=dao_factory)
        route_dao = await get_route_dao(dao_factory=dao_factory)
        workflow_dao = await get_workflow_dao(dao_factory=dao_factory)
        versioner = ConfigVersioner(
            collection=client[config.db_name][CONFIG_VERSION_COLLECTION]
        )
        config_manager = ConfigManager(
            loader=ConfigLoaderAdapter(
                model_dao=model_dao, route_dao=route_dao, workflow_dao=workflow_dao
            ),
            versioner=versioner,
            model_deriver=ModelDeriver(),
            writer=ConfigWriterAdapter(
                model_dao=model_dao,
                route_dao=route_dao,
                workflow_dao=workflow_dao,
                config_versioner=versioner,
            ),
        )
        config_lock = ConfigLockAdapter(
            collection=client[config.db_name][CONFIG_LOCK_COLLECTION],
            worker_id=config.worker_id,
            lock_expiry_seconds=config.lock_expiry_seconds,
            poll_interval=config.lock_poll_interval,
            timeout=config.lock_timeout,
        )
        incoming_aem_pack_queue = aem_pack_queue_override or IncomingAEMPackQueue(
            collection=client[config.db_name][INCOMING_AEM_PACK_COLLECTION],
            worker_id=config.worker_id,
        )
        yield _ConfigStack(
            config_manager=config_manager,
            config_lock=config_lock,
            versioner=versioner,
            incoming_aem_pack_queue=incoming_aem_pack_queue,
        )


@asynccontextmanager
async def prepare_config_updater(
    *, config: Config
) -> AsyncGenerator[ConfigUpdaterPort]:
    """Construct and initialize an event subscriber with all its dependencies."""
    async with _prepare_config_stack(config=config) as stack:
        yield ConfigUpdater(
            input_config_path=config.input_config_path,
            config_lock=stack.config_lock,
            config_manager=stack.config_manager,
            versioner=stack.versioner,
            incoming_aem_pack_queue=stack.incoming_aem_pack_queue,
        )


@asynccontextmanager
async def prepare_aem_pack_registry(
    *,
    config: Config,
    aem_pack_queue_override: IncomingAEMPackQueuePort | None = None,
) -> AsyncGenerator[AEMPackRegistryPort]:
    """Constructs and initializes core components and their outbound dependencies.

    A single Mongo client is shared between the config stack and the
    derived-AEMPack publisher factory; a single Kafka event publisher is
    reused by the publisher factory.
    """
    async with (
        ConfiguredMongoClient(config=config) as client,
        KafkaEventPublisher.construct(config=config) as event_publisher,
        _prepare_config_stack(
            config=config,
            mongo_client=client,
            aem_pack_queue_override=aem_pack_queue_override,
        ) as stack,
    ):
        dao_pub_factory = MongoKafkaDaoPublisherFactory(
            config=config, event_publisher=event_publisher, db_client=client
        )
        aem_pack_dao = await get_aem_pack_dao(
            dao_publisher_factory=dao_pub_factory,
            topic=config.derived_aem_pack_topic,
        )
        yield AEMPackRegistry(
            config=config,
            aem_pack_dao=aem_pack_dao,
            config_manager=stack.config_manager,
            config_lock=stack.config_lock,
            incoming_aem_pack_queue=stack.incoming_aem_pack_queue,
        )


@asynccontextmanager
async def prepare_event_subscriber(
    *,
    config: Config,
    core_override: AEMPackRegistryPort | None = None,
    aem_pack_queue_override: IncomingAEMPackQueuePort | None = None,
) -> AsyncGenerator[KafkaEventSubscriber]:
    """Construct and initialize an event subscriber with all its dependencies.

    By default, the core dependencies are automatically prepared but you can also
    provide them using the core_override parameter.
    """
    registry_ctx = (
        nullcontext(core_override)
        if core_override
        else prepare_aem_pack_registry(
            config=config, aem_pack_queue_override=aem_pack_queue_override
        )
    )
    async with (
        registry_ctx as aem_pack_registry,
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
