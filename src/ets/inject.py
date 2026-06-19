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
from contextlib import asynccontextmanager
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
    get_status_event_dao,
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
from ets.ports.inbound.aem_pack_registry import AEMPackRegistryPort
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_lock import ConfigLockPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.config_writer import ConfigWriterPort
from ets.ports.outbound.dao import AEMPackDao, ModelDao, RouteDao, WorkflowDao
from ets.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort


@dataclass
class _BaseWiring:
    """Contains everything reused across all higher-level preparation steps."""

    config_updater: ConfigUpdater
    config_lock: ConfigLockPort
    versioner: ConfigVersionerPort
    incoming_aem_pack_queue: IncomingAEMPackQueuePort
    model_dao: ModelDao
    route_dao: RouteDao
    workflow_dao: WorkflowDao
    loader: ConfigLoaderPort
    writer: ConfigWriterPort


@dataclass
class Wiring(_BaseWiring):
    """Extends ``_BaseWiring`` with Kafka specifics and the shared Mongo client handle.

    Production code only needs the registry. Tests use ``prepare_wiring`` to seed and
    inspect the underlying DAOs and adapters without opening duplicate Mongo or Kafka
    clients.
    """

    aem_pack_registry: AEMPackRegistry
    aem_pack_dao: AEMPackDao
    mongo_client: AsyncMongoClient
    event_publisher: KafkaEventPublisher


@asynccontextmanager
async def _prepare_base_wiring(
    *, config: Config, client: AsyncMongoClient
) -> AsyncGenerator[_BaseWiring]:
    """Wire all Mongo-backed entities off a caller-supplied Mongo client."""
    db = client[config.db_name]
    dao_factory = MongoDbDaoFactory(config=config, client=client)
    model_dao = await get_persisted_model_dao(dao_factory=dao_factory)
    route_dao = await get_route_dao(dao_factory=dao_factory)
    workflow_dao = await get_workflow_dao(dao_factory=dao_factory)
    versioner = ConfigVersioner(collection=db[CONFIG_VERSION_COLLECTION])
    loader = ConfigLoaderAdapter(
        model_dao=model_dao, route_dao=route_dao, workflow_dao=workflow_dao
    )
    writer = ConfigWriterAdapter(
        model_dao=model_dao,
        route_dao=route_dao,
        workflow_dao=workflow_dao,
        config_versioner=versioner,
    )
    config_updater = ConfigUpdater(
        loader=loader,
        versioner=versioner,
        writer=writer,
    )
    config_lock = ConfigLockAdapter(
        collection=db[CONFIG_LOCK_COLLECTION],
        worker_id=config.worker_id,
        lock_expiry_seconds=config.config_lock_expiry_seconds,
        poll_interval=config.config_lock_poll_interval,
        timeout=config.config_lock_timeout,
    )
    incoming_aem_pack_queue = IncomingAEMPackQueue(
        collection=db[INCOMING_AEM_PACK_COLLECTION], worker_id=config.worker_id
    )
    yield _BaseWiring(
        config_updater=config_updater,
        config_lock=config_lock,
        versioner=versioner,
        incoming_aem_pack_queue=incoming_aem_pack_queue,
        model_dao=model_dao,
        route_dao=route_dao,
        workflow_dao=workflow_dao,
        loader=loader,
        writer=writer,
    )


@asynccontextmanager
async def prepare_config_manager(*, config: Config) -> AsyncGenerator[ConfigManager]:
    """Construct and initialize a ConfigManager with all its dependencies."""
    async with (
        ConfiguredMongoClient(config=config) as client,
        _prepare_base_wiring(config=config, client=client) as base,
    ):
        yield ConfigManager(
            input_config_path=config.processing_configuration_path,
            config_lock=base.config_lock,
            config_updater=base.config_updater,
            incoming_aem_pack_queue=base.incoming_aem_pack_queue,
        )


@asynccontextmanager
async def prepare_wiring(*, config: Config) -> AsyncGenerator[Wiring]:
    """Open one Mongo client and one Kafka publisher; wire everything off them."""
    async with (
        ConfiguredMongoClient(config=config) as client,
        KafkaEventPublisher.construct(config=config) as event_publisher,
        _prepare_base_wiring(config=config, client=client) as base,
    ):
        dao_pub_factory = MongoKafkaDaoPublisherFactory(
            config=config, event_publisher=event_publisher, db_client=client
        )
        aem_pack_dao = await get_aem_pack_dao(
            dao_publisher_factory=dao_pub_factory,
            topic=config.derived_aem_pack_topic,
        )
        status_event_dao = await get_status_event_dao(
            dao_publisher_factory=dao_pub_factory,
            topic=config.aem_pack_processing_status_topic,
        )
        aem_pack_registry = AEMPackRegistry(
            config=config,
            aem_pack_dao=aem_pack_dao,
            status_event_dao=status_event_dao,
            config_updater=base.config_updater,
            config_lock=base.config_lock,
            incoming_aem_pack_queue=base.incoming_aem_pack_queue,
        )
        yield Wiring(
            **vars(base),
            aem_pack_registry=aem_pack_registry,
            aem_pack_dao=aem_pack_dao,
            mongo_client=client,
            event_publisher=event_publisher,
        )


@asynccontextmanager
async def prepare_aem_pack_registry(
    *, config: Config
) -> AsyncGenerator[AEMPackRegistryPort]:
    """Yield an AEMPackRegistry with all its outbound dependencies wired up."""
    async with prepare_wiring(config=config) as wiring:
        yield wiring.aem_pack_registry


@asynccontextmanager
async def prepare_event_subscriber(
    *, config: Config
) -> AsyncGenerator[KafkaEventSubscriber]:
    """Construct an event subscriber that reuses the wiring's Kafka publisher."""
    async with prepare_wiring(config=config) as wiring:
        event_sub_translator = EventSubTranslator(
            config=config, aem_pack_registry=wiring.aem_pack_registry
        )
        translator = ComboTranslator(translators=[event_sub_translator])
        async with KafkaEventSubscriber.construct(
            config=config, translator=translator, dlq_publisher=wiring.event_publisher
        ) as event_subscriber:
            yield event_subscriber
