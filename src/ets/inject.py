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
from dataclasses import asdict, dataclass

from hexkit.providers.akafka import (
    ComboTranslator,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.mongodb import ConfiguredMongoClient, MongoDbDaoFactory
from hexkit.providers.mongokafka import MongoKafkaDaoPublisherFactory

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
from ets.core.config_comparator import ConfigComparator
from ets.core.config_manager import ConfigManager
from ets.core.config_validator import ConfigValidator
from ets.core.model_derivation import ModelDeriver
from ets.ports.inbound.aem_pack_registry import AEMPackRegistryPort
from ets.ports.inbound.config_comparator import ConfigComparatorPort
from ets.ports.inbound.config_manager import ConfigManagerPort
from ets.ports.inbound.config_validator import ConfigValidatorPort
from ets.ports.inbound.model_derivation import ModelDeriverPort
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_lock import ConfigLockPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.config_writer import ConfigWriterPort
from ets.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort


@dataclass
class ConfigAdapters:
    """Holds the config loader, writer, and version adapters sharing the same DAO instances."""

    comparator: ConfigComparatorPort
    loader: ConfigLoaderPort
    model_deriver: ModelDeriverPort
    validator: ConfigValidatorPort
    versioner: ConfigVersionerPort
    writer: ConfigWriterPort


@asynccontextmanager
async def prepare_config_adapters(
    *, config: Config, mongo_client: ConfiguredMongoClient | None = None
) -> AsyncGenerator[ConfigAdapters]:
    """Constructs config loader and writer instances sharing a single MongoDB connection.

    Factored out for better testability.
    """
    async with (
        MongoDbDaoFactory.construct(config=config) as dao_factory,
        ConfiguredMongoClient(config=config) as mongo_client,
    ):
        model_dao = await get_persisted_model_dao(dao_factory=dao_factory)
        route_dao = await get_route_dao(dao_factory=dao_factory)
        workflow_dao = await get_workflow_dao(dao_factory=dao_factory)
        config_version = ConfigVersioner(
            collection=mongo_client[config.db_name][CONFIG_VERSION_COLLECTION]
        )
        config_loader = ConfigLoaderAdapter(
            model_dao=model_dao, route_dao=route_dao, workflow_dao=workflow_dao
        )
        config_writer = ConfigWriterAdapter(
            model_dao=model_dao,
            route_dao=route_dao,
            workflow_dao=workflow_dao,
            config_versioner=config_version,
        )
        yield ConfigAdapters(
            comparator=ConfigComparator(),
            loader=config_loader,
            model_deriver=ModelDeriver(),
            validator=ConfigValidator(),
            versioner=config_version,
            writer=config_writer,
        )


@asynccontextmanager
async def prepare_config_manager(
    *, config: Config
) -> AsyncGenerator[ConfigManagerPort]:
    """Construct a fully wired ConfigManager.

    Reuses prepare_config_adapters so loader, writer, and version share the same
    DAO instances. Construction is I/O-free; the actual load/write happens when
    resolve_and_persist() is called.
    """
    async with prepare_config_adapters(config=config) as adapters:
        yield ConfigManager(**asdict(adapters))


@asynccontextmanager
async def prepare_config_lock(
    *, config: Config, mongo_client: ConfiguredMongoClient | None = None
) -> AsyncGenerator[ConfigLockPort]:
    """Construct a ConfigLockAdapter backed by the config_lock collection."""
    async with ConfiguredMongoClient(config=config) as mongo_client:
        collection = mongo_client[config.db_name][CONFIG_LOCK_COLLECTION]
        yield ConfigLockAdapter(
            collection=collection,
            worker_id=config.worker_id,
            lock_expiry_seconds=config.lock_expiry_seconds,
            poll_interval=config.lock_poll_interval,
            timeout=config.lock_timeout,
        )


@asynccontextmanager
async def prepare_incoming_aem_pack_queue(
    *,
    config: Config,
    mongo_client: ConfiguredMongoClient | None = None,
) -> AsyncGenerator[IncomingAEMPackQueuePort]:
    """Construct an IncomingAEMPackQueue backed by the incoming AEMPack collection."""
    async with ConfiguredMongoClient(config=config) as mongo_client:
        yield IncomingAEMPackQueue(
            collection=mongo_client[config.db_name][INCOMING_AEM_PACK_COLLECTION],
            worker_id=config.worker_id,
        )


@asynccontextmanager
async def prepare_aem_pack_registry(
    *,
    config: Config,
    aem_pack_queue_override: IncomingAEMPackQueuePort | None = None,
) -> AsyncGenerator[AEMPackRegistryPort]:
    """Constructs and initializes core components and their outbound dependencies."""
    async with (
        prepare_config_adapters(config=config) as adapters,
        prepare_config_lock(config=config) as config_lock,
        MongoKafkaDaoPublisherFactory.construct(config=config) as dao_pub_factory,
        nullcontext(aem_pack_queue_override)
        if aem_pack_queue_override
        else prepare_incoming_aem_pack_queue(config=config) as incoming_aem_pack_queue,
    ):
        aem_pack_dao = await get_aem_pack_dao(
            dao_publisher_factory=dao_pub_factory,
            topic=config.derived_aem_pack_topic,
        )
        config_manager = ConfigManager(
            comparator=ConfigComparator(),
            loader=adapters.loader,
            versioner=adapters.versioner,
            model_deriver=ModelDeriver(),
            validator=ConfigValidator(),
            writer=adapters.writer,
        )

        yield AEMPackRegistry(
            config=config,
            aem_pack_dao=aem_pack_dao,
            config_manager=config_manager,
            config_lock=config_lock,
            incoming_aem_pack_queue=incoming_aem_pack_queue,
        )


def prepare_aem_pack_registry_with_override(
    *,
    config: Config,
    core_override: AEMPackRegistryPort | None = None,
    aem_pack_queue_override: IncomingAEMPackQueuePort | None = None,
):
    """Resolve the prepare_core context manager based on config and override (if any)."""
    return (
        nullcontext(core_override)
        if core_override
        else prepare_aem_pack_registry(
            config=config, aem_pack_queue_override=aem_pack_queue_override
        )
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
    async with (
        prepare_aem_pack_registry_with_override(
            config=config,
            core_override=core_override,
            aem_pack_queue_override=aem_pack_queue_override,
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
