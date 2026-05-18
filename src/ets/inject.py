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
from ets.core.config_comparator import ConfigComparator
from ets.core.config_manager import ConfigManager
from ets.core.config_updater import ConfigUpdater
from ets.core.config_validator import ConfigValidator
from ets.core.model_derivation import ModelDeriver
from ets.ports.inbound.aem_pack_registry import AEMPackRegistryPort
from ets.ports.inbound.config_manager import ConfigManagerPort
from ets.ports.inbound.config_updater import ConfigUpdaterPort
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_lock import ConfigLockPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.config_writer import ConfigWriterPort
from ets.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort


def _open_or_share_client(*, config: Config, mongo_client: AsyncMongoClient | None):
    """Reuse an injected Mongo client or open a fresh one for the local scope."""
    return (
        nullcontext(mongo_client)
        if mongo_client
        else ConfiguredMongoClient(config=config)
    )


@dataclass
class ConfigHelpers:
    """Holds all config helpers used by the manager, sharing the same mongo client and DAO instances."""

    comparator: ConfigComparator
    loader: ConfigLoaderPort
    model_deriver: ModelDeriver
    validator: ConfigValidator
    versioner: ConfigVersionerPort
    writer: ConfigWriterPort


@dataclass
class ConfigStack:
    """Config-related collaborators wired off a shared Mongo client."""

    config_manager: ConfigManagerPort
    config_lock: ConfigLockPort
    versioner: ConfigVersionerPort
    incoming_aem_pack_queue: IncomingAEMPackQueuePort


@asynccontextmanager
async def prepare_config_helpers(
    *,
    config: Config,
    mongo_client: AsyncMongoClient | None = None,
) -> AsyncGenerator[ConfigHelpers]:
    """Constructs config loader and writer instances sharing a single MongoDB connection.

    Factored out for better testability.
    """
    async with (
        _open_or_share_client(config=config, mongo_client=mongo_client) as client,
        MongoDbDaoFactory.construct(config=config) as dao_factory,
    ):
        model_dao = await get_persisted_model_dao(dao_factory=dao_factory)
        route_dao = await get_route_dao(dao_factory=dao_factory)
        workflow_dao = await get_workflow_dao(dao_factory=dao_factory)
        config_version = ConfigVersioner(
            collection=client[config.db_name][CONFIG_VERSION_COLLECTION]
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
        yield ConfigHelpers(
            comparator=ConfigComparator(),
            loader=config_loader,
            model_deriver=ModelDeriver(),
            validator=ConfigValidator(),
            versioner=config_version,
            writer=config_writer,
        )


@asynccontextmanager
async def prepare_config_manager(
    *,
    config: Config,
    mongo_client: AsyncMongoClient | None = None,
    helpers: ConfigHelpers | None = None,
) -> AsyncGenerator[ConfigManagerPort]:
    """Construct a fully wired ConfigManager.

    Reuses prepare_config_helpers so loader, writer, and version share the same
    DAO instances. If `helpers` is supplied, it is reused as-is (the caller
    keeps ownership of its context); otherwise a fresh set is constructed.
    Construction is I/O-free; the actual load/write happens when
    resolve_and_persist() is called.
    """
    async with (
        prepare_config_helpers(config=config, mongo_client=mongo_client)
        if helpers is None
        else nullcontext(helpers)
    ) as adapters:
        yield ConfigManager(
            comparator=adapters.comparator,
            loader=adapters.loader,
            versioner=adapters.versioner,
            model_deriver=adapters.model_deriver,
            validator=adapters.validator,
            writer=adapters.writer,
        )


@asynccontextmanager
async def prepare_config_lock(
    *, config: Config, mongo_client: AsyncMongoClient | None = None
) -> AsyncGenerator[ConfigLockPort]:
    """Construct a ConfigLockAdapter backed by the config_lock collection."""
    async with _open_or_share_client(
        config=config, mongo_client=mongo_client
    ) as client:
        yield ConfigLockAdapter(
            collection=client[config.db_name][CONFIG_LOCK_COLLECTION],
            worker_id=config.worker_id,
            lock_expiry_seconds=config.lock_expiry_seconds,
            poll_interval=config.lock_poll_interval,
            timeout=config.lock_timeout,
        )


@asynccontextmanager
async def prepare_incoming_aem_pack_queue(
    *,
    config: Config,
    mongo_client: AsyncMongoClient | None = None,
) -> AsyncGenerator[IncomingAEMPackQueuePort]:
    """Construct an IncomingAEMPackQueue backed by the incoming AEMPack collection."""
    async with _open_or_share_client(
        config=config, mongo_client=mongo_client
    ) as client:
        yield IncomingAEMPackQueue(
            collection=client[config.db_name][INCOMING_AEM_PACK_COLLECTION],
            worker_id=config.worker_id,
        )


@asynccontextmanager
async def prepare_config_stack(
    *,
    config: Config,
    mongo_client: AsyncMongoClient | None = None,
    aem_pack_queue_override: IncomingAEMPackQueuePort | None = None,
) -> AsyncGenerator[ConfigStack]:
    """Wire config manager, lock, versioner, and incoming queue under a shared Mongo client."""
    async with (
        _open_or_share_client(config=config, mongo_client=mongo_client) as client,
        prepare_config_helpers(config=config, mongo_client=client) as helpers,
        prepare_config_manager(config=config, helpers=helpers) as config_manager,
        prepare_config_lock(config=config, mongo_client=client) as config_lock,
        (
            nullcontext(aem_pack_queue_override)
            if aem_pack_queue_override
            else prepare_incoming_aem_pack_queue(config=config, mongo_client=client)
        ) as incoming_aem_pack_queue,
    ):
        yield ConfigStack(
            config_manager=config_manager,
            config_lock=config_lock,
            versioner=helpers.versioner,
            incoming_aem_pack_queue=incoming_aem_pack_queue,
        )


@asynccontextmanager
async def prepare_config_updater(
    *,
    config: Config,
    mongo_client: AsyncMongoClient | None = None,
) -> AsyncGenerator[ConfigUpdaterPort]:
    """Construct the startup-time ConfigUpdater with all its collaborators.

    A single MongoDB client is shared between the config helpers, config
    manager, config lock, and incoming AEMPack queue unless one is injected.
    """
    async with prepare_config_stack(config=config, mongo_client=mongo_client) as stack:
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

    By default, a single MongoDB client is created and shared between the
    config adapters, config lock, and incoming AEMPack queue.
    """
    async with (
        prepare_config_stack(
            config=config, aem_pack_queue_override=aem_pack_queue_override
        ) as stack,
        MongoKafkaDaoPublisherFactory.construct(config=config) as dao_pub_factory,
    ):
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
            config=config,
            aem_pack_queue_override=aem_pack_queue_override,
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
    provide them using the core_override parameter. A single MongoDB client is
    created and shared between the three entities the subscriber depends on
    (config lock, config manager, incoming AEMPack queue) unless one is injected.
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
