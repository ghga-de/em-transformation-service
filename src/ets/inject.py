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

"""Dependency injection and preparation"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, nullcontext

from hexkit.providers.akafka import (
    ComboTranslator,
    KafkaEventPublisher,
    KafkaEventSubscriber,
)
from hexkit.providers.mongodb import MongoDbDaoFactory

from ets.adapters.inbound.event_sub import EventSubTranslator
from ets.adapters.outbound import dao
from ets.config import Config
from ets.core.aem_pack_registry import AEMPackRegistry
from ets.ports.inbound.aem_pack_registry import AEMPackRegistryPort


@asynccontextmanager
async def prepare_core(
    *,
    config: Config,
) -> AsyncGenerator[AEMPackRegistryPort]:
    """Constructs and initializes core components and their outbound dependencies."""
    async with (
        MongoDbDaoFactory.construct(config=config) as dao_factory,
    ):
        aem_pack_dao = await dao.aem_pack_dao(
            dao_factory=dao_factory,
        )
        yield AEMPackRegistry(aem_pack_dao=aem_pack_dao)


def prepare_core_with_override(
    *,
    config: Config,
    core_override: AEMPackRegistryPort | None = None,
):
    """Resolve the prepare_core context manager based on config and override (if any)."""
    return nullcontext(core_override) if core_override else prepare_core(config=config)


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
        prepare_core_with_override(
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
