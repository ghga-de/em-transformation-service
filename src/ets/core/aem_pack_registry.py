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

"""This module contains functionalities for transforming models and data."""

import asyncio
import logging
from datetime import timedelta
from typing import Any
from uuid import uuid4

from hexkit.utils import now_utc_ms_prec
from metldata import get_transformation_registry
from metldata.transform.handling import TransformationHandler
from pydantic import UUID4
from pymongo import ASCENDING, AsyncMongoClient
from schemapack.spec.datapack import DataPack
from schemapack.spec.schemapack import SchemaPack

from ets.adapters.outbound.dao import UNPROCESSED_AEM_PACK_COLLECTION
from ets.config import Config
from ets.core.models import AEMPack, PersistedConfig, UnprocessedAEMPack, Workflow
from ets.ports.inbound.aem_pack_registry import (
    AEMPackRegistryPort,
)
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.dao import AEMPackDao, UnprocessedAEMPackDao

log = logging.getLogger(__name__)


class AEMPackRegistry(AEMPackRegistryPort):
    """Core service for managing AEMPack transformations."""

    def __init__(
        self,
        *,
        config: Config,
        aem_pack_dao: AEMPackDao,
        unprocessed_aem_pack_dao: UnprocessedAEMPackDao,
        config_loader: ConfigLoaderPort,
        mongo_client: AsyncMongoClient,
    ):
        self._config = config
        self._aem_pack_dao = aem_pack_dao
        self._unprocessed_aem_pack_dao = unprocessed_aem_pack_dao
        self._config_loader = config_loader
        self._mongo_client = mongo_client
        # Bypassing DAO as we need specific atomicity guarantees for the operations
        # DAO based code would need to deal with possible race conditions
        self._unprocessed_aem_pack_collection = mongo_client[config.db_name][
            UNPROCESSED_AEM_PACK_COLLECTION
        ]
        self._transformation_registry = get_transformation_registry()

    async def queue_unprocessed(self, aem_pack: UnprocessedAEMPack):
        """Fetch new AEMs via event subscriber and put them into the queue for processing."""
        doc = aem_pack.model_dump(
            mode="json", exclude={"processor", "started_processing_at"}
        )
        doc["_id"] = doc.pop("id")

        await self._unprocessed_aem_pack_collection.find_one_and_update(
            filter={"_id": aem_pack.id},
            update=[
                {
                    "$set": {
                        **doc,
                        # mark as dirty by setting placeholder processor
                        # this will block processing until the current iteration
                        # is finished and marks it as freed
                        "processor": {
                            "$cond": {
                                "if": "$processor",
                                "then": self._config.dirty_marker,
                                "else": None,
                            }
                        },
                        "started_processing_at": {
                            "$cond": {
                                "if": "$processor",
                                "then": now_utc_ms_prec(),
                                "else": None,
                            }
                        },
                    }
                }
            ],
            upsert=True,
        )

    async def process_aem_packs(self) -> None:
        """Derives AEM packs from incoming AEM."""
        config = await self._config_loader.load_config_from_db()

        while True:
            # Try to fetch fresh AEM first
            unprocessed_aem_pack = (
                await self._unprocessed_aem_pack_collection.find_one_and_update(
                    filter={"original_id": None, "processor": None},
                    update={
                        "$set": {
                            "processor": self._config.service_instance_id,
                            "started_processing_at": now_utc_ms_prec(),
                        }
                    },
                    return_document=True,
                )
            )
            if not unprocessed_aem_pack:
                # No fresh AEMs left, see if a stale one can be grabbed, get the oldest one first in that case
                unprocessed_aem_pack = (
                    await self._unprocessed_aem_pack_collection.find_one_and_update(
                        filter={
                            "original_id": None,
                            "started_processing_at": {
                                "$lt": now_utc_ms_prec()
                                - timedelta(self._config.stale_after)
                            },
                        },
                        update={
                            "$set": {
                                "processor": self._config.service_instance_id,
                                "started_processing_at": now_utc_ms_prec(),
                            }
                        },
                        sort=[("started_processing_at", ASCENDING)],
                        return_document=True,
                    )
                )

            if unprocessed_aem_pack:
                await self._process_next_aem_pack(
                    incoming=unprocessed_aem_pack, config=config
                )
            else:
                log.info(
                    f"No new AEM found, sleeping for {self._config.sleep_for} seconds."
                )
                await asyncio.sleep(self._config.sleep_for)

    async def _process_next_aem_pack(
        self, *, incoming: UnprocessedAEMPack, config: PersistedConfig
    ):
        """Perform transformation on the whole subgraph matching the incoming AEMs ingress model."""
        dirty_map = {
            aem_pack.model_name: aem_pack.id
            async for aem_pack in self._aem_pack_dao.find_all(
                mapping={"original_id": incoming.id}
            )
        }

        transformed_map: dict[str, AEMPack] = {
            incoming.model_name: AEMPack(
                **incoming.model_dump(exclude={"processed", "started_processing_at"})
            )
        }

        aem_packs_to_publish, dirty_map = self._traverse_graph(
            incoming=incoming,
            dirty_map=dirty_map,
            transformed_map=transformed_map,
            config=config,
        )

        if dirty_map:
            # Check if there are corresponding models remaining or if they have been removed from the config.

            # AEMPacks without matching models can be a result of configuration and need to be deleted, as they've become unreachable.
            # Extant AEMPacks with existing models point to the AEMPack moving to a different subgraph with a different original ID.
            # In this case it also needs to be removed an recreated
            models_by_name = {model.name: model for model in config.models}
            for model_name, aem_pack_id in dirty_map.items():
                if not models_by_name.get(model_name):
                    log.warning(
                        f"Model with name {model_name} no longer exists in the config, previously derived AEMPack with id {aem_pack_id} is no longer valid. Removing."
                    )
                    await self._aem_pack_dao.delete(aem_pack_id)
                else:
                    log.warning(
                        f"Derived AEMPack with id {aem_pack_id} is no reachable from its previous original ID. Removing."
                    )
                    await self._aem_pack_dao.delete(aem_pack_id)

        # check if an update version of the original AEM might have arrived in the meantime
        current_db_aem = await self._unprocessed_aem_pack_dao.get_by_id(incoming.id)
        if current_db_aem.processor == self._config.dirty_marker:
            log.warning(
                f"A different version of the ingress AEM {incoming.id} has been received during processing. Discarding changes."
            )
            # free for processing
            current_db_aem.processor = None
            current_db_aem.started_processing_at = None
            await self._unprocessed_aem_pack_dao.upsert(current_db_aem)
            return

        for aem_pack in aem_packs_to_publish:
            log.info(f"Upserting derived AEM Pack {aem_pack.id}")
            await self._aem_pack_dao.upsert(aem_pack)

    def _traverse_graph(
        self,
        *,
        incoming: AEMPack,
        dirty_map: dict[str, UUID4],
        transformed_map: dict[str, AEMPack],
        config: PersistedConfig,
    ) -> tuple[list[AEMPack], dict[str, UUID4]]:
        """Traverse the transformation graph in topological order, applying workflows to produce transformed AEMPacks."""
        aem_packs_to_publish: list[AEMPack | UnprocessedAEMPack] = []
        models_by_name = {model.name: model for model in config.models}
        model_order = {model.name: model.order for model in config.models}
        workflows_by_name = {workflow.name: workflow for workflow in config.workflows}

        current_model_name = incoming.model_name
        ingress_model = models_by_name.get(current_model_name)
        if not ingress_model:
            # Needs DLQ setup
            raise ValueError(
                f"No model with name {current_model_name} registered for AEMPack with id {incoming.id}."
            )
        if ingress_model.publish:
            aem_packs_to_publish.append(incoming)

        current_routes = sorted(
            [
                route
                for route in config.routes
                if route.input_model_name == current_model_name
            ],
            key=lambda route: model_order[route.output_model_name],
        )

        while transformed_map:
            for route in current_routes:
                current_aem_pack = transformed_map[current_model_name]
                current_model = models_by_name[current_model_name]
                current_workflow = workflows_by_name[route.workflow_name]
                if not current_aem_pack:
                    raise ValueError("Invalid state, TODO")

                transformed_data = self._apply_workflow_to_data(
                    data=current_aem_pack.data,
                    annotation={},
                    input_schema=current_model.schema_,
                    workflow=current_workflow,
                )
                transformed_aem_pack = self._create_aem_pack(
                    data=transformed_data,
                    model_name=route.output_model_name,
                    original_id=incoming.id,
                    annotation=incoming.annotation,
                )
                # TODO: needs to account for branching here
                transformed_map[route.output_model_name] = transformed_aem_pack
                model = models_by_name[transformed_aem_pack.model_name]
                if model.publish:
                    aem_packs_to_publish.append(transformed_aem_pack)

            # processed, no longer dirty
            dirty_map.pop(current_model_name, None)
            # processed, no longer in queue
            transformed_map.pop(current_model_name)
            # continue with next item in transformation map based on insertion order
            current_aem_pack = next(iter(transformed_map.values()))
            current_model_name = current_aem_pack.model_name
            current_routes = sorted(
                [
                    route
                    for route in config.routes
                    if route.input_model_name == current_model_name
                ],
                key=lambda route: model_order[route.output_model_name],
            )

        return aem_packs_to_publish, dirty_map

    def _apply_workflow_to_data(
        self,
        *,
        data: DataPack,
        annotation: dict,
        input_schema: SchemaPack,
        workflow: Workflow,
    ) -> DataPack:
        """Apply every step of a workflow to a DataPack and return the result."""
        current_data = data
        current_schema = input_schema
        for step in workflow.workflow.operations:
            transformation_def = self._transformation_registry[step.name]
            typed_config = transformation_def.config_cls.model_validate(step.args)
            handler = TransformationHandler(
                transformation_definition=transformation_def,
                transformation_config=typed_config,
                input_model=current_schema,
            )
            current_data = handler.transform_data(current_data, annotation)
            current_schema = handler.transformed_model
        return current_data

    def _create_aem_pack(
        self,
        *,
        aem_id: UUID4 | None = None,
        model_name: str,
        original_id: UUID4,
        data: DataPack,
        annotation: dict[str, Any],
    ) -> AEMPack:
        """TODO"""
        return AEMPack(
            id=aem_id or uuid4(),
            model_name=model_name,
            original_id=original_id,
            data=data,
            annotation=annotation,
        )
