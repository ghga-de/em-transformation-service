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
from typing import Any
from uuid import uuid4

from metldata import get_transformation_registry
from metldata.transform.handling import TransformationHandler
from pydantic import UUID4
from schemapack.spec.datapack import DataPack
from schemapack.spec.schemapack import SchemaPack

from ets.core.models import PersistedConfig, Workflow
from ets.event_schemas import AEMPack, UnprocessedAEMPack
from ets.ports.inbound.aem_pack_registry import (
    AEMPackRegistryPort,
)
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.dao import AEMPackDao

log = logging.getLogger(__name__)


class AEMPackRegistry(AEMPackRegistryPort):
    """Core service for managing AEMPack transformations."""

    def __init__(
        self,
        *,
        aem_pack_dao: AEMPackDao,
        config_loader: ConfigLoaderPort,
        service_instance_id: str,
    ):
        self._aem_pack_dao = aem_pack_dao
        self._config_loader = config_loader
        self._service_instance_id = service_instance_id
        self._transformation_registry = get_transformation_registry()


    async def queue_unprocessed(self, aem_pack: UnprocessedAEMPack):
        """TODO"""
        

    async def process_aem_packs(self) -> None:
        """Derives AEM packs."""
        config = await self._config_loader.load_config_from_db()
        while True:
            async for unprocessed_aem_pack in self._aem_pack_dao.find_all(
                mapping={"original_id": None, "processed": False, "processor": None}
            ):
                unprocessed_aem_pack.processor = self._service_instance_id
                await self._aem_pack_dao.update(unprocessed_aem_pack)
                await self._main_loop(incoming=unprocessed_aem_pack, config=config)
                # DB state might have changed in the meantime, fetch a fresh batch
                break
            else:
                sleep_for = 60
                log.info("Non new AEM found, sleeping for {60} seconds.")
                await asyncio.sleep(sleep_for)

    async def _main_loop(self, *, incoming: UnprocessedAEMPack, config: PersistedConfig):
        """TODO"""
        dirty_map = {
            aem_pack.model_name: aem_pack.id
            async for aem_pack in self._aem_pack_dao.find_all(
                mapping={"original_id": incoming.id}
            )
        }
        transformed_map = {incoming.model_name: incoming}

        aem_packs_to_publish, dirty_map = self._traverse_graph(
            incoming=incoming,
            dirty_map=dirty_map,
            transformed_map=transformed_map,
            config=config,
        )

        if dirty_map:
            # Check if there are corresponding models remaining or if they have been removed from the config.

            # AEMPacks without matching models can be a result of configuration and need to be deleted, as they've become unreachable.
            # Extant AEMPacks with existing models point to the AEMPack moving to a different subgraph with a different original ID:
            # TODO:
            # - Check if that can happen in the program logic
            # - Can this be consolidated or is this a critical, structural error?
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

        # check if another service instance might have already written the derived AEMs due to a race condition
        current_db_aem = await self._aem_pack_dao.get_by_id(incoming.id)
        if current_db_aem.processor != self._service_instance_id:
            log.warning(
                f"Another service instance has already written the derived AEMs to the database. Discarding changes for serivce instance ID {self._service_instance_id}."
            )
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
        aem_packs_to_publish = []
        models_by_name = {model.name: model for model in config.models}
        model_order = {model.name: model.order for model in config.models}
        workflows_by_name = {workflow.name: workflow for workflow in config.workflows}

        current_model_name = incoming.model_name
        original_model = models_by_name.get(current_model_name)
        if not original_model:
            # Needs DLQ setup
            raise ValueError(
                f"No model with name {current_model_name} registered for AEMPack with id {incoming.id}."
            )

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
