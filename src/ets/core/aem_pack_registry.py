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

"""Contains logic for AEMPack derivation."""

import asyncio
import logging
from typing import Any
from uuid import uuid4

from hexkit.correlation import set_correlation_id
from metldata import get_transformation_registry
from metldata.transform.handling import TransformationHandler
from pydantic import UUID4, BaseModel, ConfigDict
from schemapack import SchemaPackValidator
from schemapack.exceptions import ValidationError
from schemapack.spec.datapack import DataPack
from schemapack.spec.schemapack import SchemaPack

from ets.config import Config
from ets.core.models import AEMPack, PersistedConfig, Workflow
from ets.ports.inbound.aem_pack_registry import (
    AEMPackRegistryPort,
)
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.config_lock import ConfigLockPort
from ets.ports.outbound.config_version import ConfigVersionerPort
from ets.ports.outbound.dao import AEMPackDao
from ets.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort

log = logging.getLogger(__name__)


class _AnnotationModel(BaseModel):
    """Wraps a plain annotation dict to satisfy the BaseModel-bound SubmissionAnnotation TypeVar."""

    model_config = ConfigDict(extra="allow")


class AEMPackRegistry(AEMPackRegistryPort):
    """Core service for managing AEMPack transformations."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        config: Config,
        aem_pack_dao: AEMPackDao,
        config_loader: ConfigLoaderPort,
        config_lock: ConfigLockPort,
        config_versioner: ConfigVersionerPort,
        incoming_aem_pack_queue: IncomingAEMPackQueuePort,
    ):
        self._config = config
        self._aem_pack_dao = aem_pack_dao
        self._config_loader = config_loader
        self._config_lock = config_lock
        self._config_versioner = config_versioner
        self._incoming_aem_pack_queue = incoming_aem_pack_queue
        self._transformation_registry = get_transformation_registry()
        self._known_config_version: int = 0
        self._graph_config: PersistedConfig | None = None

    @property
    def graph_config(self) -> PersistedConfig:
        """Wrapper around PersistedConfig access used in conjunction with deferred loading."""
        if self._graph_config is None:
            raise ValueError(
                "Could not access graph config as it hasn't been loaded yet."
            )
        return self._graph_config

    async def _reload_config_if_changed(self) -> bool:
        """Check config version and reload from DB if it changed."""
        current_version = await self._config_versioner.get_version()
        if self._graph_config is None:
            log.info("Loading initial config (version %d).", current_version)
            self._graph_config = await self._config_loader.load_config_from_db()
            self._known_config_version = current_version
            return True
        elif self._known_config_version < current_version:
            log.info(
                "Config version changed (%d -> %d), reloading.",
                self._known_config_version,
                current_version,
            )
            self._graph_config = await self._config_loader.load_config_from_db()
            self._known_config_version = current_version
            return True
        elif self._known_config_version > current_version:
            inconsistent_version = ValueError(
                f"Encountered inconsistent current config version: {current_version}. Worker config version: {self._known_config_version}"
            )
            log.critical(inconsistent_version)
            raise inconsistent_version
        return False

    async def queue_unprocessed(self, aem_pack: AEMPack):
        """Fetch new AEMPacks via event subscriber and put them into the queue for processing."""
        await self._config_lock.wait_for_lock_release()
        await self._reload_config_if_changed()

        matching_model = next(
            (m for m in self.graph_config.models if m.name == aem_pack.model_name), None
        )

        if not matching_model:
            model_lookup_error = ValueError(
                f"No model with name {aem_pack.model_name} registered for AEMPack with id {aem_pack.id}."
            )
            log.error(model_lookup_error)
            raise model_lookup_error

        # Validate DataPack against corresponding SchemaPack
        validator = SchemaPackValidator(schemapack=matching_model.schema_)
        try:
            validator.validate(datapack=aem_pack.data)
        except ValidationError as error:
            log.error(error)
            raise

        await self._incoming_aem_pack_queue.queue(aem_pack)

    async def process_aem_packs(self) -> None:
        """Derives AEMPacks from incoming AEMPacks."""
        while True:
            await self._config_lock.wait_for_lock_release()
            await self._reload_config_if_changed()
            claimed = await self._incoming_aem_pack_queue.claim_next()
            if claimed:
                await self._process_next_aem_pack(
                    incoming_aem=claimed, correlation_id=claimed.correlation_id
                )
            else:
                log.info(
                    f"No new AEM found, sleeping for {self._config.sleep_for} seconds."
                )
                await asyncio.sleep(self._config.sleep_for)

    async def _process_next_aem_pack(
        self, *, incoming_aem: AEMPack, correlation_id: UUID4
    ):
        """Perform transformation on the whole subgraph matching the incoming AEMPack ingress model."""
        dirty_map = {
            aem_pack.model_name: aem_pack.id
            async for aem_pack in self._aem_pack_dao.find_all(
                mapping={"pid": incoming_aem.pid}
            )
        }
        transformed_map: dict[str, AEMPack] = {incoming_aem.model_name: incoming_aem}

        aem_packs_to_publish, dirty_map = self._traverse_graph(
            incoming=incoming_aem, dirty_map=dirty_map, transformed_map=transformed_map
        )

        await self._config_lock.wait_for_lock_release()
        config_changed = await self._reload_config_if_changed()

        if config_changed:
            log.info(
                "Graph config changed while processing AEMPack '%s'.\n"
                + "Discarding changes and freeing for reprocessing with new config",
                incoming_aem.id,
            )
            await self._incoming_aem_pack_queue.free(incoming_aem.id)
            return

        async with set_correlation_id(correlation_id):
            if dirty_map:
                # Check if there are corresponding models remaining or if they have been removed from the config.

                # AEMPacks without matching models can be a result of configuration change and need to be deleted, as they've become unreachable.
                # Extant AEMPacks with existing models point to the AEMPack moving to a different subgraph with a different original ID.
                # In this case it also needs to be removed an recreated by separately iterating over its own ingress AEM
                models_by_name = {
                    model.name: model for model in self.graph_config.models
                }
                for model_name, aem_pack_id in dirty_map.items():
                    if models_by_name.get(model_name):
                        log.warning(
                            f"Derived AEMPack with id {aem_pack_id} is no longer reachable from its previous original ID. Removing."
                        )
                    else:
                        log.warning(
                            f"Model with name {model_name} no longer exists in the config, previously derived AEMPack with id {aem_pack_id} is no longer valid. Removing."
                        )
                    await self._aem_pack_dao.delete(aem_pack_id)

            for aem_pack in aem_packs_to_publish:
                log.info(f"Upserting derived AEMPack {aem_pack.id}")
                await self._aem_pack_dao.upsert(aem_pack)

        await self._incoming_aem_pack_queue.mark_processed(incoming_aem.id)

    def _traverse_graph(
        self,
        *,
        incoming: AEMPack,
        dirty_map: dict[str, UUID4],
        transformed_map: dict[str, AEMPack],
    ) -> tuple[list[AEMPack], dict[str, UUID4]]:
        """Traverse the transformation graph in topological order and apply workflows.

        Returns a tuple, where the first element contains a list of derived AEMPacks
        and the second element the remaining items from the dirty map that are now stale
        and need to be removed.
        """
        aem_packs_to_publish: list[AEMPack] = []
        models_by_name = {model.name: model for model in self.graph_config.models}
        model_order = {model.name: model.order for model in self.graph_config.models}
        workflows_by_name = {
            workflow.name: workflow for workflow in self.graph_config.workflows
        }
        routes_by_input: dict[str, list] = {}
        for route in self.graph_config.routes:
            routes_by_input.setdefault(route.input_model_name, []).append(route)

        if not models_by_name.get(incoming.model_name):
            model_lookup_error = ValueError(
                f"No model with name {incoming.model_name} registered for AEMPack with id {incoming.id}."
                + "This means a previously existing model vanished, which should not happen."
            )
            log.critical(model_lookup_error)
            raise model_lookup_error

        # Relies on dict insertion-order guarantee.
        # Avoid copying or re-sorting this dict, as that would break the traversal order.
        while transformed_map:
            current_model_name = next(iter(transformed_map))
            current_aem_pack = transformed_map[current_model_name]
            current_model = models_by_name[current_model_name]

            if current_model.publish:
                aem_packs_to_publish.append(current_aem_pack)

            current_routes = sorted(
                routes_by_input.get(current_model_name, []),
                key=lambda r: model_order[r.output_model_name],
            )
            for route in current_routes:
                transformed_data = self._apply_workflow_to_data(
                    data=current_aem_pack.data,
                    annotation=current_aem_pack.annotation,
                    input_schema=current_model.schema_,
                    workflow=workflows_by_name[route.workflow_name],
                )
                transformed_aem_pack = self._create_aem_pack(
                    aem_id=dirty_map.get(route.output_model_name),
                    data=transformed_data,
                    model_name=route.output_model_name,
                    pid=incoming.pid,
                    annotation=current_aem_pack.annotation,
                )
                transformed_map[route.output_model_name] = transformed_aem_pack

            # processed, no longer dirty or in queue
            dirty_map.pop(current_model_name, None)
            transformed_map.pop(current_model_name)

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
            handler: TransformationHandler = TransformationHandler(
                transformation_definition=transformation_def,
                transformation_config=typed_config,
                input_model=current_schema,
            )
            current_data = handler.transform_data(
                current_data, _AnnotationModel.model_validate(annotation)
            )
            current_schema = handler.transformed_model
        return current_data

    def _create_aem_pack(
        self,
        *,
        aem_id: UUID4 | None = None,
        model_name: str,
        pid: str,
        data: DataPack,
        annotation: dict[str, Any],
    ) -> AEMPack:
        """Wrap DataPack in an AEMPack."""
        return AEMPack(
            id=aem_id or uuid4(),
            model_name=model_name,
            pid=pid,
            data=data,
            annotation=annotation,
        )
