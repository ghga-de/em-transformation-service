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
import time
from typing import Any
from uuid import uuid4

from hexkit.correlation import set_correlation_id
from metldata import WorkflowRunner
from metldata.workflow.exceptions import WorkflowExecutionError
from pydantic import UUID4, BaseModel, ConfigDict
from schemapack import SchemaPackValidator
from schemapack.exceptions import ValidationError
from schemapack.spec.datapack import DataPack
from schemapack.spec.schemapack import SchemaPack

from emts.config import Config
from emts.core.config_updater import ConfigUpdater
from emts.core.models import (
    AEMPack,
    AEMPackStatus,
    AEMPackStatusEvent,
    IncomingAEMPack,
    PersistedConfig,
    VersionedAEMPack,
    Workflow,
)
from emts.ports.inbound.aem_pack_registry import (
    AEMPackRegistryPort,
    DataDerivationError,
)
from emts.ports.outbound.config_lock import ConfigLockPort
from emts.ports.outbound.dao import AEMPackDao, StatusEventDao
from emts.ports.outbound.incoming_aem_pack_queue import IncomingAEMPackQueuePort

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
        status_event_dao: StatusEventDao,
        config_updater: ConfigUpdater,
        config_lock: ConfigLockPort,
        incoming_aem_pack_queue: IncomingAEMPackQueuePort,
    ):
        self._config = config
        self._aem_pack_dao = aem_pack_dao
        self._status_event_dao = status_event_dao
        self._config_updater = config_updater
        self._config_lock = config_lock
        self._incoming_aem_pack_queue = incoming_aem_pack_queue

    async def queue_unprocessed(self, aem_pack: VersionedAEMPack):
        """Fetch new AEMPacks via event subscriber and put them into the queue for processing."""
        await self._config_lock.wait_for_lock_release()
        # load the most recent config
        await self._config_updater.update_config()
        config = self._config_updater.current_config

        matching_model = next(
            (m for m in config.models if m.name == aem_pack.model_name), None
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

        stored = await self._incoming_aem_pack_queue.queue(aem_pack)
        if stored:
            # Only emitted once the pack is actually accepted (a strictly newer
            # version); rejected republishes do not produce a status event.
            await self._status_event_dao.upsert(
                AEMPackStatusEvent(
                    pid=aem_pack.pid,
                    model_name=aem_pack.model_name,
                    version=aem_pack.version,
                    status=AEMPackStatus.QUEUED,
                )
            )

    async def process_aem_packs(self) -> None:
        """Derives AEMPacks from incoming AEMPacks."""
        while True:
            await self._config_lock.wait_for_lock_release()

            claimed = await self._incoming_aem_pack_queue.claim_next()
            if claimed:
                start = time.monotonic()
                await self._process_next_aem_pack(
                    incoming_aem=claimed, correlation_id=claimed.correlation_id
                )
                log.info(
                    "Finished handling AEMPack '%s' in %.1fs (claim TTL is %ds).",
                    claimed.id,
                    time.monotonic() - start,
                    self._config.claim_ttl_seconds,
                )
            else:
                log.info(
                    "No new AEM found, sleeping for %d seconds.",
                    self._config.processing_poll_pause,
                )
                await asyncio.sleep(self._config.processing_poll_pause)

    async def _process_next_aem_pack(
        self, *, incoming_aem: IncomingAEMPack, correlation_id: UUID4
    ):
        """Perform transformation on the whole subgraph matching the incoming AEMPack ingress model."""
        dirty_map = {
            aem_pack.model_name: aem_pack.id
            async for aem_pack in self._aem_pack_dao.find_all(
                mapping={"pid": incoming_aem.pid}
            )
        }
        transformed_map: dict[str, AEMPack] = {incoming_aem.model_name: incoming_aem}

        await self._config_lock.wait_for_lock_release()
        await self._config_updater.update_config()
        config = self._config_updater.current_config
        version_before = self._config_updater.known_version

        try:
            aem_packs_to_publish, dirty_map = self._traverse_graph(
                incoming=incoming_aem,
                dirty_map=dirty_map,
                transformed_map=transformed_map,
                config=config,
            )
        except DataDerivationError as error:
            # Deterministic transformation failure: publish a failure event, mark the
            # AEMPack as failed, and abort without propagating any (partial) derived
            # results. The loop continues with the next AEMPack instead of crashing.
            transformation_step = error.transformation_step
            error_type = type(error.error).__name__
            error_message = str(error.error)
            log.error(
                "Data derivation failed for AEMPack '%s'. Marking as failed and aborting.",
                incoming_aem.id,
                extra={
                    "pid": error.pid,
                    "model_name": error.model_name,
                    "transformation_step": transformation_step,
                    "error_type": error_type,
                    "error_message": error_message,
                },
            )
            # Publish before updating queue state: a crash after publishing only causes
            # a reprocess that republishes (at-least-once), never a lost failure event.
            async with set_correlation_id(correlation_id):
                await self._status_event_dao.upsert(
                    AEMPackStatusEvent(
                        pid=error.pid,
                        model_name=error.model_name,
                        version=incoming_aem.version,
                        status=AEMPackStatus.FAILED,
                        transformation_step=transformation_step,
                        error_type=error_type,
                        error_message=error_message,
                    )
                )
            await self._incoming_aem_pack_queue.mark_as_failed(
                incoming_aem.id, incoming_aem.version
            )
            return

        await self._config_lock.wait_for_lock_release()
        await self._config_updater.update_config()
        version_after = self._config_updater.known_version

        if version_after != version_before:
            log.info(
                "Graph config changed while processing AEMPack '%s'.\n"
                + "Discarding changes and freeing for reprocessing with new config",
                incoming_aem.id,
            )
            await self._incoming_aem_pack_queue.free(
                incoming_aem.id, incoming_aem.version
            )
            return

        # Early abort for concurrent processors. This is best-effort only.
        # Different workers can enter the subsequent block as long as the final state
        # hasn't been committed.
        if await self._incoming_aem_pack_queue.is_superseded_or_processed(
            incoming_aem.id, incoming_aem.version
        ):
            log.info(
                "AEMPack '%s' (version %d) was superseded by a newer version or already"
                " processed by another instance; discarding stale derived results.",
                incoming_aem.id,
                incoming_aem.version,
            )
            return

        aem_packs_to_publish = await self._prune_derived_aem_packs_on_delete(
            incoming_aem_id=incoming_aem.id,
            pid=incoming_aem.pid,
            aem_packs_to_publish=aem_packs_to_publish,
            dirty_map=dirty_map,
        )

        async with set_correlation_id(correlation_id):
            if dirty_map:
                # Check if there are corresponding models remaining or if they have been removed from the config.

                # AEMPacks without matching models can be a result of configuration change and need to be deleted, as they've become unreachable.
                # Extant AEMPacks with existing models point to the AEMPack moving to a different subgraph with a different original ID.
                # In this case it also needs to be removed an recreated by separately iterating over its own ingress AEM
                models_by_name = {model.name: model for model in config.models}
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
                log.info("Upserting derived AEMPack %s.", aem_pack.id)
                await self._aem_pack_dao.upsert(aem_pack)

            # Published before marking processed: a crash after publishing only causes
            # a reprocess that republishes (at-least-once), never a lost status event.
            await self._status_event_dao.upsert(
                AEMPackStatusEvent(
                    pid=incoming_aem.pid,
                    model_name=incoming_aem.model_name,
                    version=incoming_aem.version,
                    status=AEMPackStatus.PROCESSED,
                )
            )

        await self._incoming_aem_pack_queue.mark_processed(
            incoming_aem.id, incoming_aem.version
        )

    async def _prune_derived_aem_packs_on_delete(
        self,
        incoming_aem_id: UUID4,
        pid: str,
        aem_packs_to_publish: list[AEMPack],
        dirty_map: dict[str, UUID4],
    ) -> list[AEMPack]:
        """Prune derived AEMPacks when the original AEMPack is marked for deletion.
        This handles the case where an original AEMPack is marked for deletion after
        it was claimed for processing but before the processing is completed.
        Mutates dirty_map in place to include all existing derived AEMPacks for deletion.
        """
        if not aem_packs_to_publish:
            return aem_packs_to_publish

        if not await self._incoming_aem_pack_queue.is_marked_or_deleted(
            incoming_aem_id
        ):
            return aem_packs_to_publish

        log.warning(
            f"Original AEMPack with id {incoming_aem_id} is marked for deletion. Pruning derived AEMPacks."
        )

        # Mark only packs that already exist in the DAO for deletion.
        # Newly-derived packs (never upserted) are simply dropped.
        dirty_map.update(
            {
                pack.model_name: pack.id
                async for pack in self._aem_pack_dao.find_all(mapping={"pid": pid})
            }
        )

        return []

    def _traverse_graph(
        self,
        *,
        incoming: AEMPack,
        dirty_map: dict[str, UUID4],
        transformed_map: dict[str, AEMPack],
        config: PersistedConfig,
    ) -> tuple[list[AEMPack], dict[str, UUID4]]:
        """Traverse the transformation graph in topological order and apply workflows.

        Returns a tuple, where the first element contains a list of derived AEMPacks
        and the second element the remaining items from the dirty map that are now stale
        and need to be removed.
        """
        aem_packs_to_publish: list[AEMPack] = []
        models_by_name = {model.name: model for model in config.models}
        model_order = {model.name: model.order for model in config.models}
        workflows_by_name = {workflow.name: workflow for workflow in config.workflows}
        routes_by_input: dict[str, list] = {}
        for route in config.routes:
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
                    aem_pack=current_aem_pack,
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
        aem_pack: AEMPack,
        input_schema: SchemaPack,
        workflow: Workflow,
    ) -> DataPack:
        """Apply the workflow to the AEMPack's DataPack and return the result.

        Raises:
            DataDerivationError: if any workflow data step fails.
        """
        runner: WorkflowRunner = WorkflowRunner(
            workflow=workflow.workflow, input_model=input_schema
        )

        try:
            return runner.run_workflow(
                data=aem_pack.data,
                annotation=_AnnotationModel.model_validate(aem_pack.annotation),
            )
        except WorkflowExecutionError as error:
            raise DataDerivationError(
                pid=aem_pack.pid,
                model_name=aem_pack.model_name,
                error=error,
                # step name is only exposed as a private attribute by metldata; read
                # it here at the single wrap boundary rather than at every catch site.
                transformation_step=getattr(error, "_step_name", None),
            ) from error

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

    async def delete_aem_pack_and_descendants(self, incoming_aem_id: UUID4):
        """Delete an incoming AEMPack and all AEMPacks derived from it.

        First marks the AEMPack for deletion to prevent it from being claimed for processing.
        This also signals to any ongoing processing that derived AEMPacks should not be published.
        Then hard-deletes the incoming AEMPack along with all its descendants, if any.
        """
        # clean up the queue
        await self._soft_delete_aem_pack(incoming_aem_id)
        await self._hard_delete_aem_pack(incoming_aem_id)

        # clean up the aem_packs derived from the deleted one
        async for aem_pack in self._aem_pack_dao.find_all(
            mapping={"pid": str(incoming_aem_id)}
        ):
            await self._aem_pack_dao.delete(aem_pack.id)

    async def _soft_delete_aem_pack(self, incoming_aem_id: UUID4):
        """Mark an AEMPack for deletion."""
        await self._incoming_aem_pack_queue.mark_for_deletion(incoming_aem_id)

    async def _hard_delete_aem_pack(self, incoming_aem_id: UUID4):
        """Delete an AEMPack from the queue."""
        await self._incoming_aem_pack_queue.delete_marked(incoming_aem_id)
