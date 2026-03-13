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

import logging
from uuid import uuid4

from metldata import get_transformation_registry
from metldata.transform.handling import TransformationHandler
from pydantic import UUID4
from schemapack.spec.datapack import DataPack
from schemapack.spec.schemapack import SchemaPack

from ets.adapters.inbound.event_schemas import AEMPack
from ets.core.models import Model, PersistedConfig, Route, Workflow
from ets.ports.inbound.aem_pack_registry import (
    AEMPackRegistryPort,
)
from ets.ports.outbound.config_loader import ConfigLoaderPort
from ets.ports.outbound.dao import AEMPackDao, ResourceNotFoundError

log = logging.getLogger(__name__)


class AEMPackRegistry(AEMPackRegistryPort):
    """Core service for managing AEMPack transformations."""

    def __init__(self, *, aem_pack_dao: AEMPackDao, config_loader: ConfigLoaderPort):
        self._aem_pack_dao = aem_pack_dao
        self._config_loader = config_loader
        self._transformation_registry = get_transformation_registry()

    async def _build_dirty_map(self, original_id: str) -> dict[str, UUID4]:
        """Build a mapping from model names to AEMPack IDs for all existing
        derived data that share the given original ID.

        This tracks data that may need to be re-created or deleted during
        transformation.
        """
        dirty_map: dict[str, UUID4] = {}
        async for aem_pack in self._aem_pack_dao.find_all(
            mapping={"original_id": original_id}
        ):
            dirty_map[aem_pack.model_name] = aem_pack.id
        return dirty_map

    def _build_transformed_map(self, original: AEMPack) -> dict[str, AEMPack]:
        """Initialize the transformed map with the incoming original AEMPack.

        The map accumulates all data generated during the transformation,
        keyed by model name.
        """
        return {original.model_name: original}

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

    def _traverse_graph(
        self,
        *,
        original: AEMPack,
        dirty_map: dict[str, UUID4],
        transformed_map: dict[str, AEMPack],
        config: PersistedConfig,
    ) -> None:
        """Traverse the transformation graph in topological order, applying
        workflows to produce transformed AEMPacks.

        Mutates dirty_map (removing re-created entries) and transformed_map
        (adding newly produced entries) in place.
        """
        routes_by_input: dict[str, Route] = {
            r.input_model_name: r for r in config.routes
        }
        workflows_by_name: dict[str, Workflow] = {w.name: w for w in config.workflows}
        schemas_by_model: dict[str, SchemaPack] = {
            m.name: m.schema_ for m in config.models
        }
        models_sorted: list[Model] = sorted(config.models, key=lambda m: m.order)

        for model in models_sorted:
            if model.name not in routes_by_input:
                continue

            route = routes_by_input[model.name]
            input_aem_pack = transformed_map.get(route.input_model_name)
            if input_aem_pack is None:
                raise RuntimeError(
                    f"Input data for model '{route.input_model_name}' not found in"
                    f" transformed map when processing route '{route.name}'."
                    " This indicates an error in the topological ordering."
                )

            input_schema = schemas_by_model[route.input_model_name]
            workflow = workflows_by_name[route.workflow_name]
            transformed_data = self._apply_workflow_to_data(
                data=input_aem_pack.data,
                annotation=input_aem_pack.annotation,
                input_schema=input_schema,
                workflow=workflow,
            )

            output_model_name = route.output_model_name
            original_id = str(original.id)

            if output_model_name in dirty_map:
                existing_id = dirty_map.pop(output_model_name)
                transformed_map[output_model_name] = AEMPack(
                    id=existing_id,
                    model_name=output_model_name,
                    original_id=original_id,
                    data=transformed_data,
                    annotation=original.annotation,
                )
            else:
                transformed_map[output_model_name] = AEMPack(
                    id=uuid4(),
                    model_name=output_model_name,
                    original_id=original_id,
                    data=transformed_data,
                    annotation=original.annotation,
                )

    async def transform_aem_pack(
        self, original: AEMPack
    ) -> tuple[dict[str, AEMPack], dict[str, UUID4]]:
        """Run steps 1-3 of the transformation user journey.

        Returns:
            A tuple of (transformed_map, dirty_map) for downstream persistence
            in step 4.
        """
        config = await self._config_loader.load_config_from_db()
        dirty_map = await self._build_dirty_map(str(original.id))
        transformed_map = self._build_transformed_map(original)
        self._traverse_graph(
            original=original,
            dirty_map=dirty_map,
            transformed_map=transformed_map,
            config=config,
        )
        return transformed_map, dirty_map

    async def upsert_aem_pack(self, aem_pack: AEMPack) -> None:
        """Upsert AEM Pack. Inserts a new AEMPack or updates an existing one.

        Args:
            aem_pack (AEMPack): The AEMPack to process.
        Raises:
            ModelNotFoundError: If the model referenced doesn't exist in configuration.
            DataPackValidationError: If the data doesn't conform to the model schema.
            UpsertionError: If the database operation fails.
        """
        try:
            await self._aem_pack_dao.get_by_id(aem_pack.id)
            log.debug(
                "Found an AEMPack with id '%s', updating entry.",
                aem_pack.id,
            )
        except ResourceNotFoundError:
            log.debug(
                "No existing AEMPack found with id '%s', creating new entry.",
                aem_pack.id,
            )

        await self._aem_pack_dao.upsert(aem_pack)

        log.debug("AEMPack upserted with id '%s'.", aem_pack.id)
