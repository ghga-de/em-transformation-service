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

"""Contains functionality for SchemaPack derivation for non-EMIM models."""

from metldata import get_transformation_registry
from metldata.transform.handling import TransformationHandler
from schemapack import is_equivalent_schemapack
from schemapack.spec.schemapack import SchemaPack

from ets.core.models import Model, Route, ValidatedConfig
from ets.ports.inbound.model_derivation import ModelDerivationError, ModelDeriverPort


class ModelDeriver(ModelDeriverPort):
    """Derives output schemas for all models in the transformation graph."""

    def __init__(self, *, config: ValidatedConfig):
        """Initialise the deriver.

        Args:
            config: Validated transformation configuration containing models,
                routes, and workflows.
        """
        self._models = config.models
        self._routes = config.routes
        self._transformation_registry = get_transformation_registry()
        self._workflows_by_name = {w.name: w for w in config.workflows}

    def _process_routes(self, *, schemas: dict[str, SchemaPack]) -> None:
        """Process each route in order, collecting derived output schemas."""
        topological_order: dict[str, int] = {
            model.name: model.order for model in self._models
        }
        routes_sorted = sorted(
            self._routes,
            key=lambda route: topological_order[route.input_model_name],
        )
        for route in routes_sorted:
            for model_name in (route.input_model_name, route.output_model_name):
                if model_name not in topological_order:
                    raise ModelDerivationError(
                        f"Model '{model_name}' referenced by route '{route.name}' "
                        "is not present in the topological order. "
                        "Ensure the route references a model that exists in the configuration."
                    )
            input_schema = schemas.get(route.input_model_name)
            if input_schema is None:
                raise ModelDerivationError(
                    f"Schema for input model '{route.input_model_name}' is not "
                    f"available when processing route '{route.name}'. "
                    "This indicates an error in the topological ordering."
                )
            derived_schema = self._apply_workflow(
                route=route, input_schema=input_schema
            )
            existing_schema = schemas.get(route.output_model_name)
            if existing_schema is not None and not is_equivalent_schemapack(
                existing_schema, derived_schema
            ):
                raise ModelDerivationError(
                    f"Model '{route.output_model_name}' for route '{route.name}' already "
                    "has a derived schema that differs from the newly derived one. "
                    "This indicates structural issues in the transformation configuration."
                )
            schemas[route.output_model_name] = derived_schema

    def _apply_workflow(self, *, route: Route, input_schema: SchemaPack) -> SchemaPack:
        """Apply every workflow step to the `input_schema` and return the derived schema."""
        workflow = self._workflows_by_name[route.workflow_name]

        current_schema = input_schema
        for step in workflow.workflow.operations:
            transformation_def = self._transformation_registry[step.name]
            try:
                typed_config = transformation_def.config_cls.model_validate(step.args)
                handler: TransformationHandler = TransformationHandler(
                    transformation_definition=transformation_def,
                    transformation_config=typed_config,
                    input_model=current_schema,
                )
                current_schema = handler.transformed_model
            except Exception as err:
                raise ModelDerivationError(
                    f"Schema derivation failed for route '{route.name}' "
                    f"at workflow step '{step.name}': {err}"
                ) from err

        return current_schema

    def _update_models(self, *, schemas: dict[str, SchemaPack]) -> list[Model]:
        """Build a list of `Model` objects and populate missing schemas."""
        models = []
        for raw_model in self._models:
            if raw_model.name not in schemas:
                raise ModelDerivationError(
                    f"Schema for model '{raw_model.name}' could not be derived. "
                    "It is neither an ingress model nor the output of any route."
                )
            models.append(
                Model(
                    name=raw_model.name,
                    description=raw_model.description,
                    is_ingress=raw_model.is_ingress,
                    version=raw_model.version,
                    publish=raw_model.publish,
                    order=raw_model.order,
                    schema_=schemas[raw_model.name],
                )
            )
        return models

    def derive_models(self) -> list[Model]:
        """Derive and return all models with populated schemas."""
        schemas: dict[str, SchemaPack] = {
            model.name: model.schema_  # type: ignore[misc]
            for model in self._models
            if model.is_ingress
        }
        self._process_routes(schemas=schemas)
        return self._update_models(schemas=schemas)
