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

from metldata import WorkflowRunner
from metldata.transform.exceptions import ModelAssumptionError, ModelTransformationError
from metldata.workflow.exceptions import WorkflowExecutionError
from schemapack import is_equivalent_schemapack
from schemapack.spec.schemapack import SchemaPack

from emts.core.models import Model, OrderedRawModel, Route, ValidatedConfig, Workflow


class ModelDerivationError(RuntimeError):
    """Raised when schema derivation fails for a route in the transformation graph."""


class ConsistencyError(RuntimeError):
    """Raised when an internal consistency check fails during model derivation.

    This indicates a programming error or a state that should have been caught
    by the config validator before reaching the derivation stage.
    """


def derive_models(config: ValidatedConfig) -> list[Model]:
    """Derive and return all models with populated schemas."""
    schemas: dict[str, SchemaPack] = {
        model.name: model.schema_  # type: ignore[misc]
        for model in config.models
        if model.is_ingress
    }
    _process_routes(config=config, schemas=schemas)
    return _update_models(raw_models=config.models, schemas=schemas)


def _process_routes(
    *,
    config: ValidatedConfig,
    schemas: dict[str, SchemaPack],
) -> None:
    """Process each route in order, collecting derived output schemas."""
    topological_order: dict[str, int] = {
        model.name: model.order for model in config.models
    }
    workflows_by_name = {w.name: w for w in config.workflows}

    for route in config.routes:
        for model_name in (route.input_model_name, route.output_model_name):
            if model_name not in topological_order:
                raise ConsistencyError(
                    f"Model '{model_name}' referenced by route '{route.name}' "
                    "is not present in the topological order. "
                    "This is an internal consistency error that should have been "
                    "caught by the config validator."
                )
    routes_sorted = sorted(
        config.routes,
        key=lambda route: topological_order[route.input_model_name],
    )
    for route in routes_sorted:
        input_schema = schemas.get(route.input_model_name)
        if input_schema is None:
            raise ModelDerivationError(
                f"Schema for input model '{route.input_model_name}' is not "
                f"available when processing route '{route.name}'. "
                "This indicates an error in the topological ordering."
            )
        derived_schema = _apply_workflow(
            route=route,
            workflow=workflows_by_name[route.workflow_name],
            input_schema=input_schema,
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


def _apply_workflow(
    *, route: Route, workflow: Workflow, input_schema: SchemaPack
) -> SchemaPack:
    """Apply the workflow to the `input_schema` and return the derived schema."""
    try:
        runner: WorkflowRunner = WorkflowRunner(
            workflow=workflow.workflow, input_model=input_schema
        )
    except WorkflowExecutionError as err:
        if isinstance(err.error, (ModelAssumptionError, ModelTransformationError)):
            raise ModelDerivationError(
                f"Schema derivation failed for route '{route.name}': {err}"
            ) from err
        raise err.error from err

    return runner.model


def _update_models(
    *, raw_models: list[OrderedRawModel], schemas: dict[str, SchemaPack]
) -> list[Model]:
    """Build a list of `Model` objects and populate missing schemas."""
    models = []
    for raw_model in raw_models:
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
