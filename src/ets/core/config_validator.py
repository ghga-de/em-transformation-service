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
"""Contains functionality to validate the loaded config."""

from metldata import get_transformation_registry, validate_workflow_against_registry
from metldata.workflow.exceptions import WorkflowValidationError

from ets.core.graph import CyclicGraphError, NonUniquePathError, get_topological_order
from ets.core.models import OrderedRawModel, RawConfig, ValidatedConfig


class ConfigValidationError(RuntimeError):
    """Raised when configuration validation fails."""


class ConfigValidator:
    """Concrete implementation of configuration validator."""

    def validate(self, raw_config: RawConfig) -> ValidatedConfig:
        """Validate new configuration loaded from yaml file.

        This should only be called when the loaded config does not match what has
        already been persisted previously.

        Args:
            raw_config: The new RawConfig to validate.

        Raises:
            ConfigValidationError: If any validation fails.
        """
        # models are already parsed into schemapacks for comparison and validated at that
        # point in time
        self._validate_routes(raw_config)
        self._validate_workflows(raw_config)
        self._validate_models(raw_config)
        return self._validate_graph_and_add_order(raw_config)

    def _validate_routes(self, raw_config: RawConfig) -> None:
        """Ensure all routes have valid references and model types.

        The following properties are validated:
        - Routes reference existing models and workflows
        - Output models for routes are NOT ingress models

        Args:
            raw_config: The new RawConfig containing routes and models/workflows.

        Raises:
            ConfigValidationError: If any route validation fails.
        """
        models_by_name = {model.name: model for model in raw_config.models}
        workflow_names = {workflow.name for workflow in raw_config.workflows}

        for route in raw_config.routes:
            # Verify input model exists
            if route.input_model_name not in models_by_name:
                raise ConfigValidationError(
                    f"Route '{route.name}' references non-existent input model "
                    f"'{route.input_model_name}'."
                )

            # Verify workflow exists
            if route.workflow_name not in workflow_names:
                raise ConfigValidationError(
                    f"Route '{route.name}' references non-existent workflow "
                    f"'{route.workflow_name}'."
                )

            # Verify output model exists and is NOT ingress
            if route.output_model_name not in models_by_name:
                raise ConfigValidationError(
                    f"Route '{route.name}' references non-existent output model "
                    f"'{route.output_model_name}'."
                )

            output_model = models_by_name[route.output_model_name]
            if output_model.is_ingress:
                raise ConfigValidationError(
                    f"Route '{route.name}' output model '{output_model.name}' "
                    f"must not be an ingress model (is_ingress must be False)."
                )

    def _validate_workflows(self, raw_config: RawConfig) -> None:
        """Validate all workflows using the metldata library.

        Workflows have already been parsed into their internal representation, and
        should be structurally and syntactically valid at this point.

        It validates that the workflows reference existing transformation definitions
        and the arguments match the definition.

        Validating that the workflows are executable is not possible at this point,
        as not all model schemas_ are available yet for input.

        Args:
            raw_config: The new RawConfig containing workflows.

        Raises:
            ConfigValidationError: If any workflow validation fails.
        """
        transformation_registry = get_transformation_registry()
        for workflow in raw_config.workflows:
            try:
                validate_workflow_against_registry(
                    workflow=workflow.workflow,
                    transformation_registry=transformation_registry,
                )
            except WorkflowValidationError as error:
                raise ConfigValidationError(str(error)) from error

    def _validate_models(self, raw_config: RawConfig) -> None:
        """Validate all models.

        Schema validation (`schema_`) against the SchemaPack specification is
        handled by the transformation config file loader when serializing models
        into SchemaPacks. Any issues with the schema should be caught at that stage.

        This validator specifically ensures that if a model is marked as an ingress
        model (`is_ingress=True`), it must have a `schema_` defined.

        Args:
            raw_config: The new RawConfig containing models.

        Raises:
            ConfigValidationError: If any model fails validation.
        """
        for model in raw_config.models:
            if model.is_ingress and model.schema_ is None:
                raise ConfigValidationError(
                    f"Model '{model.name}' is marked as ingress but does not have a schema defined. "
                    f"Ingress models must have a schema defined."
                )
            if not model.is_ingress and model.schema_:
                raise ConfigValidationError(
                    f"Model '{model.name}' is not marked as ingress but has a schema defined."
                    " Non-ingress models must not define a schema, as it is derived from"
                    " ingress models after loading."
                )

    def _validate_graph_and_add_order(self, raw_config: RawConfig) -> ValidatedConfig:
        """Validate the transformation graph defined by the routes and assign a topological
         order to the models.

         This method performs the following steps:
         1. Validates that the directed graph formed by the models and routes is acyclic
         and had unique path properties.
         2. Computes a topological ordering of the models.
         3. Returns a new `ValidatedConfig` object where each model is wrapped as an
        `OrderedRawModel` with the corresponding `order` assigned.

        Args:
            raw_config: The new RawConfig containing models.

        Raises:
            ConfigValidationError: If the graph fails validation.
        """
        topological_order = self._validate_graph_and_calculate_order(raw_config)

        # For any model, that is not referenced by any route, assign an order of 0
        ordered_models = [
            OrderedRawModel(
                **model.model_dump(),
                order=topological_order.get(model.name, -1),
            )
            for model in raw_config.models
        ]
        return ValidatedConfig(
            models=ordered_models,
            workflows=raw_config.workflows,
            routes=raw_config.routes,
        )

    def _validate_graph_and_calculate_order(
        self, raw_config: RawConfig
    ) -> dict[str, int]:
        """Validate that the graph defined by the routes meets the unique path
        requirement and it is a directed acyclic graph (DAG).

        After a successful validation, the routes are updated with the topological order.
        """
        # get edges from the routes
        edges = [
            (route.input_model_name, route.output_model_name)
            for route in raw_config.routes
        ]

        # calculate the topological order which also validates the graph structure
        try:
            return get_topological_order(edges)
        except (CyclicGraphError, NonUniquePathError) as error:
            raise ConfigValidationError(str(error)) from error
