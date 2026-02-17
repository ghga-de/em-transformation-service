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

from ets.core.models import ComparisonResultChanged
from ets.ports.inbound.config_validator import (
    ConfigValidationError,
    ConfigValidatorPort,
)


class ConfigValidator(ConfigValidatorPort):
    """Concrete implementation of configuration validator."""

    def validate(self, changed_config: ComparisonResultChanged) -> None:
        """Validate new configuration loaded from yaml file.

        This should only be called when the loaded config does not match what has
        already been persisted previously.

        Args:
            result: ComparisonResultChanged containing models, routes, and workflows.

        Raises:
            ConfigValidationError: If any validation fails.
        """
        # models are already parsed into schemapacks for comparison and validated at that
        # point in time
        self._validate_routes(changed_config)
        self._validate_workflows(changed_config)

    def _validate_routes(self, changed_config: ComparisonResultChanged) -> None:
        """Ensure all routes have valid references and model types.

        The following properties are validated:
        - Routes reference existing models and workflows
        - Output models for routes are NOT ingress models

        Args:
            result: ComparisonResultChanged containing routes and models/workflows.

        Raises:
            ConfigValidationError: If any route validation fails.
        """
        models_by_name = {model.name: model for model in changed_config.models}
        workflow_names = {workflow.name for workflow in changed_config.workflows}

        for route in changed_config.routes:
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

    def _validate_workflows(self, changed_config: ComparisonResultChanged) -> None:
        """Validate all workflows using the metldata library.

        In contrast to schemas, which are still serialized at this point, Workflows
        have already been parsed into their internal representation and should be
        structurally and syntactically valid at this point.

        What needs to be validated here is that the workflows reference existing
        transformation definitions and the arguments match the definition.

        Validating that the workflows are executable is not possible at this point,
        as not all model schemas_ are available yet for input.

        Args:
            result: ComparisonResultChanged containing workflows.

        Raises:
            ConfigValidationError: If any workflow validation fails.
        """
        transformation_registry = get_transformation_registry()
        for workflow in changed_config.workflows:
            try:
                validate_workflow_against_registry(
                    workflow=workflow.workflow,
                    transformation_registry=transformation_registry,
                )
            except Exception as error:
                raise ConfigValidationError(str(error)) from error
