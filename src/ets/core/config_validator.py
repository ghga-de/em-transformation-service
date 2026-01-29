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
"""Contains functionality to validate the loaded config."""

from abc import ABC, abstractmethod

from metldata.workflow.base import Workflow as MetldataWorkflow
from schemapack.spec.schemapack import SchemaPack

from ets.core.models import ComparisonResultChanged, RawModel


class ConfigValidationError(RuntimeError):
    """Raised when configuration validation fails."""


class ConfigValidatorPort(ABC):
    """Abstract base class for configuration validation."""

    @abstractmethod
    def validate(self, result: ComparisonResultChanged) -> None:
        """Validate configuration from comparison result.

        Args:
            result: ComparisonResultChanged containing models, routes, and workflows.

        Raises:
            ConfigValidationError: If any validation fails.
        """


class ConfigValidator(ConfigValidatorPort):
    """Concrete implementation of configuration validator."""

    def validate(self, result: ComparisonResultChanged) -> None:
        """Validate configuration from comparison result.

        Performs the following validations:
        - Routes reference existing models and workflows
        - Input models for routes are ingress models
        - Output models for routes are NOT ingress models
        - Model schemas are valid SchemaPack definitions
        - Workflows are valid metldata workflows

        Args:
            result: ComparisonResultChanged containing models, routes, and workflows.

        Raises:
            ConfigValidationError: If any validation fails.
        """
        try:
            self._validate_routes(result)
            self._validate_model_schemas(result)
            self._validate_workflows(result)
        except Exception as err:
            raise ConfigValidationError(f"Unexpected validation error: {err}") from err

    def _validate_routes(self, result: ComparisonResultChanged) -> None:
        """Validate all routes have valid references and model types.

        Args:
            result: ComparisonResultChanged containing routes and models/workflows.

        Raises:
            ConfigValidationError: If any route validation fails.
        """
        # Build lookup indices
        models_by_name = {model.name: model for model in result.models}
        workflows_by_name = {workflow.name: workflow for workflow in result.workflows}

        for route in result.routes:
            # Verify input model exists and is ingress
            if route.input_model_name not in models_by_name:
                raise ConfigValidationError(
                    f"Route '{route.name}' references non-existent input model "
                    f"'{route.input_model_name}'."
                )

            input_model = models_by_name[route.input_model_name]
            if not input_model.is_ingress:
                raise ConfigValidationError(
                    f"Route '{route.name}' input model '{route.input_model_name}' "
                    f"must be an ingress model (is_ingress=True)."
                )

            # Verify workflow exists
            if route.workflow_name not in workflows_by_name:
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
                    f"Route '{route.name}' output model '{route.output_model_name}' "
                    f"must not be an ingress model (is_ingress must be False)."
                )

    def _validate_model_schemas(self, models: list[RawModel]) -> None:
        """Validate all model schemas using SchemaPack library.

        Args:
            result: ComparisonResultChanged containing models.

        Raises:
            ConfigValidationError: If any schema validation fails.
        """
        for model in models:
            if model.schema_ is not None:
                try:
                    SchemaPack.model_validate(model.schema_)
                except Exception as err:
                    raise ConfigValidationError(
                        f"Invalid schema for model '{model.name}': {err}"
                    ) from err

    def _validate_workflows(self, result: ComparisonResultChanged) -> None:
        """Validate all workflows using metldata library.

        Args:
            result: ComparisonResultChanged containing workflows.

        Raises:
            ConfigValidationError: If any workflow validation fails.
        """
        for workflow in result.workflows:
            try:
                # Validate workflow structure using metldata
                if not isinstance(workflow.workflow, MetldataWorkflow):
                    raise ConfigValidationError(
                        f"Workflow '{workflow.name}' is not a valid MetldataWorkflow."
                    )

                # Verify operations exist
                if not hasattr(workflow.workflow, "operations"):
                    raise ConfigValidationError(
                        f"Workflow '{workflow.name}' has no operations."
                    )

                if not workflow.workflow.operations:
                    # Empty operations is allowed but warn
                    pass

                # Validate each operation
                for operation in workflow.workflow.operations:
                    if not hasattr(operation, "name"):
                        raise ConfigValidationError(
                            f"Workflow '{workflow.name}' has operation without name."
                        )
                    if not hasattr(operation, "description"):
                        raise ConfigValidationError(
                            f"Workflow '{workflow.name}' operation '{operation.name}' "
                            f"has no description."
                        )
                    if not hasattr(operation, "args"):
                        raise ConfigValidationError(
                            f"Workflow '{workflow.name}' operation '{operation.name}' "
                            f"has no args."
                        )

            except ConfigValidationError:
                raise
            except Exception as err:
                raise ConfigValidationError(
                    f"Invalid workflow '{workflow.name}': {err}"
                ) from err