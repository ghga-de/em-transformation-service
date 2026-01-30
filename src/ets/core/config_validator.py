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

from metldata.builtin_transformations.delete_class.main import (
    DELETE_CLASS_TRANSFORMATION,
)
from metldata.builtin_transformations.duplicate_class.main import (
    DUPLICATE_CLASS_TRANSFORMATION,
)
from metldata.builtin_transformations.infer_relation.main import (
    INFER_RELATION_TRANSFORMATION,
)
from metldata.builtin_transformations.merge_relations.main import (
    MERGE_RELATIONS_TRANSFORMATION,
)
from metldata.builtin_transformations.rename_id_property.main import (
    RENAME_ID_PROPERTY_TRANSFORMATION,
)
from metldata.builtin_transformations.replace_resource_ids.main import (
    REPLACE_RESOURCE_IDS_TRANSFORMATION,
)
from metldata.builtin_transformations.transform_content.main import (
    TRANSFORM_CONTENT_TRANSFORMATION,
)
from metldata.transform.base import TransformationDefinition
from schemapack.spec.schemapack import SchemaPack

from ets.core.models import ComparisonResultChanged, RawModel

# We should expose this at the metldata level for consumption instead
# Duplicated for now to make the tests work
TRANSFORMATION_REGISTRY: dict[str, TransformationDefinition] = {
    "delete_class": DELETE_CLASS_TRANSFORMATION,
    "duplicate_class": DUPLICATE_CLASS_TRANSFORMATION,
    "infer_relation": INFER_RELATION_TRANSFORMATION,
    "merge_relations": MERGE_RELATIONS_TRANSFORMATION,
    "transform_content": TRANSFORM_CONTENT_TRANSFORMATION,
    "rename_id_property": RENAME_ID_PROPERTY_TRANSFORMATION,
    "replace_resource_ids": REPLACE_RESOURCE_IDS_TRANSFORMATION,
}


class ConfigValidationError(RuntimeError):
    """Raised when configuration validation fails."""


class ConfigValidatorPort(ABC):
    """Abstract base class for configuration validation."""

    @abstractmethod
    def validate(self, result: ComparisonResultChanged) -> None:
        """Validate new configuration loaded from yaml file.

        This should only be called when the loaded config does not match what has
        already been persisted previously.

        Args:
            result: ComparisonResultChanged containing models, routes, and workflows.

        Raises:
            ConfigValidationError: If any validation fails.
        """


class ConfigValidator(ConfigValidatorPort):
    """Concrete implementation of configuration validator."""

    def validate(self, result: ComparisonResultChanged) -> None:
        """Validate configuration from comparison result.

        This should only be called when the loaded config does not match what has
        already been persisted previously.

        Args:
            result: ComparisonResultChanged containing models, routes, and workflows.

        Raises:
            ConfigValidationError: If any validation fails.
        """
        try:
            self._validate_routes(result)
            self._validate_model_schemas(result.models)
            self._validate_workflows(result)
        except Exception as err:
            raise ConfigValidationError(f"Unexpected validation error: {err}") from err

    def _validate_routes(self, result: ComparisonResultChanged) -> None:
        """Ensure all routes have valid references and model types.

        The following properties are validated:
        - Routes reference existing models and workflows
        - Output models for routes are NOT ingress models

        Args:
            result: ComparisonResultChanged containing routes and models/workflows.

        Raises:
            ConfigValidationError: If any route validation fails.
        """
        models_by_name = {model.name: model for model in result.models}
        workflow_names = {workflow.name for workflow in result.workflows}

        for route in result.routes:
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

    def _validate_model_schemas(self, models: list[RawModel]) -> None:
        """Validate all model schemas using SchemaPack library.

        Args:
            models: List of RawModel objects to validate.

        Raises:
            ConfigValidationError: If any schema validation fails.
        """
        for model in models:
            if model.schema_ is not None:
                try:
                    # Could return the validated model here and replace the serialized version,
                    # but that would need another intermediate BaseModel with nullable schema_
                    SchemaPack.model_validate(model.schema_)
                except Exception as err:
                    raise ConfigValidationError(
                        f"Invalid schema for model '{model.name}': {err}"
                    ) from err

    def _validate_workflows(self, result: ComparisonResultChanged) -> None:
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
        for workflow in result.workflows:
            for operation in workflow.workflow.operations:
                if not operation.name in TRANSFORMATION_REGISTRY:
                    raise ConfigValidationError(
                        f"Unknown transformation '{operation.name}' in workflow '{workflow.name}'."
                    )

                transformation_definition = TRANSFORMATION_REGISTRY[operation.name]
                provided_config = operation.args
                expected_config_clas = transformation_definition.config_cls
                if not isinstance(provided_config, expected_config_clas):
                    raise ConfigValidationError(
                        f"Invalid transformation config for workflow '{workflow.name}': '{operation.name}' got config class of type '{type(provided_config)}', but should be '{expected_config_clas}'."
                    )
