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

"""Defines dataclasses for holding business-logic data."""

from collections.abc import Mapping
from typing import Any, Self

from metldata.workflow.base import Workflow as MetldataWorkflow
from pydantic import (
    BaseModel,
    Field,
    model_validator,
)
from schemapack.spec.schemapack import SchemaPack


class ModelBase(BaseModel):
    """Base for different model variants (old, new, serialized)"""

    name: str = Field(
        default=..., description="A Unique human-readable name of the model."
    )
    description: str | None = Field(
        default=None, description="A human-readable description of the model."
    )
    is_ingress: bool = Field(
        default=...,
        description="Whether this model is an experimental metadata ingress model (EMIM).",
    )
    version: str | None = Field(
        default=...,
        description="The version of the model. None if the model is not an EMIM.",
    )
    publish: bool = Field(
        default=...,
        description="whether the data conforming to the schema should be published.",
    )


class RawModel(ModelBase):
    """Describes a raw model before any processing."""

    schema_: Mapping[str, Any] | None = Field(
        default=..., description="Schema associated with the model."
    )


class InternalModel(ModelBase):
    """Describes a variant of RawModel with schemas instantiated as SchemaPacks where applicable."""

    schema_: SchemaPack | None = Field(
        default=..., description="Schema associated with the model."
    )


class Model(ModelBase):
    """Describes a model after resolving the topological ordering and deriving the schemas."""

    schema_: SchemaPack = Field(
        default=..., description="Schema associated with the model."
    )
    order: int = Field(
        default=...,
        description="Topological order of the schema in the transformation graph.",
    )


class PersistedModel(ModelBase):
    """Variant of 'Model' with serialized schema_ for use as DTO in storage and events."""

    schema_: Mapping[str, Any] = Field(
        default=...,
        description="Serialized representation of a schema associated with the model.",
    )
    order: int = Field(
        default=...,
        description="Topological order of the schema in the transformation graph.",
    )


class Workflow(BaseModel):
    """Describes a metldata compatible workflow definition."""

    name: str = Field(
        default=...,
        description="A unique human-readable name of the workflow indicating the purpose of the workflow.",
    )
    description: str | None = Field(
        default=None, description="A human-readable description of the workflow."
    )
    workflow: MetldataWorkflow = Field(
        default=..., description="Workflow definition in metldata Workflow format."
    )


class RawRoute(BaseModel):
    """Describes the routes for transforming models and data by referencing the
    workflow, the input and output models involved in each transformation by name.
    """

    name: str | None = Field(
        default=None,
        description=(
            "A unique human-readable name of the route. Follows the format of "
            "'input_model_name:workflow_name:output_model_name'."
        ),
    )
    input_model_name: str | None = Field(
        default=None, description=" Name of the input model accepted by the route."
    )
    output_model_name: str | None = Field(
        default=None, description="Name of the output model produced by the route."
    )
    workflow_name: str | None = Field(
        default=None,
        description="Name of the workflow used to transform the input model to the output model.",
    )

    @model_validator(mode="after")
    def ensure_name_consistency(self) -> Self:
        """Ensures that the route name is consistent with the rest of the attributes.

        - If only 'name' provided, decomposes it.
        - If all of 'input_model_name', 'output_model_name', 'workflow_name' are provided,
        composes the 'name'.
        - Otherwise, raises a ValueError.
        """
        name_parts = [self.input_model_name, self.workflow_name, self.output_model_name]

        has_all_parts = all(part is not None for part in name_parts)
        has_no_parts = all(part is None for part in name_parts)

        if self.name and has_no_parts:
            parts = self.name.split(":")
            if len(parts) != 3:
                raise ValueError(
                    "'name' should be formatted as 'input_model_name:workflow_name:output_model_name'"
                )
            self.input_model_name, self.workflow_name, self.output_model_name = parts
            return self

        if has_all_parts:
            name = (
                f"{self.input_model_name}:{self.workflow_name}:{self.output_model_name}"
            )
            if not self.name:
                self.name = name
            # needed case to pass revalidation, i.e. when construction another
            # BaseModel containing this one as part of its attributes
            if self.name != name:
                raise ValueError(
                    f"Provided name '{self.name}' and name assembled from parts '{name}' do not match."
                )
            return self

        raise ValueError(
            "Either 'name' or all of 'input_model_name', 'workflow_name', 'output_model_name' need to be provided."
        )


class Route(BaseModel):
    """Route model after validation populates None values."""

    name: str = Field(
        default=...,
        description=(
            "A unique human-readable name of the route. Follows the format of "
            "'input_model_name:workflow_name:output_model_name'."
        ),
    )
    input_model_name: str = Field(
        default=..., description=" Name of the input model accepted by the route."
    )
    output_model_name: str = Field(
        default=..., description="Name of the output model produced by the route."
    )
    workflow_name: str = Field(
        default=...,
        description="Name of the workflow used to transform the input model to the output model.",
    )


class RawConfig(BaseModel):
    """Describes a raw transformation configuration before any processing/validation."""

    models: list[RawModel] = Field(
        default=...,
        description="List of raw models defining the transformation graph.",
    )
    workflows: list[Workflow] = Field(
        default=...,
        description="List of available workflows.",
    )
    routes: list[RawRoute] = Field(
        default=...,
        description="List of routes composing the transformation graph.",
    )


class ConfigFields(BaseModel):
    """Container for config fields that might be needed after comparison.

    routes and workflows should be populated from the config file in either case.
    If nothing changed, they correspond to what's already persisted, else they contain
    the up to date information.
    """

    new_models: list[InternalModel] = Field(
        default_factory=list, description="Raw models from the config file."
    )
    old_models: list[InternalModel] = Field(
        default_factory=list, description="Existing, persisted models."
    )
    routes: list[RawRoute] = Field(
        default_factory=list, description="Routes from the config file."
    )
    workflows: list[Workflow] = Field(
        default_factory=list, description="Workflows from the config file."
    )


class ComparisonResultBase(BaseModel):
    """Common config fields for either outcome of the comparison.

    Used as base class for either variant for the result.
    """

    models: list[InternalModel] = Field(
        default=...,
        description="Contains either the new models to run downstream processing on or the existing, persisted models.",
    )
    routes: list[RawRoute] = Field(
        default=..., description="Up to date routes for downstream processing."
    )
    workflows: list[Workflow] = Field(
        default=..., description="Up to date workflows for downstream processing."
    )


class ComparisonResultChanged(ComparisonResultBase):
    """For changed configs, the new, raw models are returned."""


class ComparisonResultUnchanged(ComparisonResultBase):
    """For unchanged configs, the persisted models are returned."""
