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

"""Defines dataclasses for holding business-logic data."""

from typing import Self

from metldata.workflow.base import Workflow as MetldataWorkflow
from pydantic import (
    BaseModel,
    Field,
    model_validator,
)
from schemapack.spec.schemapack import SchemaPack


class RawModel(BaseModel):
    """Describes a raw model before any processing."""

    name: str = Field(..., description="A Unique human-readable name of the model.")
    description: str | None = Field(
        None, description="A human-readable description of the model."
    )
    is_ingress: bool = Field(
        ...,
        description="Whether this model is an experimental metadata ingress model (EMIM).",
    )
    version: str | None = Field(
        ...,
        description="The version of the model. None if the model is not an EMIM.",
    )
    schema_: SchemaPack | None = Field(
        ...,
        description="Schema associated with the model. None if it is not an EMIM or not yet computed.",
    )
    publish: bool = Field(
        ...,
        description="whether the data conforming to the schema should be published.",
    )


class Model(RawModel):
    """Describes a model after resolving the topological ordering and deriving the schemas."""

    schema_: SchemaPack = Field(..., description="Schema associated with the model.")
    order: int = Field(
        ..., description="Topological order of the schema in the transformation graph."
    )


class Workflow(BaseModel):
    """Describes a metldata compatible workflow definition."""

    name: str = Field(
        ...,
        description="A unique human-readable name of the workflow indicating the purpose of the workflow.",
    )
    description: str | None = Field(
        None, description="A human-readable description of the workflow."
    )
    workflow: MetldataWorkflow = Field(
        ..., description="Workflow definition in metldata Workflow format."
    )


class Route(BaseModel):
    """Describes the routes for transforming models and data by referencing the
    workflow, the input and output models involved in each transformation by name.
    """

    name: str | None = Field(
        None,
        description=(
            "A unique human-readable name of the route. Follows the format of "
            "'input_model_name:workflow_name:output_model_name'."
        ),
    )
    input_model_name: str | None = Field(
        None, description=" Name of the input model accepted by the route."
    )
    output_model_name: str | None = Field(
        None, description="Name of the output model produced by the route."
    )
    workflow_name: str | None = Field(
        None,
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

        if not self.name and has_all_parts:
            self.name = (
                f"{self.input_model_name}:{self.workflow_name}:{self.output_model_name}"
            )
            return self

        raise ValueError(
            "Either 'name' or all of 'input_model_name', 'workflow_name', 'output_model_name' need to be provided."
        )


class RawConfig(BaseModel):
    """Describes a raw transformation configuration before any processing/validation."""

    models: list[RawModel] = Field(
        ...,
        description="List of raw models defining the transformation graph.",
    )
    workflows: list[Workflow] = Field(
        ...,
        description="List of available workflows.",
    )
    routes: list[Route] = Field(
        ...,
        description="List of routes composing the transformation graph.",
    )
