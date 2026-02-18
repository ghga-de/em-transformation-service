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

import json
from collections.abc import Mapping, Sequence
from typing import Any

from metldata.workflow.base import Workflow as MetldataWorkflow
from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
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

    schema_: SchemaPack | None = Field(
        default=..., description="Schema associated with the model."
    )

    @field_validator("schema_", mode="before")
    @classmethod
    def _deserialize_schema(
        cls, v: Mapping[str, Any] | SchemaPack | None
    ) -> SchemaPack | None:
        if v is None or isinstance(v, SchemaPack):
            return v
        return SchemaPack.model_validate(v)

    @field_serializer("schema_")
    def _serialize_schema(self, v: SchemaPack | None) -> dict[str, Any] | None:
        return json.loads(v.model_dump_json()) if v is not None else None


class Model(ModelBase):
    """Describes a model after resolving the topological ordering and deriving the schemas."""

    schema_: SchemaPack = Field(
        default=..., description="Schema associated with the model."
    )
    order: int = Field(
        default=...,
        description="Topological order of the schema in the transformation graph.",
    )

    @field_validator("schema_", mode="before")
    @classmethod
    def _deserialize_schema(cls, v: Mapping[str, Any] | SchemaPack) -> SchemaPack:
        if isinstance(v, SchemaPack):
            return v
        return SchemaPack.model_validate(v)

    @field_serializer("schema_")
    def _serialize_schema(self, v: SchemaPack) -> dict[str, Any]:
        return json.loads(v.model_dump_json())


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


class Route(BaseModel):
    """Describes a route for transforming models and data by referencing the
    workflow, the input and output models involved in each transformation by name.
    """

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

    @model_validator(mode="before")
    @classmethod
    def ensure_name_consistency(cls, data: Any) -> Any:
        """Ensures that the route name is consistent with the rest of the attributes.

        - If only 'name' provided, decomposes it into the three parts.
        - If all of 'input_model_name', 'output_model_name', 'workflow_name' are provided,
        composes the 'name'.
        - Otherwise, raises a ValueError.
        """
        if not isinstance(data, dict):
            return data

        name = data.get("name")
        input_model_name = data.get("input_model_name")
        output_model_name = data.get("output_model_name")
        workflow_name = data.get("workflow_name")

        name_parts = [input_model_name, workflow_name, output_model_name]
        has_all_parts = all(part is not None for part in name_parts)
        has_no_parts = all(part is None for part in name_parts)

        if name and has_no_parts:
            parts = name.split(":")
            if len(parts) != 3:
                raise ValueError(
                    "'name' should be formatted as 'input_model_name:workflow_name:output_model_name'"
                )
            (
                data["input_model_name"],
                data["workflow_name"],
                data["output_model_name"],
            ) = parts
            return data

        if has_all_parts:
            composed_name = f"{input_model_name}:{workflow_name}:{output_model_name}"
            if not name:
                data["name"] = composed_name
            elif name != composed_name:
                raise ValueError(
                    f"Provided name '{name}' and name assembled from parts '{composed_name}' do not match."
                )
            return data

        raise ValueError(
            "Either 'name' or all of 'input_model_name', 'workflow_name', 'output_model_name' need to be provided."
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
    routes: list[Route] = Field(
        default=...,
        description="List of routes composing the transformation graph.",
    )


class ComparisonResultBase(BaseModel):
    """Common config fields for either outcome of the comparison.

    Used as base class for either variant for the result.
    """

    models: Sequence[RawModel | Model] = Field(
        default=...,
        description="Contains either the new models to run downstream processing on or the existing, persisted models.",
    )
    routes: list[Route] = Field(
        default=..., description="Up to date routes for downstream processing."
    )
    workflows: list[Workflow] = Field(
        default=..., description="Up to date workflows for downstream processing."
    )


class ComparisonResultChanged(ComparisonResultBase):
    """For changed configs, the new, raw models are returned."""


class ComparisonResultUnchanged(ComparisonResultBase):
    """For unchanged configs, the persisted models are returned."""
