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
from collections.abc import Mapping
from enum import StrEnum
from typing import Annotated, Any
from uuid import uuid4

from annotated_types import MinLen
from ghga_service_commons.utils.utc_dates import UTCDatetime
from metldata.workflow.base import Workflow as MetldataWorkflow
from pydantic import (
    UUID4,
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)
from schemapack.spec.datapack import DataPack
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


class OrderedRawModel(RawModel):
    """Describes the validated transformation configuration for models."""

    order: int = Field(
        default=...,
        description="Topological order of the schema in the transformation graph.",
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
    """For changed configs, the new, raw models are returned."""

    models: Annotated[list[RawModel], MinLen(1)] = Field(
        default=...,
        description="List of raw models defining the transformation graph.",
    )
    routes: Annotated[list[Route], MinLen(1)] = Field(
        default=..., description="Up to date routes for downstream processing."
    )
    workflows: Annotated[list[Workflow], MinLen(1)] = Field(
        default=..., description="Up to date workflows for downstream processing."
    )

    @model_validator(mode="after")
    def _require_at_least_one_emim(self) -> "RawConfig":
        if not any(model.is_ingress for model in self.models):
            raise ValueError("At least one model must be an EMIM (is_ingress=True).")
        return self


class ValidatedConfig(BaseModel):
    """Describes the validated transformation configuration.
    It captures the state after the config validation, before the model derivation and
    schema generation.
    """

    models: Annotated[list[OrderedRawModel], MinLen(1)] = Field(
        default=..., description="Validated models with topological order."
    )
    routes: Annotated[list[Route], MinLen(1)] = Field(
        default=...,
        description="Validated routes with consistent naming and references.",
    )
    workflows: Annotated[list[Workflow], MinLen(1)] = Field(
        default=..., description="Validated workflows."
    )


class PersistedConfig(BaseModel):
    """For unchanged configs, the persisted models are returned.

    All list fields may be empty as the config might not be fully populated yet.
    """

    models: list[Model] = Field(
        default=...,
        description="Contains the existing, persisted models.",
    )
    routes: list[Route] = Field(
        default=..., description="Up to date routes for downstream processing."
    )
    workflows: list[Workflow] = Field(
        default=..., description="Up to date workflows for downstream processing."
    )


class AEMPack(BaseModel):
    """Model for derived AEMPacks."""

    id: UUID4 = Field(
        default=...,
        description="Unique identifier of the EMPack.",
    )
    pid: str = Field(
        default=...,
        description="Non-unique identifier that's shared between an incoming AEMPacks and all its derived AEMPacks.",
    )
    model_name: str = Field(
        default=...,
        description="Unique name of the model the EMPack conforms to.",
    )
    data: DataPack = Field(
        default=...,
        description="The data conforming to a corresponding Schemapack stored in the model denoted by model_name.",
    )
    annotation: dict = Field(
        default=...,
        description="Additional information used in some workflows during derivation.",
    )
    model_config = ConfigDict(frozen=True)

    @field_validator("data", mode="before")
    @classmethod
    def _deserialize_data(cls, v: Mapping[str, Any] | DataPack) -> DataPack:
        if isinstance(v, DataPack):
            return v
        return DataPack.model_validate(v)

    @field_serializer("data")
    def _serialize_data(self, v: DataPack) -> dict[str, Any]:
        return json.loads(v.model_dump_json())


class VersionedAEMPack(AEMPack):
    """An AEMPack that additionally carries a version."""

    version: int = Field(
        default=...,
        description="Current version of the AEMPack. Used to resolve republishing conflicts.",
    )


class IncomingAEMPack(VersionedAEMPack):
    """Variant of the AEMPack for the processing queue."""

    correlation_id: UUID4 = Field(
        default=...,
        description="Correlation ID of the event that triggered ingestion of this AEMPack.",
    )
    claimed_at: UTCDatetime | None = Field(
        default=None,
        description=(
            "When this AEMPack was claimed for processing, recorded by the MongoDB"
            " server clock. None if unclaimed. A claim older than the configured TTL"
            " is considered stale and may be reclaimed by another instance."
        ),
    )
    processed_at: UTCDatetime | None = Field(
        default=None,
        description="When this AEMPack was successfully processed. None if not yet processed.",
    )
    failed_at: UTCDatetime | None = Field(
        default=None,
        description=(
            "When this AEMPack's processing failed. None if it has not failed."
            " Failures also set ``processed_at`` (so the pack is not re-claimed)."
            " This field is what distinguishes a failed pack from a successful one."
        ),
    )
    needs_reprocessing: bool = Field(
        default=False,
        description="Set to True when a new version of this AEMPack arrives while it is being processed, signalling that reprocessing is required after the current run completes.",
    )
    attempts: int = Field(
        default=0,
        description=(
            "Number of unexpected (non-DataDerivationError) processing failures counted"
            " against the currently stored content/version. Reset to 0 when a newer"
            " version is queued or the pack is flagged for reprocessing. Once it reaches"
            " processing_max_attempts the pack is parked as failed instead of retried."
        ),
    )
    model_config = ConfigDict(frozen=True)


class AEMPackStatus(StrEnum):
    """Final lifecycle states of an incoming AEMPack that are published as events.

    Intermediate states (e.g. being claimed for processing) are intentionally not
    represented: only states an outside consumer cares about are emitted.
    """

    QUEUED = "queued"
    PROCESSED = "processed"
    FAILED = "failed"


class AEMPackStatusEvent(BaseModel):
    """A processing-lifecycle event published on the status channel.

    A single model carries every status so all events sit on the same topic and can
    be correlated back to the originating incoming AEMPack via (pid, model_name,
    version). The ``transformation_step``/``error_*`` fields are only populated for
    ``FAILED`` events.
    """

    id: UUID4 = Field(
        default_factory=uuid4, description="Unique identifier of the event."
    )
    pid: str = Field(
        default=...,
        description="Shared identifier of the incoming AEMPack and its derived packs.",
    )
    model_name: str = Field(
        default=...,
        description="Name of the model the AEMPack being processed conforms to.",
    )
    version: int = Field(
        default=..., description="Version of the incoming AEMPack this event concerns."
    )
    status: AEMPackStatus = Field(
        default=...,
        description="The lifecycle state this event reports.",
    )
    transformation_step: str | None = Field(
        default=None,
        description="Name of the workflow step that failed, if known. Only set for FAILED.",
    )
    error_type: str | None = Field(
        default=None,
        description="Class name of the error that caused the failure. Only set for FAILED.",
    )
    error_message: str | None = Field(
        default=None,
        description="Human-readable message of the underlying error. Only set for FAILED.",
    )
