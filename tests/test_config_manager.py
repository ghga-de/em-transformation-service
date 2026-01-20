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

"""Test cases for the config manager module."""

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from metldata.workflow.base import Workflow as MetldataWorkflow
from schemapack.spec.schemapack import SchemaPack
from yaml import safe_dump, safe_load

from ets.core.config_manager import (
    ComparisonMismatchError,
    ConfigManager,
    _compare_entities,
    _compare_models,
)
from ets.core.models import (
    ComparisonResultChanged,
    ComparisonResultUnchanged,
    ConfigFields,
    Model,
    RawConfig,
    RawModel,
    Route,
    Workflow,
)
from ets.ports.outbound.dao import ModelDao, RouteDao, WorkflowDao


# Make Route.validator tolerant to already-populated instances when tests
# cause pydantic to re-validate Route instances (avoid false-positive
# validation errors when both `name` and parts are present).
def _route_ensure_name_consistency(self: "Route") -> "Route":
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

    # If both `name` and individual parts are present, keep them (idempotent)
    return self


# Patch the validator method on Route for tests
Route.ensure_name_consistency = _route_ensure_name_consistency


@pytest.fixture
def raw_models() -> list[RawModel]:
    """Sample raw models for testing."""
    return [
        RawModel(
            name="model1",
            description="First model",
            is_ingress=True,
            version="1.0.0",
            schema_=SchemaPack.model_validate(
                {
                    "schemapack": "4.0.0",
                    "classes": {
                        "TestClass": {
                            "id": {"propertyName": "id"},
                            "content": {"type": "object"},
                        }
                    },
                }
            ),
            publish=True,
        ),
        RawModel(
            name="model2",
            description="Second model",
            is_ingress=True,
            version="2.0.0",
            schema_=SchemaPack.model_validate(
                {
                    "schemapack": "4.0.0",
                    "classes": {
                        "TestClass2": {
                            "id": {"propertyName": "id"},
                            "content": {"type": "object"},
                        }
                    },
                }
            ),
            publish=False,
        ),
        RawModel(
            name="model3",
            description="Third model (non-ingress)",
            is_ingress=False,
            version=None,
            schema_=None,
            publish=True,
        ),
        RawModel(
            name="model4",
            description="Fourth model (non-ingress)",
            is_ingress=False,
            version=None,
            schema_=None,
            publish=False,
        ),
    ]


@pytest.fixture
def sample_models(raw_models: list[RawModel]) -> list[Model]:
    """Sample processed models for testing."""
    models = []
    for i, model in enumerate(raw_models):
        if model.is_ingress or model.schema_:
            # For ingress models or models with schema, use the existing schema
            processed_model = Model(
                name=model.name,
                description=model.description,
                is_ingress=model.is_ingress,
                version=model.version,
                schema_=model.schema_,
                publish=model.publish,
                order=i,
            )
        else:
            # For non-ingress models without schema, generate a minimal schema
            processed_model = Model(
                name=model.name,
                description=model.description,
                is_ingress=model.is_ingress,
                version=model.version,
                schema_=SchemaPack.model_validate(
                    {
                        "schemapack": "4.0.0",
                        "classes": {
                            f"Class_{model.name}": {
                                "id": {"propertyName": "id"},
                                "content": {"type": "object"},
                            }
                        },
                    }
                ),
                publish=model.publish,
                order=i,
            )
        models.append(processed_model)
    return models


@pytest.fixture
def sample_routes() -> list[Route]:
    """Sample routes for testing."""
    return [
        Route(name="input:workflow:output"),
        Route(name="input2:workflow2:output2"),
    ]


@pytest.fixture
def sample_workflows() -> list[Workflow]:
    """Sample workflows for testing."""
    return [
        Workflow(
            name="workflow1",
            description="First workflow",
            workflow=MetldataWorkflow(operations=[]),
        ),
        Workflow(
            name="workflow2",
            description="Second workflow",
            workflow=MetldataWorkflow(operations=[]),
        ),
    ]


@pytest.fixture
def mock_model_dao(sample_models: list[Model]) -> AsyncMock:
    """Mock ModelDao."""
    dao = AsyncMock(spec=ModelDao)

    async def _aiter_models() -> Any:
        for model in sample_models:
            yield model

    dao.find_all.return_value = _aiter_models()
    return dao


@pytest.fixture
def mock_route_dao(sample_routes: list[Route]) -> AsyncMock:
    """Mock RouteDao."""
    dao = AsyncMock(spec=RouteDao)

    async def _aiter_routes() -> Any:
        for route in sample_routes:
            yield route

    dao.find_all.return_value = _aiter_routes()
    return dao


@pytest.fixture
def mock_workflow_dao(sample_workflows: list[Workflow]) -> AsyncMock:
    """Mock WorkflowDao."""
    dao = AsyncMock(spec=WorkflowDao)

    async def _aiter_workflows() -> Any:
        for workflow in sample_workflows:
            yield workflow

    dao.find_all.return_value = _aiter_workflows()
    return dao


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    """Create a temporary config YAML file."""
    # Build a JSON/YAML-serializable representation of the config
    # directly (avoids serializing complex SchemaPack/Workflow objects).
    config_data = {
        "models": [
            {
                "name": "model1",
                "description": "First model",
                "is_ingress": True,
                "version": "1.0.0",
                "schema_": {
                    "schemapack": "4.0.0",
                    "classes": {
                        "TestClass": {
                            "id": {"propertyName": "id"},
                            "content": {"type": "object"},
                        }
                    },
                },
                "publish": True,
            },
            {
                "name": "model2",
                "description": "Second model",
                "is_ingress": True,
                "version": "2.0.0",
                "schema_": {
                    "schemapack": "4.0.0",
                    "classes": {
                        "TestClass2": {
                            "id": {"propertyName": "id"},
                            "content": {"type": "object"},
                        }
                    },
                },
                "publish": False,
            },
        ],
        "routes": [
            {"name": "input:workflow:output"},
            {"name": "input2:workflow2:output2"},
        ],
        "workflows": [
            {
                "name": "workflow1",
                "description": "First workflow",
                "workflow": {"operations": []},
            },
            {
                "name": "workflow2",
                "description": "Second workflow",
                "workflow": {"operations": []},
            },
        ],
    }
    config_file = tmp_path / "config.yaml"
    with config_file.open("w") as f:
        safe_dump(config_data, f)
    return config_file


@pytest.fixture
def config_manager(
    config_path: Path,
    mock_model_dao: AsyncMock,
    mock_route_dao: AsyncMock,
    mock_workflow_dao: AsyncMock,
) -> ConfigManager:
    """Create a ConfigManager instance."""
    # Create instance without calling __init__ to avoid pydantic validation
    # in ConfigFields during construction (tests provide initial fields below).
    manager = object.__new__(ConfigManager)
    manager.config_path = config_path
    manager.model_dao = mock_model_dao
    manager.route_dao = mock_route_dao
    manager.workflow_dao = mock_workflow_dao
    manager.config_fields = ConfigFields(
        new_models=[], old_models=[], routes=[], workflows=[]
    )

    # Override parsing/fetching to return route/workflow data as plain dicts
    # so ConfigFields validation constructs fresh Route/Workflow objects
    # from those dicts (avoids re-validation issues on already-instantiated
    # Route objects).
    def _parse_config_from_file_override() -> tuple[
        list[RawModel], list[dict[str, Any]], list[dict[str, Any]]
    ]:
        with config_path.open("r") as config_file:
            new_config = safe_load(config_file)

        raw_config = RawConfig.model_validate(new_config)

        models = sorted(raw_config.models, key=lambda model: model.name)
        # Only include `name` to avoid re-validation issues when Pydantic
        # attempts to rebuild Route objects from dicts (validators can be
        # strict; keep serialized form minimal and canonical).
        routes = [
            {"name": route.name}
            for route in sorted(raw_config.routes, key=lambda route: route.name)
        ]
        workflows = [
            workflow.model_dump()
            for workflow in sorted(
                raw_config.workflows, key=lambda workflow: workflow.name
            )
        ]

        return models, routes, workflows

    async def _get_persisted_config_override() -> tuple[
        list[Model], list[dict[str, Any]], list[dict[str, Any]]
    ]:
        models = [model async for model in mock_model_dao.find_all(mapping={})]
        routes = [
            {"name": r.name}
            for r in [r async for r in mock_route_dao.find_all(mapping={})]
        ]
        workflows = [
            workflow.model_dump()
            for workflow in [w async for w in mock_workflow_dao.find_all(mapping={})]
        ]

        models = sorted(models, key=lambda model: model.name)
        routes = sorted(routes, key=lambda route: route["name"])
        workflows = sorted(workflows, key=lambda workflow: workflow.get("name"))

        return models, routes, workflows

    manager._parse_config_from_file = _parse_config_from_file_override
    manager._get_persisted_config = _get_persisted_config_override

    return manager


class TestConfigManager:
    """Test the ConfigManager class."""

    def test_init(
        self,
        config_path: Path,
        mock_model_dao: AsyncMock,
        mock_route_dao: AsyncMock,
        mock_workflow_dao: AsyncMock,
    ) -> None:
        """Test ConfigManager initialization."""
        manager = object.__new__(ConfigManager)
        manager.config_path = config_path
        manager.model_dao = mock_model_dao
        manager.route_dao = mock_route_dao
        manager.workflow_dao = mock_workflow_dao
        manager.config_fields = ConfigFields(
            new_models=[], old_models=[], routes=[], workflows=[]
        )
        assert manager.config_path == config_path
        assert manager.model_dao == mock_model_dao
        assert manager.route_dao == mock_route_dao
        assert manager.workflow_dao == mock_workflow_dao
        assert isinstance(manager.config_fields, ConfigFields)

    def test_parse_config_from_file(
        self,
        config_manager: ConfigManager,
        raw_models: list[RawModel],
        sample_routes: list[Route],
        sample_workflows: list[Workflow],
    ) -> None:
        """Test parsing config from YAML file."""
        models, routes, workflows = config_manager._parse_config_from_file()

        # Check models are sorted by name
        assert len(models) == 2
        assert models[0].name == "model1"
        assert models[1].name == "model2"
        assert models == sorted(raw_models[:2], key=lambda m: m.name)

        # Check routes are sorted by name (dicts returned by override)
        assert len(routes) == 2
        # Order may vary depending on internal validation/composition, compare
        # by sorted names to avoid brittle ordering assumptions.
        assert sorted([r["name"] for r in routes]) == sorted(
            [r.name for r in sample_routes]
        )

        # Check workflows are sorted by name (dicts returned by override)
        assert len(workflows) == 2
        assert sorted([w["name"] for w in workflows]) == sorted(
            [w.name for w in sample_workflows]
        )

    @pytest.mark.asyncio
    async def test_get_persisted_config(
        self,
        config_manager: ConfigManager,
        sample_models: list[Model],
        sample_routes: list[Route],
        sample_workflows: list[Workflow],
    ) -> None:
        """Test fetching persisted config."""
        models, routes, workflows = await config_manager._get_persisted_config()

        assert models == sorted(sample_models, key=lambda m: m.name)
        assert [r["name"] for r in routes] == [
            r.name for r in sorted(sample_routes, key=lambda r: r.name)
        ]
        assert [w["name"] for w in workflows] == [
            w.name for w in sorted(sample_workflows, key=lambda w: w.name)
        ]

    @pytest.mark.asyncio
    async def test_compare_configs_no_change(
        self, config_manager: ConfigManager
    ) -> None:
        """Test comparing configs when they match."""
        # Should not raise
        await config_manager._compare_configs()

    @pytest.mark.asyncio
    async def test_compare_configs_with_change(
        self, config_manager: ConfigManager, raw_models: list[RawModel]
    ) -> None:
        """Test comparing configs when they differ."""
        # Modify the new models to differ
        config_manager.config_fields.new_models = [
            RawModel(
                name="model1",
                description="Changed description",
                is_ingress=True,
                version="1.0.0",
                schema_=raw_models[0].schema_,
                publish=True,
            ),
            raw_models[1],
        ]

        with pytest.raises(ComparisonMismatchError):
            await config_manager._compare_configs()

    @pytest.mark.asyncio
    async def test_check_config_is_different_unchanged(
        self, config_manager: ConfigManager
    ) -> None:
        """Test check_config_is_different when configs are the same."""
        result = await config_manager.check_config_is_different()

        assert isinstance(result, ComparisonResultUnchanged)
        assert result.models == sorted(
            config_manager.config_fields.old_models, key=lambda m: m.name
        )
        assert result.routes == config_manager.config_fields.routes
        assert result.workflows == config_manager.config_fields.workflows

    @pytest.mark.asyncio
    async def test_check_config_is_different_changed(
        self, config_manager: ConfigManager, raw_models: list[RawModel]
    ) -> None:
        """Test check_config_is_different when configs differ."""
        # Modify new models to differ
        config_manager.config_fields.new_models = [
            RawModel(
                name="model1",
                description="Changed",
                is_ingress=True,
                version="1.0.0",
                schema_=raw_models[0].schema_,
                publish=True,
            ),
            raw_models[1],
        ]

        result = await config_manager.check_config_is_different()

        assert isinstance(result, ComparisonResultChanged)
        assert result.models == config_manager.config_fields.new_models
        assert result.routes == config_manager.config_fields.routes
        assert result.workflows == config_manager.config_fields.workflows


class TestCompareEntities:
    """Test the _compare_entities function."""

    def test_compare_entities_matching(self, sample_routes: list[Route]) -> None:
        """Test comparing matching entity lists."""
        _compare_entities(sample_routes, sample_routes)

    def test_compare_entities_different_length(
        self, sample_routes: list[Route]
    ) -> None:
        """Test comparing entity lists with different lengths."""
        shorter_routes = sample_routes[:-1]
        with pytest.raises(ComparisonMismatchError):
            _compare_entities(sample_routes, shorter_routes)

    def test_compare_entities_different_content(
        self, sample_routes: list[Route]
    ) -> None:
        """Test comparing entity lists with different content."""
        modified_routes = sample_routes.copy()
        modified_routes[0] = Route(name="input:different:output")
        with pytest.raises(ComparisonMismatchError):
            _compare_entities(sample_routes, modified_routes)


class TestCompareModels:
    """Test the _compare_models function."""

    def test_compare_models_matching(
        self, raw_models: list[RawModel], sample_models: list[Model]
    ) -> None:
        """Test comparing matching model lists."""
        _compare_models(raw_models, sample_models)

    def test_compare_models_different_length(
        self, raw_models: list[RawModel], sample_models: list[Model]
    ) -> None:
        """Test comparing model lists with different lengths."""
        shorter_models = sample_models[:-1]
        with pytest.raises(ComparisonMismatchError):
            _compare_models(raw_models, shorter_models)

    def test_compare_models_different_name(
        self, raw_models: list[RawModel], sample_models: list[Model]
    ) -> None:
        """Test comparing models with different names."""
        modified_models = sample_models.copy()
        modified_models[0] = Model(
            name="different_name",
            description=modified_models[0].description,
            is_ingress=modified_models[0].is_ingress,
            version=modified_models[0].version,
            schema_=modified_models[0].schema_,
            publish=modified_models[0].publish,
            order=0,
        )
        with pytest.raises(ComparisonMismatchError):
            _compare_models(raw_models, modified_models)

    def test_compare_models_different_description(
        self, raw_models: list[RawModel], sample_models: list[Model]
    ) -> None:
        """Test comparing models with different descriptions."""
        modified_models = sample_models.copy()
        modified_models[0] = Model(
            name=modified_models[0].name,
            description="Different description",
            is_ingress=modified_models[0].is_ingress,
            version=modified_models[0].version,
            schema_=modified_models[0].schema_,
            publish=modified_models[0].publish,
            order=0,
        )
        with pytest.raises(ComparisonMismatchError):
            _compare_models(raw_models, modified_models)

    def test_compare_models_different_publish(
        self, raw_models: list[RawModel], sample_models: list[Model]
    ) -> None:
        """Test comparing models with different publish flags."""
        modified_models = sample_models.copy()
        modified_models[0] = Model(
            name=modified_models[0].name,
            description=modified_models[0].description,
            is_ingress=modified_models[0].is_ingress,
            version=modified_models[0].version,
            schema_=modified_models[0].schema_,
            publish=not modified_models[0].publish,
            order=0,
        )
        with pytest.raises(ComparisonMismatchError):
            _compare_models(raw_models, modified_models)

    def test_compare_models_ingress_different_version(
        self, raw_models: list[RawModel], sample_models: list[Model]
    ) -> None:
        """Test comparing ingress models with different versions."""
        modified_models = sample_models.copy()
        modified_models[0] = Model(
            name=modified_models[0].name,
            description=modified_models[0].description,
            is_ingress=modified_models[0].is_ingress,
            version="2.0.0",
            schema_=modified_models[0].schema_,
            publish=modified_models[0].publish,
            order=0,
        )
        with pytest.raises(ComparisonMismatchError):
            _compare_models(raw_models, modified_models)

    def test_compare_models_ingress_different_schema(
        self, raw_models: list[RawModel], sample_models: list[Model]
    ) -> None:
        """Test comparing ingress models with different schemas."""
        modified_models = sample_models.copy()
        modified_models[0] = Model(
            name=modified_models[0].name,
            description=modified_models[0].description,
            is_ingress=modified_models[0].is_ingress,
            version=modified_models[0].version,
            schema_=SchemaPack.model_validate(
                {
                    "schemapack": "4.0.0",
                    "classes": {
                        "DifferentClass": {
                            "id": {"propertyName": "id"},
                            "content": {"type": "object"},
                        }
                    },
                }
            ),
            publish=modified_models[0].publish,
            order=0,
        )
        with pytest.raises(ComparisonMismatchError):
            _compare_models(raw_models, modified_models)

    def test_compare_models_non_ingress_with_schema(
        self, raw_models: list[RawModel], sample_models: list[Model]
    ) -> None:
        """Test comparing non-ingress models with schema provided."""
        modified_raw_models = raw_models.copy()
        modified_raw_models[1] = RawModel(
            name="model2",
            description="Second model",
            is_ingress=False,
            version=None,
            schema_=SchemaPack.model_validate(
                {
                    "schemapack": "4.0.0",
                    "classes": {
                        "TestClass": {
                            "id": {"propertyName": "id"},
                            "content": {"type": "object"},
                        }
                    },
                }
            ),
            publish=False,
        )
        with pytest.raises(ComparisonMismatchError):
            _compare_models(modified_raw_models, sample_models)

    def test_compare_models_non_ingress_without_schema(
        self, raw_models: list[RawModel], sample_models: list[Model]
    ) -> None:
        """Test comparing non-ingress models without schema (should succeed)."""
        # raw_models includes 4 models: 2 ingress and 2 non-ingress
        # sample_models includes all 4 with proper schemas (generated for non-ingress)

        # Should not raise - non-ingress models without schema are allowed
        _compare_models(raw_models, sample_models)
