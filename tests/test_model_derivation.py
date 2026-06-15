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

"""Integration tests for the model derivation module."""

from unittest.mock import MagicMock, patch

import pytest
from metldata.transform.exceptions import ModelAssumptionError, ModelTransformationError
from schemapack import is_equal_schemapack
from schemapack.spec.schemapack import SchemaPack

from ets.core.model_derivation import ConsistencyError, ModelDerivationError
from tests.fixtures.examples import (
    FILE_RENAMED_ID_SCHEMA,
    FILE_SCHEMA,
    INVALID_MODEL_DERIVATION_CONFIGS,
    RENAMED_ID_WITH_BACKUP_SCHEMA,
    VALID_MODEL_DERIVATION_CONFIGS,
    ModelDerivationFixture,
    mock_apply_workflow,  # noqa: F401
    model_derivation_fixture,  # noqa: F401
)


@pytest.mark.parametrize(
    "model_derivation_fixture",
    VALID_MODEL_DERIVATION_CONFIGS.values(),
    ids=VALID_MODEL_DERIVATION_CONFIGS.keys(),
    indirect=True,
)
def test_valid_config_derives_successfully(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
):
    """Confirm valid configs produce models without raising."""
    models = model_derivation_fixture.deriver.derive_models(
        model_derivation_fixture.config
    )

    assert models
    for model in models:
        assert model.schema_ is not None


@pytest.mark.parametrize(
    "model_derivation_fixture",
    INVALID_MODEL_DERIVATION_CONFIGS.values(),
    ids=INVALID_MODEL_DERIVATION_CONFIGS.keys(),
    indirect=True,
)
def test_invalid_config_raises(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
):
    """Confirm invalid configs raise ModelDerivationError during derive_models."""
    with pytest.raises(ModelDerivationError):
        model_derivation_fixture.deriver.derive_models(model_derivation_fixture.config)


@pytest.mark.parametrize(
    "model_derivation_fixture",
    [VALID_MODEL_DERIVATION_CONFIGS["multi_step_workflow"]],
    ids=["multi_step_workflow"],
    indirect=True,
)
@pytest.mark.parametrize(
    "side_effect_exc, expected_exc",
    [
        (ModelAssumptionError("model assumption violated"), ModelDerivationError),
        (ModelTransformationError("model transformation failed"), ModelDerivationError),
        (ValueError("unexpected error"), ValueError),
    ],
    ids=["ModelAssumptionError", "ModelTransformationError", "ValueError"],
)
def test_apply_workflow_exception_handling(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
    side_effect_exc: Exception,
    expected_exc: type[Exception],
):
    """Confirm expected metldata exceptions are wrapped in ModelDerivationError."""
    cfg = model_derivation_fixture.config
    deriver = model_derivation_fixture.deriver
    workflow = next(w for w in cfg.workflows if w.name == cfg.routes[0].workflow_name)

    with patch(
        "ets.core.model_derivation.TransformationHandler",
        side_effect=side_effect_exc,
    ):
        with pytest.raises(expected_exc):
            deriver._apply_workflow(
                route=cfg.routes[0], workflow=workflow, input_schema=FILE_SCHEMA
            )


@pytest.mark.parametrize(
    "model_derivation_fixture, expected_schemas",
    [
        (
            VALID_MODEL_DERIVATION_CONFIGS["multi_step_workflow"],
            {"IngressModel": FILE_SCHEMA, "DerivedModel1": FILE_RENAMED_ID_SCHEMA},
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            {
                "IngressModel": FILE_SCHEMA,
                "DerivedModel1": FILE_SCHEMA,
                "DerivedModel2": FILE_SCHEMA,
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["long_chain"],
            {
                "IngressModel": FILE_SCHEMA,
                "DerivedModel1": FILE_RENAMED_ID_SCHEMA,
                "DerivedModel2": RENAMED_ID_WITH_BACKUP_SCHEMA,
                "DerivedModel3": RENAMED_ID_WITH_BACKUP_SCHEMA,
                "DerivedModel4": FILE_RENAMED_ID_SCHEMA,
                "DerivedModel5": FILE_RENAMED_ID_SCHEMA,
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["isolated_ingress"],
            {
                "IngressModel1": FILE_SCHEMA,
                "IngressModel2": FILE_SCHEMA,
                "IngressModel3": FILE_SCHEMA,
                "DerivedModel1": FILE_RENAMED_ID_SCHEMA,
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["forking_graph"],
            {
                "IngressModel": FILE_SCHEMA,
                "DerivedModel1": FILE_RENAMED_ID_SCHEMA,
                "DerivedModel2": FILE_SCHEMA,
                "DerivedModel1a": FILE_RENAMED_ID_SCHEMA,
                "DerivedModel1b": RENAMED_ID_WITH_BACKUP_SCHEMA,
                "DerivedModel2a": FILE_RENAMED_ID_SCHEMA,
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["convergence"],
            {
                "IngressModel1": FILE_SCHEMA,
                "IngressModel2": FILE_SCHEMA,
                "DerivedModel1": FILE_SCHEMA,
                "DerivedModel2": FILE_SCHEMA,
                "ConvergedModel": FILE_RENAMED_ID_SCHEMA,
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["wide_convergence"],
            {
                "IngressModel1": FILE_SCHEMA,
                "IngressModel2": FILE_SCHEMA,
                "IngressModel3": FILE_SCHEMA,
                "DerivedModel1": FILE_SCHEMA,
                "DerivedModel2": FILE_SCHEMA,
                "DerivedModel3": FILE_SCHEMA,
                "ConvergedModel": FILE_RENAMED_ID_SCHEMA,
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["disjunct_subgraphs"],
            {
                "IngressModel1": FILE_SCHEMA,
                "IngressModel2": FILE_SCHEMA,
                "IngressModel3": FILE_SCHEMA,
                "DerivedModel1": FILE_RENAMED_ID_SCHEMA,
                "DerivedModel2": RENAMED_ID_WITH_BACKUP_SCHEMA,
                "DerivedModel3": FILE_RENAMED_ID_SCHEMA,
                "DerivedModel4": FILE_RENAMED_ID_SCHEMA,
                "DerivedModel5": RENAMED_ID_WITH_BACKUP_SCHEMA,
                "DerivedModel6": FILE_RENAMED_ID_SCHEMA,
                "DerivedModel7": FILE_SCHEMA,
                "DerivedModel8": FILE_RENAMED_ID_SCHEMA,
            },
        ),
    ],
    ids=[
        "multi_step_workflow",
        "chained_routes",
        "long_chain",
        "isolated_ingress",
        "forking_graph",
        "convergence_compatible",
        "wide_convergence",
        "disjunct_subgraphs",
    ],
    indirect=["model_derivation_fixture"],
)
def test_derive_models_produces_correct_schemas(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
    expected_schemas: dict[str, SchemaPack],
):
    """Confirm derive_models assigns the correct schema to every model across various graph topologies."""
    models = model_derivation_fixture.deriver.derive_models(
        model_derivation_fixture.config
    )

    assert len(models) == len(expected_schemas)
    by_name = {m.name: m for m in models}
    for name, expected in expected_schemas.items():
        assert is_equal_schemapack(by_name[name].schema_, expected)


@pytest.mark.parametrize(
    "model_derivation_fixture",
    [INVALID_MODEL_DERIVATION_CONFIGS["convergence_conflicting_schemas"]],
    ids=["convergence_conflicting_schemas"],
    indirect=True,
)
def test_convergence_conflicting_schemas_raises(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
):
    """Confirm converging paths that produce incompatible schemas at the output node raise."""
    with pytest.raises(ModelDerivationError, match="already has a derived schema"):
        model_derivation_fixture.deriver.derive_models(model_derivation_fixture.config)


@pytest.mark.parametrize(
    "model_derivation_fixture, expected_order",
    [
        (
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            ["IngressModel->DerivedModel1", "DerivedModel1->DerivedModel2"],
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["parallel_chains"],
            [
                "IngressModel1->DerivedModel1",
                "DerivedModel1->DerivedModel2",
                "IngressModel2->DerivedModel3",
                "DerivedModel3->DerivedModel4",
            ],
        ),
    ],
    ids=["chained_routes", "parallel_chains"],
    indirect=["model_derivation_fixture"],
)
def test_routes_processed_in_topological_order(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
    mock_apply_workflow: MagicMock,  # noqa: F811
    expected_order: list[str],
):
    """Confirm routes are processed in topological order regardless of fixture listing order."""
    call_order: list[str] = []

    def record_call_order(*, route, workflow, input_schema):
        call_order.append(f"{route.input_model_name}->{route.output_model_name}")
        return input_schema

    mock_apply_workflow.side_effect = record_call_order
    model_derivation_fixture.deriver.derive_models(model_derivation_fixture.config)

    assert call_order == expected_order


@pytest.mark.parametrize(
    "model_derivation_fixture, expected_match",
    [
        (
            INVALID_MODEL_DERIVATION_CONFIGS["orphans"],
            "could not be derived",
        ),
    ],
    ids=["all_orphans"],
    indirect=["model_derivation_fixture"],
)
def test_invalid_config_specific_error(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
    expected_match: str,
):
    """Confirm each invalid config raises ModelDerivationError with the expected message."""
    with pytest.raises(ModelDerivationError, match=expected_match):
        model_derivation_fixture.deriver.derive_models(model_derivation_fixture.config)


@pytest.mark.parametrize(
    "model_derivation_fixture",
    [VALID_MODEL_DERIVATION_CONFIGS["chained_routes"]],
    ids=["chained_routes"],
    indirect=True,
)
def test_route_references_unknown_model_raises_internal_error(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
):
    """Confirm that a route referencing a model absent from the model list raises
    ConsistencyError (the sanity check inside _process_routes).

    This state should be unreachable via the normal config-validation path;
    it is triggered here by deliberately removing a model from the config
    passed to derive_models.
    """
    deriver = model_derivation_fixture.deriver
    cfg = model_derivation_fixture.config
    # Remove DerivedModel1, which is referenced as the output of the
    # IngressModel→DerivedModel1 route
    broken_config = cfg.model_copy(
        update={"models": [m for m in cfg.models if m.name != "DerivedModel1"]}
    )
    with pytest.raises(ConsistencyError, match="internal consistency error"):
        deriver.derive_models(broken_config)
