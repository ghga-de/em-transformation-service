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

"""Integration tests for the model derivation module.

Configurations are loaded from YAML fixture files under
``tests/fixtures/example_configs/model_derivation/``.  Valid configs live in
``valid/``, invalid ones in ``invalid/``.  Each file contains a
``ValidatedConfig``-compatible mapping (models with ``order``, routes, and
workflows).

``_apply_workflow`` is mocked only in tests that deliberately control
per-route schema values to verify routing and ordering logic.  Tests that
merely check for success or failure run against real ``TransformationHandler``
execution.
"""

from unittest.mock import MagicMock, patch

import pytest
from schemapack import is_equal_schemapack
from schemapack.spec.schemapack import SchemaPack

from ets.ports.inbound.model_derivation import ModelDerivationError
from tests.fixtures.examples import (
    INVALID_MODEL_DERIVATION_CONFIGS,
    VALID_MODEL_DERIVATION_CONFIGS,
)
from tests.fixtures.model_derivation import (
    SCHEMA_A,
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
    models = model_derivation_fixture.deriver.derive_models()

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
        model_derivation_fixture.deriver.derive_models()


@pytest.mark.parametrize(
    "model_derivation_fixture",
    [VALID_MODEL_DERIVATION_CONFIGS["multi_step_workflow"]],
    ids=["multi_step_workflow"],
    indirect=True,
)
def test_apply_workflow_wraps_step_failure(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
):
    """Confirm any exception raised inside a workflow step is wrapped in ModelDerivationError."""
    cfg = model_derivation_fixture.config
    deriver = model_derivation_fixture.deriver

    with patch(
        "ets.core.model_derivation.TransformationHandler",
        side_effect=ValueError("Destined to fail."),
    ):
        with pytest.raises(ModelDerivationError, match="Schema derivation failed"):
            deriver._apply_workflow(route=cfg.routes[0], input_schema=SCHEMA_A)


@pytest.mark.parametrize(
    "model_derivation_fixture, expected_schemas",
    [
        (
            VALID_MODEL_DERIVATION_CONFIGS["multi_step_workflow"],
            {"in": SCHEMA_A, "out": SCHEMA_A},
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            {"A": SCHEMA_A, "B": SCHEMA_A, "C": SCHEMA_A},
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["long_chain"],
            {
                "I": SCHEMA_A,
                **{f"D{i}": SCHEMA_A for i in range(1, 6)},
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["isolated_ingress"],
            {"I1": SCHEMA_A, "I2": SCHEMA_A, "I3": SCHEMA_A},
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["forking_graph"],
            {
                "I": SCHEMA_A,
                "D1": SCHEMA_A,
                "D2": SCHEMA_A,
                "D1a": SCHEMA_A,
                "D1b": SCHEMA_A,
                "D2a": SCHEMA_A,
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["convergence"],
            {
                "I1": SCHEMA_A,
                "I2": SCHEMA_A,
                "D1": SCHEMA_A,
                "D2": SCHEMA_A,
                "D_out": SCHEMA_A,
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["wide_convergence"],
            {
                "I1": SCHEMA_A,
                "I2": SCHEMA_A,
                "I3": SCHEMA_A,
                "D1": SCHEMA_A,
                "D2": SCHEMA_A,
                "D3": SCHEMA_A,
                "D_out": SCHEMA_A,
            },
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["disjunct_subgraphs"],
            {
                "I1": SCHEMA_A,
                "I2": SCHEMA_A,
                "I3": SCHEMA_A,
                "D1": SCHEMA_A,
                "D2": SCHEMA_A,
                "D3": SCHEMA_A,
                "D4": SCHEMA_A,
                "D5": SCHEMA_A,
                "D6": SCHEMA_A,
                "D7": SCHEMA_A,
                "D8": SCHEMA_A,
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
    models = model_derivation_fixture.deriver.derive_models()

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
        model_derivation_fixture.deriver.derive_models()


@pytest.mark.parametrize(
    "model_derivation_fixture, expected_order",
    [
        (
            VALID_MODEL_DERIVATION_CONFIGS["chained_routes"],
            ["A->B", "B->C"],
        ),
        (
            VALID_MODEL_DERIVATION_CONFIGS["interleaved_subgraphs"],
            ["I1->D1", "I2->D3", "D1->D2", "D3->D4"],
        ),
    ],
    ids=["chained_routes", "interleaved_subgraphs"],
    indirect=["model_derivation_fixture"],
)
def test_routes_processed_in_topological_order(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
    mock_apply_workflow: MagicMock,  # noqa: F811
    expected_order: list[str],
):
    """Confirm routes are processed in topological order regardless of fixture listing order."""
    call_order: list[str] = []

    def record_call_order(*, route, input_schema):
        call_order.append(f"{route.input_model_name}->{route.output_model_name}")
        return input_schema

    mock_apply_workflow.side_effect = record_call_order
    model_derivation_fixture.deriver.derive_models()

    assert call_order == expected_order


@pytest.mark.parametrize(
    "model_derivation_fixture, expected_match",
    [
        (
            INVALID_MODEL_DERIVATION_CONFIGS["all_orphans"],
            "could not be derived",
        ),
        (
            INVALID_MODEL_DERIVATION_CONFIGS["nonexistent_model_route"],
            "topological order",
        ),
    ],
    ids=["all_orphans", "nonexistent_model_route"],
    indirect=["model_derivation_fixture"],
)
def test_invalid_config_specific_error(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
    expected_match: str,
):
    """Confirm each invalid config raises ModelDerivationError with the expected message."""
    with pytest.raises(ModelDerivationError, match=expected_match):
        model_derivation_fixture.deriver.derive_models()
