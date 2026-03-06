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

"""Tests for convergence graphs that involve non-identity transformations.

Two complementary scenarios are covered:

1. **Incompatible convergence** - one branch applies the identity transformation
   (preserving ``format``) while the other applies the non-identity
   ``drop_format`` transformation (removing ``format``).  Both branches feed
   into the same convergence node, so the schemas that arrive there are
   incompatible and ``derive_models`` must raise ``ModelDerivationError``.

2. **Compatible convergence** - one branch applies *only* the non-identity
   ``drop_format`` transformation; the other applies the identity transformation
   *followed by* ``drop_format``.  Both paths therefore produce an equivalent
   schema (no ``format`` field) at the convergence node, so ``derive_models``
   must succeed and the final schema must not contain ``format``.
"""

import pytest
from schemapack import is_equal_schemapack
from schemapack.spec.schemapack import SchemaPack

from ets.ports.inbound.model_derivation import ModelDerivationError
from tests.fixtures.examples import (
    INVALID_MODEL_DERIVATION_CONFIGS,
    VALID_MODEL_DERIVATION_CONFIGS,
)
from tests.fixtures.model_derivation import (
    ModelDerivationFixture,
    model_derivation_fixture,  # noqa: F401
)

# Expected schema at the convergence node: File without the 'format' field.
EXPECTED_REDUCED_SCHEMA = SchemaPack.model_validate(
    {
        "schemapack": "4.0.0",
        "classes": {
            "File": {
                "id": {"propertyName": "alias"},
                "content": {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "additionalProperties": False,
                    "properties": {
                        "checksum": {"type": "string"},
                        "filename": {"type": "string"},
                        "size": {"type": "integer"},
                    },
                    "required": ["checksum", "filename", "size"],
                    "type": "object",
                },
            }
        },
    }
)


@pytest.mark.parametrize(
    "model_derivation_fixture",
    [INVALID_MODEL_DERIVATION_CONFIGS["convergence_non_identity_incompatible"]],
    ids=["convergence_non_identity_incompatible"],
    indirect=True,
)
def test_convergence_incompatible_schemas_raises(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
):
    """Convergence where identity and non-identity branches produce incompatible schemas.

    One branch keeps ``format`` (identity workflow) while the other drops it
    (``drop_format`` workflow).  The two schemas arriving at the shared
    convergence node are structurally incompatible, so ``derive_models`` must
    raise ``ModelDerivationError`` with a message indicating the conflict.
    """
    with pytest.raises(ModelDerivationError, match="already has a derived schema"):
        model_derivation_fixture.deriver.derive_models()


@pytest.mark.parametrize(
    "model_derivation_fixture",
    [VALID_MODEL_DERIVATION_CONFIGS["convergence_non_identity_compatible"]],
    ids=["convergence_non_identity_compatible"],
    indirect=True,
)
def test_convergence_compatible_schemas_succeeds(
    model_derivation_fixture: ModelDerivationFixture,  # noqa: F811
):
    """Convergence where branches using different workflow combinations agree on the schema.

    Branch A uses *only* the non-identity ``drop_format`` transformation.
    Branch B uses the identity transformation *followed by* ``drop_format``.
    Both paths remove ``format``, so they produce an equivalent schema at the
    shared convergence node.  ``derive_models`` must succeed and the convergence
    node ``D_out`` must carry a schema that:

    * differs from the original ingress schema (which contained ``format``), and
    * exactly matches the expected reduced schema (without ``format``).
    """
    models = model_derivation_fixture.deriver.derive_models()

    by_name = {m.name: m for m in models}
    assert set(by_name.keys()) == {"I1", "I2", "D1", "D2", "D_out"}

    d_out_schema = by_name["D_out"].schema_
    i1_schema = by_name["I1"].schema_

    # The convergence node schema must differ from the ingress schema.
    assert not is_equal_schemapack(d_out_schema, i1_schema)

    # The convergence node schema must match the expected reduced schema.
    assert is_equal_schemapack(d_out_schema, EXPECTED_REDUCED_SCHEMA)

    # Verify 'format' is absent from D_out but present in the ingress models.
    d_out_props = d_out_schema.classes["File"].content.get("properties", {})
    i1_props = i1_schema.classes["File"].content.get("properties", {})
    assert "format" not in d_out_props
    assert "format" in i1_props

    # The two intermediate models must also carry the reduced schema.
    for name in ("D1", "D2"):
        intermediate_props = (
            by_name[name].schema_.classes["File"].content.get("properties", {})
        )
        assert "format" not in intermediate_props
