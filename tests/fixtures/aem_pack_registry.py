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

"""Fixtures for AEMPackRegistry tests."""

from uuid import uuid4

from schemapack.spec.datapack import DataPack

from ets.adapters.inbound.event_schemas import AEMPack
from ets.core.model_derivation import ModelDeriver
from ets.core.models import PersistedConfig
from tests.fixtures.examples import (
    VALID_MODEL_DERIVATION_CONFIGS,
    load_model_derivation_config,
)


def build_chained_routes_config() -> PersistedConfig:
    """Build a fully-derived PersistedConfig from the chained_routes YAML fixture.

    Graph: A(ingress, order=0) → B(derived, order=1) → C(derived, order=2)
    All models use the identity-like ``preserve_content`` workflow.
    """
    validated = load_model_derivation_config(
        VALID_MODEL_DERIVATION_CONFIGS["chained_routes"]
    )
    derived_models = ModelDeriver(config=validated).derive_models()
    return PersistedConfig(
        models=derived_models,
        routes=validated.routes,
        workflows=validated.workflows,
    )


# Shared DataPack and AEMPack used across registry tests.
# The ``File`` class in the DataPack aligns with the File class defined in
# the chained_routes SchemaPack.
REGISTRY_DATAPACK = DataPack.model_validate(
    {"datapack": "3.0.0", "resources": {"File": {}}}
)

ORIGINAL_ID = uuid4()

ORIGINAL_AEM_PACK = AEMPack(
    id=ORIGINAL_ID,
    model_name="A",
    original_id=None,
    data=REGISTRY_DATAPACK,
    annotation={"source": "test"},
)
