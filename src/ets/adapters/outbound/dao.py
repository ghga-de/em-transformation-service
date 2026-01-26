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

"""DAO translators for accessing the database."""

from hexkit.protocols.dao import DaoFactoryProtocol

from ets.adapters.inbound.event_schemas import AEMPack
from ets.core import models
from ets.ports.outbound.dao import AEMPackDao, ModelDao, RouteDao, WorkflowDao


async def get_model_dao(*, dao_factory: DaoFactoryProtocol) -> ModelDao:
    """Setup the Model DAO using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name="models", dto_model=models.Model, id_field="name"
    )


async def get_workflow_dao(*, dao_factory: DaoFactoryProtocol) -> WorkflowDao:
    """Setup the Workflow DAO using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name="workflows", dto_model=models.Workflow, id_field="name"
    )


async def get_route_dao(*, dao_factory: DaoFactoryProtocol) -> RouteDao:
    """Setup the Route DAO using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(
        name="routes", dto_model=models.RouteDTO, id_field="name"
    )


async def get_aem_pack_dao(*, dao_factory: DaoFactoryProtocol) -> AEMPackDao:
    """Setup the AEMPack DAO using the specified provider of the DaoFactoryProtocol."""
    return await dao_factory.get_dao(name="aem_packs", dto_model=AEMPack, id_field="id")
