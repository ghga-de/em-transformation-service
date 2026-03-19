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

"""DAO interface for accessing the database."""

from abc import ABC, abstractmethod

from hexkit.protocols.dao import Dao, ResourceNotFoundError
from hexkit.protocols.daopub import DaoPublisher

from ets.core import models
from ets.core.models import AEMPack, UnprocessedAEMPack

__all__ = ["ResourceNotFoundError"]


ModelDao = Dao[models.Model]
WorkflowDao = Dao[models.Workflow]
RouteDao = Dao[models.Route]
AEMPackDao = Dao[AEMPack]
UnprocessedAEMPackDao = Dao[UnprocessedAEMPack]

class AEMPackEventPublisherPort(ABC):
    """Port for publishing AEMPack lifecycle events to downstream consumers."""

    @abstractmethod
    async def get_aem_pack_dao(self) -> DaoPublisher[AEMPack]:
        """Publish an upsert event for the given AEMPack."""
