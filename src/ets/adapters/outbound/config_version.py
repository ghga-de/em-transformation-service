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

"""MongoDB adapter for the config version tracker."""

import logging

from pymongo import ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection

from ets.constants import CONFIG_VERSION_ID
from ets.ports.outbound.config_version import ConfigVersionPort

log = logging.getLogger(__name__)


class ConfigVersion(ConfigVersionPort):
    """MongoDB-backed config version tracker.

    Uses a single document with a fixed _id and an integer `version` field.
    """

    def __init__(self, *, collection: AsyncCollection):
        self._collection = collection

    async def get_version(self) -> int:
        """Return the current config version.

        Returns 0 if no version document exists yet.
        """
        doc = await self._collection.find_one({"_id": CONFIG_VERSION_ID})
        if doc is None:
            return 0
        return doc["version"]

    async def increment_version(self) -> int:
        """Increment the config version and return the new value."""
        doc = await self._collection.find_one_and_update(
            {"_id": CONFIG_VERSION_ID},
            {"$inc": {"version": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        new_version = doc["version"]  # type: ignore
        log.info("Config version incremented to %d.", new_version)
        return new_version
