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

"""Port for the config version tracker."""

from abc import ABC, abstractmethod


class ConfigVersionPort(ABC):
    """Port for tracking the monotonically increasing config version.

    The version is incremented each time a new config is persisted.
    Processing checkpoints compare their stored version against the
    current version to detect config changes.
    """

    @abstractmethod
    async def get_version(self) -> int:
        """Return the current config version.

        Returns 0 if no version document exists yet.
        """

    @abstractmethod
    async def increment_version(self) -> int:
        """Increment the config version and return the new value."""
