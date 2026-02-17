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
"""Contains functionality to load and compare service config."""

from abc import ABC, abstractmethod

from ets.core.models import ComparisonResultChanged, ComparisonResultUnchanged


class ComparisonMismatchError(RuntimeError):
    """Custom error type raised on any mismatch between the existing and new config."""


class ConfigManagerPort(ABC):
    """Manages loading old and new config and comparing them."""

    @abstractmethod
    async def check_config_is_different(
        self,
    ) -> ComparisonResultChanged | ComparisonResultUnchanged:
        """Compare new config with the persisted one.
        Returns:
                ComparisonResultChanged: when the configs differ, containing the new models, routes, and workflows.
                ComparisonResultUnchanged: when the configs are equal, containing the persisted models, routes, and workflows.
        """
