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
"""Inbound port for configuration validation."""

from abc import ABC, abstractmethod

from ets.core.models import ComparisonResultChanged


class ConfigValidationError(RuntimeError):
    """Raised when configuration validation fails."""


class ConfigValidatorPort(ABC):
    """Inbound port for configuration validation."""

    @abstractmethod
    def validate(self, changed_config: ComparisonResultChanged) -> None:
        """Validate new configuration loaded from yaml file.

        This should only be called when the loaded config does not match what has
        already been persisted previously.

        Args:
            result: ComparisonResultChanged containing models, routes, and workflows.

        Raises:
            ConfigValidationError: If any validation fails.
        """
