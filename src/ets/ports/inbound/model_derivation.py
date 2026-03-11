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
"""Inbound port for model derivation."""

from abc import ABC, abstractmethod

from ets.core.models import Model


class ModelDerivationError(RuntimeError):
    """Raised when schema derivation fails for a route in the transformation graph."""


class ConsistencyError(ModelDerivationError):
    """Raised when an internal consistency check fails inside the model deriver.

    This indicates a programming error or a state that should have been caught
    by the config validator before reaching the derivation stage.
    """


class ModelDeriverPort(ABC):
    """Derives output schemas for all models in the transformation graph."""

    @abstractmethod
    def derive_models(self) -> list[Model]:
        """Derive and return all models with populated schemas.

        Raises:
            ModelDerivationError: If schema derivation fails for any route.
        """
