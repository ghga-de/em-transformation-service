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

"""Manages transformation config related operations."""

import logging
from collections import defaultdict

from ets.core.models import PersistedConfig, RawConfig, ValidatedConfig
from ets.ports.inbound.config_comparator import ConfigComparatorPort
from ets.ports.inbound.config_manager import ConfigManagerPort
from ets.ports.inbound.config_validator import (
    ConfigValidationError,
    ConfigValidatorPort,
)

log = logging.getLogger(__name__)


class ConfigManager(ConfigManagerPort):
    """Manages loading, comparison, validation and selection of an active config."""

    def __init__(
        self, validator: ConfigValidatorPort, comparator: ConfigComparatorPort
    ):
        self.validator = validator
        self.comparator = comparator

    def resolve_transformation_config(self) -> PersistedConfig | ValidatedConfig:
        """Resolve the given transformation config.

        This includes:
        - Comparing raw config with the persisted config
        - If they differ, validate the raw config and return it for further processing
        - If they are the same, return the persisted config
        """
        # compare configs
        match self.comparator.compare_configs():
            case RawConfig() as raw_config:
                # validate new config
                config = self.validator.validate(raw_config)
                return self._prune_unpublished_leaves(config)
            case PersistedConfig() as persisted_config:
                return persisted_config

    def _prune_unpublished_leaves(self, config: ValidatedConfig) -> ValidatedConfig:
        """Remove unpublished trailing models, along with their associated routes and orphaned workflows.

        Traverses models in reverse order, pruning any that appear after the last published
        model within each subgraph. Routes referencing pruned models are removed, and workflows
        are pruned if no remaining routes reference them.
        """
        pruned_model_names = self._collect_unpublished_model_names(config)

        # Directly prune models from config as they are unique
        config.models = [m for m in config.models if m.name not in pruned_model_names]
        for name in pruned_model_names:
            log.warning("Pruned unpublished model: %s", name)
        if not config.models:
            raise ConfigValidationError("All models were pruned from the config.")

        # Collect workflow candidates and drop routes referencing pruned models
        surviving_routes = []
        workflow_prune_candidates: set[str] = set()
        for route in config.routes:
            if (
                route.input_model_name in pruned_model_names
                or route.output_model_name in pruned_model_names
            ):
                workflow_prune_candidates.add(route.workflow_name)
                log.warning("Pruned route referencing removed model: %s", route.name)
            else:
                surviving_routes.append(route)
        if not surviving_routes:
            raise ConfigValidationError("All routes were pruned from the config.")

        config.routes = surviving_routes

        # Remove still referenced workflows from the list of deletion candidates
        for route in config.routes:
            workflow_prune_candidates.discard(route.workflow_name)

        # Finally, also prune the workflows
        config.workflows = [
            workflow
            for workflow in config.workflows
            if workflow.name not in workflow_prune_candidates
        ]
        for name in workflow_prune_candidates:
            log.warning("Pruned orphaned workflow: %s", name)
        if not config.workflows:
            raise ConfigValidationError("All workflows were pruned from the config.")

        return config

    def _collect_unpublished_model_names(self, config: ValidatedConfig) -> set[str]:
        """Return names of unpublished models with no surviving downstream neighbors.

        Iterates models in reverse topological order. An unpublished model is pruned
        if all its direct downstream neighbors have already been pruned, or it has none.
        Published models are never pruned.
        """
        # Build downstream neighbor map
        downstream = defaultdict(set)
        for route in config.routes:
            downstream[route.input_model_name].add(route.output_model_name)

        # Use inverted model order to start at the last leaf
        models = sorted(config.models, key=lambda model: model.order, reverse=True)

        pruned: set[str] = set()

        for model in models:
            if not model.publish and not (downstream[model.name] - pruned):
                pruned.add(model.name)

        return pruned
