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

"""Pruning logic for unproductive subgraphs in transformation configs."""

import logging
from collections import defaultdict

from ets.core.models import ValidatedConfig
from ets.ports.inbound.config_validator import ConfigValidationError

log = logging.getLogger(__name__)


def prune_unproductive_subgraphs(config: ValidatedConfig) -> ValidatedConfig:
    """Remove unpublished trailing models, along with their associated routes and orphaned workflows.

    Traverses models in reverse order, pruning any that appear after the last published
    model within each subgraph - EMIMs are preserved even if they are no longer referenced.
    Routes referencing pruned models are removed, and workflows
    are pruned if no remaining routes reference them.
    """
    pruned_model_names = _prune_models(config)
    workflow_prune_candidates = _prune_routes(config, pruned_model_names)
    _prune_workflows(config, workflow_prune_candidates)
    return config


def _prune_models(config: ValidatedConfig) -> set[str]:
    """Prune unpublished leaf models and return their names."""
    # Build downstream neighbor map
    downstream = defaultdict(set)
    for route in config.routes:
        downstream[route.input_model_name].add(route.output_model_name)

    # Check which models need pruning by traversing in reverse topological order
    pruned_model_names: set[str] = set()
    for model in sorted(config.models, key=lambda model: model.order, reverse=True):
        if (
            not model.is_ingress
            and not model.publish
            and not (downstream[model.name] - pruned_model_names)
        ):
            pruned_model_names.add(model.name)

    # Directly prune models from config as they are unique
    config.models = [m for m in config.models if m.name not in pruned_model_names]
    for name in pruned_model_names:
        log.warning("Pruned unpublished model: %s", name)
    if not config.models:
        raise ConfigValidationError("All models were pruned from the config.")

    return pruned_model_names


def _prune_routes(config: ValidatedConfig, pruned_model_names: set[str]) -> set[str]:
    """Prune routes referencing pruned models and return orphaned workflow pruning candidates."""
    surviving_routes = []
    workflow_prune_candidates: set[str] = set()
    for route in config.routes:
        if route.output_model_name in pruned_model_names:
            workflow_prune_candidates.add(route.workflow_name)
            log.warning("Pruned route referencing removed model: %s", route.name)
        else:
            surviving_routes.append(route)
    if not surviving_routes:
        raise ConfigValidationError("All routes were pruned from the config.")

    for route in surviving_routes:
        workflow_prune_candidates.discard(route.workflow_name)
    config.routes = surviving_routes

    return workflow_prune_candidates


def _prune_workflows(
    config: ValidatedConfig, workflow_prune_candidates: set[str]
) -> None:
    """Prune workflows not referenced by any surviving route."""
    config.workflows = [
        workflow
        for workflow in config.workflows
        if workflow.name not in workflow_prune_candidates
    ]
    for name in workflow_prune_candidates:
        log.warning("Pruned orphaned workflow: %s", name)
    if not config.workflows:
        raise ConfigValidationError("All workflows were pruned from the config.")
