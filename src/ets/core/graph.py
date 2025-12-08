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

"""Directed graph validation and determination of topological order.

Note that the graphlib module of the standard library already provides a
similar functionality, validating that the graph does not have cycles, but
it does not check for the stronger property of unique paths between nodes.
"""

from collections import defaultdict
from collections.abc import Hashable, Mapping, Sequence
from typing import TypeVar

__all__ = [
    "CyclicGraphError",
    "InvalidGraphError",
    "NonUniquePathError",
    "get_topological_order",
]


# Nodes can be any hashable type
N = TypeVar("N", bound=Hashable)


class InvalidGraphError(ValueError):
    """Error indicating that the graph is invalid in some way."""


class NonUniquePathError(InvalidGraphError):
    """Error indicating that a non-unique path was detected in the graph."""


class CyclicGraphError(NonUniquePathError):
    """Error indicating that a cycle was detected in the graph."""


def get_topological_order(edges: list[tuple[N, N]]) -> dict[N, int]:
    """Validate a directed graph and get its topological order.

    The graph must be specified as a list of directed edges
    that are tuples of any hashable objects (the nodes).
    Isolated nodes are not covered by this function,
    since they can be ordered arbitrarily anyway.

    The function checks that that any path connecting two nodes
    is always unique, which implies that the graph is acyclic,
    but is a stronger property (allowing no diamond shapes).

    Raises CyclicGraphError if a cycle is detected.
    Raises NonUniquePathError if a non-unique path is detected.

    Returns a mapping of each node to its topological order index.
    """
    # extract nodes and build adjacency list and in-degrees
    nodes = _get_nodes_from_edges(edges)
    adj = _get_adjacency_list(edges)
    in_degrees = _get_in_degrees(nodes, adj)

    # perform Kahn's algorithm with unique path check
    result = _kahn_algorithm(in_degrees, adj)

    # check if there are still edges left
    if any(in_degrees.values()):
        raise CyclicGraphError("A cycle was detected in the graph.")

    # return the topological order as a mapping
    return {node: i for i, node in enumerate(result)}


def _get_nodes_from_edges(edges: list[tuple[N, N]]) -> tuple[N, ...]:
    """Get all nodes from the list of edges."""
    # use a dict to deduplicate while preserving order
    nodes: dict[N, None] = {}
    for from_node, next_node in edges:
        nodes[from_node] = None
        nodes[next_node] = None
    return tuple(nodes)


def _get_adjacency_list(edges: list[tuple[N, N]]) -> Mapping[N, list[N]]:
    """Get the adjacency list from the list of edges."""
    adj: Mapping[N, list[N]] = defaultdict(list)
    for from_node, next_node in edges:
        adj[from_node].append(next_node)
    return adj


def _get_in_degrees(nodes: Sequence[N], adj: Mapping[N, list[N]]) -> dict[N, int]:
    """Get the in-degrees of all nodes in the graph."""
    in_degrees: dict[N, int] = defaultdict(int)
    for current_node in nodes:
        for next_node in adj[current_node]:
            in_degrees[next_node] += 1
    return in_degrees


def _kahn_algorithm(in_degrees: dict[N, int], adj: Mapping[N, list[N]]) -> list[N]:
    """Run Kahn's algorithm for topological ordering with unique path check.

    This is a modified version of Kahn's algorithm that checks the unique path property
    of the graph along the way and throws an error if it is violated.
    """
    ancestors: dict[N, set[N]] = {
        node: set() if in_degrees[node] else {node} for node in adj
    }

    result: list[N] = []
    queue = [node for node in adj if not in_degrees[node]]
    pop = queue.pop
    push = queue.append
    append = result.append

    while queue:
        current_node = pop()
        ancestors_of_current = ancestors[current_node]
        append(current_node)
        for next_node in adj[current_node]:
            ancestors_of_next = ancestors[next_node]
            in_degrees[next_node] -= 1
            if ancestors_of_next.intersection(ancestors_of_current):
                raise NonUniquePathError(f"Multiple paths lead to node {next_node}")
            ancestors_of_next.update(ancestors_of_current)
            if not in_degrees[next_node]:
                push(next_node)

    return result
