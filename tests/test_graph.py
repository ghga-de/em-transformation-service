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

"""Test directed graph validation and topological order."""

import pytest

from ets.core.graph import CyclicGraphError, NonUniquePathError, get_topological_order

# For the test cases, graphs are represented as strings,
# where the edges are given as pairs of letters (from, to).

CYCLIC_GRAPHS = [
    "AB BA",
    "AB BC CA",
    "AB BC CB",
    "AB BC CD DA",
    "AB BC CD CB",
    "AB BC CD DB",
    "AB BC CD CE FG GH HJ HI IF",
]

ACYCLIC_GRAPHS_WITH_UNIQUE_PATHS = [
    "AB",
    "AC BC",
    "BD CD",
    "AC BC DF EF",
    "AB BC DC DE",
    "AB CD EF",
    "AB BC CD EF FG GD DH HI HJ IK JL",
]

ACYCLIC_GRAPHS_WITH_MULTIPLE_PATHS = [
    "AB BC AC",
    "AB AC BD BC",
    "AB BC BD CE DE",
    "AB AC BD BE CE CF",
    "AB AC BD CE CF EG FG",
    "AB BC CD EF FG FB FC",
    "AB BC CD EF FB FC",
    "AD BD BE CE CH DF DF DH",
    "AB CD CE DF EF FG",
    "AB BC CD EF FG GD DH HI HJ IK JL KM ML MN",
]


def edges_from_string(s: str) -> list[tuple[str, str]]:
    """Get a list of edges from a string representation."""
    return [(s[0], s[1]) for s in s.split()]


def assert_is_topological_order(
    edges: list[tuple[str, str]], order: dict[str, int]
) -> None:
    """Assert that the specified order is a valid topological order."""
    # test that the order indices are consecutive numbers starting from 0
    for i, node in enumerate(order):
        assert order[node] == i
    # test that this is actually a topological order
    for edge in edges:
        from_node, to_node = edge
        assert from_node in order
        assert to_node in order
        assert order[from_node] < order[to_node]


def test_validates_simple_graph():
    """Test that the order of a simple valid graph is returned correctly."""
    edges = [("A", "B"), ("B", "C")]
    assert get_topological_order(edges) == {"A": 0, "B": 1, "C": 2}


def test_detects_duplicate_edges():
    """Test that duplicate edges are detected."""
    edges = [("A", "B"), ("A", "B")]
    with pytest.raises(NonUniquePathError):
        get_topological_order(edges)


@pytest.mark.parametrize("edge_string", CYCLIC_GRAPHS)
def test_cyclic_graph(edge_string: str):
    """Test that cyclic graphs are detected."""
    edges = edges_from_string(edge_string)
    with pytest.raises(CyclicGraphError):
        get_topological_order(edges)


@pytest.mark.parametrize("edge_string", ACYCLIC_GRAPHS_WITH_UNIQUE_PATHS)
def test_acyclic_graph_with_unique_paths(edge_string: str):
    """Test that acyclic graphs with unique path property are validated properly.

    Also tests that the returned topological order is correct.
    """
    edges = edges_from_string(edge_string)
    order = get_topological_order(edges)
    assert_is_topological_order(edges, order)


@pytest.mark.parametrize("edge_string", ACYCLIC_GRAPHS_WITH_MULTIPLE_PATHS)
def test_acyclic_graph_with_multiple_paths(edge_string: str):
    """Test that acyclic graphs without unique path property are detected."""
    edges = edges_from_string(edge_string)
    with pytest.raises(NonUniquePathError):
        get_topological_order(edges)
