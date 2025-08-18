import pytest
import yaml
from typing import List, Dict

from tests.fixtures.edges import *
from tests.fixtures.nodes import *
from tests.test_utils import check_report_data
from cat_merge.qc_utils import create_edge_report


@pytest.fixture
def edge_report_expected() -> Dict:
    test_create_edge_report_expected_yaml = "tests/test_data/expected/test_create_edge_report_expected.yaml"
    with open(test_create_edge_report_expected_yaml, "r") as report_file:
        expected = yaml.safe_load(report_file)
    return expected


def test_create_edge_report(kg_report_edges_1, kg_report_nodes_1, edge_report_expected):
    # TODO: Long-term group_by will be generated test-set like 'flags'
    edges_group = kg_report_edges_1.groupby(["provided_by"])[['id', 'object', 'subject', 'predicate', 'category']]
    edge_report_keys = []
    edge_report_values = []
    for edges_grouped_by, edges_grouped_by_values in edges_group:
        edge_report = create_edge_report(edges_grouped_by, edges_grouped_by_values, kg_report_nodes_1["id"])
        assert type(edge_report) is dict
        assert len(edge_report) == 8
        # Fix groupby tuple issue - extract string from tuple if needed
        key = edges_grouped_by[0] if isinstance(edges_grouped_by, tuple) else edges_grouped_by
        edge_report_keys.append(key)
        edge_report_values.append(edge_report)

    check_report_data(edge_report_keys, edge_report_expected.keys())
    check_report_data(edge_report_values, edge_report_expected.values())
