import pytest
import yaml
from typing import List, Dict

from tests.fixtures.nodes import *
from tests.fixtures.edges import *
from tests.test_utils import check_report_data
from cat_merge.qc_utils import create_edges_report


@pytest.fixture
def edges_report_expected_list() -> List[Dict]:
    edges_report_expected_list_yaml = "tests/test_data/expected/test_create_edges_report_expected_list.yaml"
    with open(edges_report_expected_list_yaml, "r") as report_file:
        report_values = yaml.safe_load(report_file)
    return report_values


@pytest.fixture
def edges_report_expected_dict() -> Dict:
    edges_report_expected_list_yaml = "tests/test_data/expected/test_create_edges_report_expected_dict.yaml"
    with open(edges_report_expected_list_yaml, "r") as report_file:
        report_values = yaml.safe_load(report_file)
    return report_values


def test_create_edges_report_defaults(kg_report_edges_1, kg_report_nodes_1, edges_report_expected_dict):
    test_report = create_edges_report(kg_report_edges_1, kg_report_nodes_1, data_type=dict)

    assert type(test_report) is dict
    assert len(test_report) == 7
    check_report_data(test_report, edges_report_expected_dict)


def test_create_edges_report_list(kg_report_edges_1, kg_report_nodes_1, edges_report_expected_list):
    test_report = create_edges_report(kg_report_edges_1, kg_report_nodes_1, data_type=list)

    assert type(test_report) is list
    assert len(test_report) == 7
    check_report_data(test_report, edges_report_expected_list)


def test_create_edges_report_dict(kg_report_edges_1, kg_report_nodes_1, edges_report_expected_dict):
    test_report = create_edges_report(kg_report_edges_1, kg_report_nodes_1, data_type=dict)

    assert type(test_report) is dict
    assert len(test_report) == 7
    check_report_data(test_report, edges_report_expected_dict)
