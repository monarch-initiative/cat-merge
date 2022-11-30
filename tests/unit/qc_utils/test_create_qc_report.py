import pytest
import yaml
from typing import Dict
from tests.test_utils import check_report_data
from cat_merge.qc_utils import create_qc_report


@pytest.fixture
def qc_report_expected_list() -> Dict:
    qc_report_expected_list_yaml = "tests/test_data/expected/test_create_qc_report_expected_list.yaml"
    with open(qc_report_expected_list_yaml, "r") as report_file:
        report_values = yaml.safe_load(report_file)
    return report_values


@pytest.fixture
def qc_report_expected_dict() -> Dict:
    qc_report_expected_dict_yaml = "tests/test_data/expected/test_create_qc_report_expected_dict.yaml"
    with open(qc_report_expected_dict_yaml, "r") as report_file:
        report_values = yaml.safe_load(report_file)
    return report_values


def test_create_qc_report_defaults(kg_1, qc_report_expected_dict):
    test_report = create_qc_report(kg_1)

    assert type(test_report) is dict
    assert len(test_report) == 4
    check_report_data(test_report.keys(), qc_report_expected_dict.keys())
    check_report_data(test_report.values(), qc_report_expected_dict.values())


def test_create_qc_report_list(kg_1, qc_report_expected_list):
    test_report = create_qc_report(kg_1, data_type=list)

    assert type(test_report) is dict
    assert len(test_report) == 4
    check_report_data(test_report.keys(), qc_report_expected_list.keys())
    check_report_data(test_report.values(), qc_report_expected_list.values())


def test_create_qc_report_dict(kg_1, qc_report_expected_dict):
    test_report = create_qc_report(kg_1, data_type=dict)

    assert type(test_report) is dict
    assert len(test_report) == 4
    check_report_data(test_report.keys(), qc_report_expected_dict.keys())
    check_report_data(test_report.values(), qc_report_expected_dict.values())
