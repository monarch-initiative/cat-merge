import pytest
from typing import List, Dict

from tests.fixtures.edges import *
from tests.fixtures.nodes import *
from tests.test_utils import check_report_data
from cat_merge.qc_utils import create_predicate_report


@pytest.fixture
def predicate_report_dict_expected() -> List:
    predicate_expected = [
        {'biolink:has_mode_of_inheritance':
            {'uri': 'biolink:has_mode_of_inheritance', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
             'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
        {'biolink:has_phenotype':
            {'uri': 'biolink:has_phenotype', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
             'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
        {'biolink:gene_associated_with_condition':
            {'uri': 'biolink:gene_associated_with_condition', 'total_number': 2, 'missing_subjects': 0,
             'missing_objects': 0, 'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
        {'biolink:orthologous_to':
            {'uri': 'biolink:orthologous_to', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
             'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
        {'biolink:subclass_of':
            {'uri': 'biolink:subclass_of', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
             'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
        {'biolink:participates_in':
            {'uri': 'biolink:participates_in', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
             'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
        {'biolink:interacts_with':
            {'uri': 'biolink:interacts_with', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
             'missing_subject_namespaces': [], 'missing_object_namespaces': []}}]
    return predicate_expected


@pytest.fixture
def predicate_report_list_expected() -> Dict:
    predicate_expected = [
        [{'uri': 'biolink:has_mode_of_inheritance', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
         'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
        [{'uri': 'biolink:has_phenotype', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
         'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
        [{'uri': 'biolink:gene_associated_with_condition', 'total_number': 2, 'missing_subjects': 0,
         'missing_objects': 0, 'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
        [{'uri': 'biolink:orthologous_to', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
         'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
        [{'uri': 'biolink:subclass_of', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
         'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
        [{'uri': 'biolink:participates_in', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
         'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
        [{'uri': 'biolink:interacts_with', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
         'missing_subject_namespaces': [], 'missing_object_namespaces': []}]]
    return predicate_expected


def test_create_predicate_report_defaults(kg_report_edges_1, kg_report_nodes_1, predicate_report_list_expected):
    # TODO: Long-term group_by will be generated test-set like 'flags'
    edges_group = kg_report_edges_1.groupby(["provided_by"])[['id', 'object', 'subject', 'predicate', 'category']]
    predicate_reports = []
    for edges_grouped_by, edges_grouped_by_values in edges_group:
        predicate_report = create_predicate_report(edges_grouped_by_values, kg_report_nodes_1["id"])
        assert type(predicate_report) is list
        assert len(predicate_report) == 1
        assert len(predicate_report[0]) == 6
        predicate_reports.append(predicate_report)

    check_report_data(predicate_reports, predicate_report_list_expected)


def test_create_predicate_report_list(kg_report_edges_1, kg_report_nodes_1, predicate_report_list_expected):
    # TODO: Long-term group_by will be generated test-set like 'flags'
    edges_group = kg_report_edges_1.groupby(["provided_by"])[['id', 'object', 'subject', 'predicate', 'category']]
    predicate_reports = []
    for edges_grouped_by, edges_grouped_by_values in edges_group:
        predicate_report = create_predicate_report(edges_grouped_by_values, kg_report_nodes_1["id"], data_type=list)
        assert type(predicate_report) is list
        assert len(predicate_report) == 1
        assert len(predicate_report[0]) == 6
        predicate_reports.append(predicate_report)

    check_report_data(predicate_reports, predicate_report_list_expected)


def test_create_predicate_report_dict(kg_report_edges_1, kg_report_nodes_1, predicate_report_dict_expected):
    # TODO: Long-term group_by will be generated test-set like 'flags'
    edges_group = kg_report_edges_1.groupby(["provided_by"])[['id', 'object', 'subject', 'predicate', 'category']]
    predicate_reports = []
    for edges_grouped_by, edges_grouped_by_values in edges_group:
        predicate_report = create_predicate_report(edges_grouped_by_values, kg_report_nodes_1["id"], data_type=dict)
        assert type(predicate_report) is dict
        assert len(predicate_report) == 1
        # assert len(predicate_report[0]) == 6
        predicate_reports.append(predicate_report)

    check_report_data(predicate_reports, predicate_report_dict_expected)
