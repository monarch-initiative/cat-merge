import pytest
from typing import List, Dict

from tests.fixtures.nodes import *
from tests.test_utils import check_report_data
from cat_merge.qc_utils import create_nodes_report


@pytest.fixture
def nodes_report_expected() -> List[Dict]:
    report_values = [{'name': 'hgnc_gene_nodes', 'namespaces': ['HGNC'], 'categories': ['biolink:Gene'],
                      'total_number': 5, 'taxon': ['NCBITaxon:9606']},
                     {'name': 'ncbi_gene_nodes', 'namespaces': ['NCBIGene'], 'categories': ['biolink:Gene'],
                      'total_number': 2, 'taxon': ['NCBITaxon:9823', 'NCBITaxon:9913']},
                     {'name': 'phenio_nodes', 'namespaces': ['HP', 'MONDO'],
                      'categories': ['biolink:Disease','biolink:PhenotypicFeature'], 'total_number': 10,
                      'taxon': ['missing taxon']},
                     {'name': 'reactome_pathway_nodes', 'namespaces': ['REACT'],
                      'categories': ['biolink:Pathway'], 'total_number': 2, 'taxon': ['NCBITaxon:9606']}]
    return report_values


def test_create_nodes_report_defaults(kg_report_nodes_1, nodes_report_expected):
    nodes_report = create_nodes_report(kg_report_nodes_1)

    assert type(nodes_report) is list
    assert len(nodes_report) == 4
    check_report_data(nodes_report, nodes_report_expected)


def test_create_nodes_report_list(kg_report_nodes_1, nodes_report_expected):
    nodes_report = create_nodes_report(kg_report_nodes_1, data_type=list)

    assert type(nodes_report) is list
    assert len(nodes_report) == 4
    check_report_data(nodes_report, nodes_report_expected)


def test_create_nodes_report_dict(kg_report_nodes_1, nodes_report_expected):
    nodes_report = create_nodes_report(kg_report_nodes_1, data_type=dict)

    assert type(nodes_report) is dict
    assert len(nodes_report) == 4
    check_report_data(nodes_report.values(), nodes_report_expected)
