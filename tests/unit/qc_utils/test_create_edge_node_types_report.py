import pytest
from typing import List, Dict

from tests.fixtures.edges import *
from tests.fixtures.nodes import *
from tests.test_utils import check_report_data
from cat_merge.qc_utils import create_edge_node_types_report


@pytest.fixture
def edge_node_types_report_expected_dict() -> List:
    predicate_expected = [
        {'phenio_nodes': {'name': 'phenio_nodes', 'categories': ['biolink:Disease', 'biolink:PhenotypicFeature'],
                          'namespaces': ['HP', 'MONDO'], 'total_number': 4, 'missing': 4, 'taxon': ['missing taxon']}},
        {'phenio_nodes': {'name': 'phenio_nodes', 'categories': ['biolink:Disease', 'biolink:PhenotypicFeature'],
                          'namespaces': ['HP', 'MONDO'], 'total_number': 3, 'missing': 3, 'taxon': ['missing taxon']}},
        {'hgnc_gene_nodes': {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'],
                             'total_number': 2, 'missing': 2, 'taxon': ['NCBITaxon:9606']},
         'phenio_nodes': {'name': 'phenio_nodes', 'categories': ['biolink:Disease'], 'namespaces': ['MONDO'],
                          'total_number': 2, 'missing': 2, 'taxon': ['missing taxon']}},
        {'hgnc_gene_nodes': {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'],
                             'total_number': 1, 'missing': 1, 'taxon': ['NCBITaxon:9606']},
         'ncbi_gene_nodes': {'name': 'ncbi_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['NCBIGene'],
                             'total_number': 2, 'missing': 2, 'taxon': ['NCBITaxon:9823', 'NCBITaxon:9913']}},
        {'phenio_nodes': {'name': 'phenio_nodes', 'categories': ['biolink:Disease'], 'namespaces': ['MONDO'],
                          'total_number': 3, 'missing': 3, 'taxon': ['missing taxon']}},
        {'hgnc_gene_nodes': {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'],
                             'total_number': 2, 'missing': 2, 'taxon': ['NCBITaxon:9606']},
         'reactome_pathway_nodes': {'name': 'reactome_pathway_nodes', 'categories': ['biolink:Pathway'],
                                    'namespaces': ['REACT'], 'total_number': 2, 'missing': 2,
                                    'taxon': ['NCBITaxon:9606']}},
        {'hgnc_gene_nodes': {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'],
                             'total_number': 3, 'missing': 3, 'taxon': ['NCBITaxon:9606']}}]
    return predicate_expected


@pytest.fixture
def edge_node_types_report_expected_list() -> List:
    reports_expected = [
        [{'name': 'phenio_nodes', 'categories': ['biolink:Disease', 'biolink:PhenotypicFeature'],
          'namespaces': ['HP', 'MONDO'], 'total_number': 4, 'missing': 4, 'taxon': ['missing taxon']}],
        [{'name': 'phenio_nodes', 'categories': ['biolink:Disease', 'biolink:PhenotypicFeature'],
          'namespaces': ['HP', 'MONDO'], 'total_number': 3, 'missing': 3, 'taxon': ['missing taxon']}],
        [{'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'], 'total_number': 2,
          'missing': 2, 'taxon': ['NCBITaxon:9606']},
         {'name': 'phenio_nodes', 'categories': ['biolink:Disease'], 'namespaces': ['MONDO'], 'total_number': 2,
          'missing': 2, 'taxon': ['missing taxon']}],
        [{'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'], 'total_number': 1,
          'missing': 1, 'taxon': ['NCBITaxon:9606']},
         {'name': 'ncbi_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['NCBIGene'], 'total_number': 2,
          'missing': 2, 'taxon': ['NCBITaxon:9823', 'NCBITaxon:9913']}],
        [{'name': 'phenio_nodes', 'categories': ['biolink:Disease'], 'namespaces': ['MONDO'], 'total_number': 3,
          'missing': 3, 'taxon': ['missing taxon']}],
        [{'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'], 'total_number': 2,
          'missing': 2, 'taxon': ['NCBITaxon:9606']},
         {'name': 'reactome_pathway_nodes', 'categories': ['biolink:Pathway'], 'namespaces': ['REACT'],
          'total_number': 2, 'missing': 2, 'taxon': ['NCBITaxon:9606']}],
        [{'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'], 'total_number': 3,
          'missing': 3, 'taxon': ['NCBITaxon:9606']}]]
    return reports_expected


def test_create_edge_node_types_report_defaults(
        kg_report_edges_1, kg_report_nodes_1, edge_node_types_report_expected_list):
    # TODO: Long-term group_by will be generated test-set like 'flags'
    edges_group = kg_report_edges_1.groupby(["provided_by"])[['id', 'object', 'subject', 'predicate', 'category']]
    all_reports = []
    for edges_grouped_by, edges_grouped_by_values in edges_group:
        report = create_edge_node_types_report(edges_grouped_by_values, kg_report_nodes_1)
        assert type(report) is list
        all_reports.append(report)

    check_report_data(all_reports, edge_node_types_report_expected_list)


def test_create_edge_node_types_report_list(
        kg_report_edges_1, kg_report_nodes_1, edge_node_types_report_expected_list):
    # TODO: Long-term group_by will be generated test-set like 'flags'
    edges_group = kg_report_edges_1.groupby(["provided_by"])[['id', 'object', 'subject', 'predicate', 'category']]
    all_reports = []
    for edges_grouped_by, edges_grouped_by_values in edges_group:
        report = create_edge_node_types_report(edges_grouped_by_values, kg_report_nodes_1, data_type=list)
        assert type(report) is list
        all_reports.append(report)

    check_report_data(all_reports, edge_node_types_report_expected_list)


def test_create_edge_node_types_report_dict(
        kg_report_edges_1, kg_report_nodes_1, edge_node_types_report_expected_dict):
    # TODO: Long-term group_by will be generated test-set like 'flags'
    edges_group = kg_report_edges_1.groupby(["provided_by"])[['id', 'object', 'subject', 'predicate', 'category']]
    all_reports = []
    for edges_grouped_by, edges_grouped_by_values in edges_group:
        report = create_edge_node_types_report(edges_grouped_by_values, kg_report_nodes_1, data_type=dict)
        assert type(report) is dict
        all_reports.append(report)

    check_report_data(all_reports, edge_node_types_report_expected_dict)
