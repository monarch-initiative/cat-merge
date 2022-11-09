import pytest
from typing import List, Dict

from tests.fixtures.edges import *
from tests.fixtures.nodes import *
from tests.test_utils import check_report_data
from cat_merge.qc_utils import create_edge_report


@pytest.fixture
def edge_report_expected() -> Dict:
    edge_expected = {
        'hpoa_disease_mode_of_inheritance_edges': {
            'name': 'hpoa_disease_mode_of_inheritance_edges', 'namespaces': ['HP', 'MONDO'],
            'categories': ['biolink:DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation'], 'total_number': 2,
            'missing_old': 0, 'missing': 0, 'predicates': [], 'node_types': []},
        'hpoa_disease_phenotype_edges': {
            'name': 'hpoa_disease_phenotype_edges', 'namespaces': ['HP', 'MONDO'],
            'categories': ['biolink:DiseaseToPhenotypicFeatureAssociation'], 'total_number': 2, 'missing_old': 0,
            'missing': 0, 'predicates': [], 'node_types': []},
        'omim_gene_to_disease_edges': {
            'name': 'omim_gene_to_disease_edges', 'namespaces': ['HGNC', 'MONDO'],
            'categories': ['biolink:GeneToDiseaseAssociation'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
            'predicates': [], 'node_types': []},
        'panther_genome_orthologs_edges': {
            'name': 'panther_genome_orthologs_edges', 'namespaces': ['HGNC', 'NCBIGene'],
            'categories': ['biolink:GeneToGeneHomologyAssociation'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
            'predicates': [], 'node_types': []},
        'phenio_edges': {
            'name': 'phenio_edges', 'namespaces': ['MONDO'], 'categories': ['missing category'], 'total_number': 2,
            'missing_old': 0, 'missing': 0, 'predicates': [], 'node_types': []},
        'reactome_gene_to_pathway_edges': {
            'name': 'reactome_gene_to_pathway_edges', 'namespaces': ['HGNC', 'REACT'],
            'categories': ['biolink:GeneToPathwayAssociation'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
            'predicates': [], 'node_types': []},
        'string_protein_links_edges': {
            'name': 'string_protein_links_edges', 'namespaces': ['HGNC'],
            'categories': ['biolink:PairwiseGeneToGeneInteraction'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
            'predicates': [], 'node_types': []}}
    return edge_expected


def test_create_edge_report(kg_report_edges_1, kg_report_nodes_1, edge_report_expected):
    # TODO: Long-term group_by will be generated test-set like 'flags'
    edges_group = kg_report_edges_1.groupby(["provided_by"])[['id', 'object', 'subject', 'predicate', 'category']]
    edge_report_keys = []
    edge_report_values = []
    for edges_grouped_by, edges_grouped_by_values in edges_group:
        edge_report = create_edge_report(edges_grouped_by, edges_grouped_by_values, kg_report_nodes_1["id"])
        assert type(edge_report) is dict
        assert len(edge_report) == 8
        edge_report_keys.append(edges_grouped_by)
        edge_report_values.append(edge_report)

    check_report_data(edge_report_keys, edge_report_expected.keys())
    check_report_data(edge_report_values, edge_report_expected.values())
