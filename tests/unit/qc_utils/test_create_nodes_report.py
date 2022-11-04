import pytest
from numpy import nan
from tests.fixtures.nodes import *
from cat_merge.qc_utils import create_nodes_report


def test_create_nodes_report_defaults(kg_report_nodes_1):
    test_nodes_report = create_nodes_report(kg_report_nodes_1)

    assert len(test_nodes_report) == 4
    report_names = [i['name'] for i in test_nodes_report]
    assert report_names == ['hgnc_gene_nodes', 'ncbi_gene_nodes', 'phenio_nodes', 'reactome_pathway_nodes']
    assert test_nodes_report[0] == {'name': 'hgnc_gene_nodes', 'namespaces': ['HGNC'], 'categories': ['biolink:Gene'],
                                    'total_number': 5, 'taxon': ['NCBITaxon:9606']}
    assert test_nodes_report[1] == {'name': 'ncbi_gene_nodes', 'namespaces': ['NCBIGene'],
                                    'categories': ['biolink:Gene'], 'total_number': 2,
                                    'taxon': ['NCBITaxon:9823', 'NCBITaxon:9913']}
    assert test_nodes_report[2] == {'name': 'phenio_nodes', 'namespaces': ['HP', 'MONDO'],
                                    'categories': ['biolink:Disease', 'biolink:PhenotypicFeature'],
                                    'total_number': 10, 'taxon': ['missing taxon']}
    assert test_nodes_report[3] == {'name': 'reactome_pathway_nodes', 'namespaces': ['REACT'],
                                    'categories': ['biolink:Pathway'], 'total_number': 2, 'taxon': ['NCBITaxon:9606']}
