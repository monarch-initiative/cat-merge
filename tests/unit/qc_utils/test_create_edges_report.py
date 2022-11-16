import pytest
from typing import List, Dict

from tests.fixtures.nodes import *
from tests.fixtures.edges import *
from tests.test_utils import check_report_data
from cat_merge.qc_utils import create_edges_report


@pytest.fixture
def edges_report_expected_list() -> List[Dict]:
    report_values = [
        {'name': 'hpoa_disease_mode_of_inheritance_edges', 'namespaces': ['HP', 'MONDO'],
         'categories': ['biolink:DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation'], 'total_number': 2,
         'missing_old': 0, 'missing': 0,
         'predicates': [
             {'uri': 'biolink:has_mode_of_inheritance', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
              'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
         'node_types': [
             {'name': 'phenio_nodes', 'categories': ['biolink:Disease', 'biolink:PhenotypicFeature'],
              'namespaces': ['HP', 'MONDO'], 'total_number': 4, 'missing': 4, 'taxon': ['missing taxon']}]},
        {'name': 'hpoa_disease_phenotype_edges', 'namespaces': ['HP', 'MONDO'],
         'categories': ['biolink:DiseaseToPhenotypicFeatureAssociation'], 'total_number': 2, 'missing_old': 0,
         'missing': 0,
         'predicates': [
            {'uri': 'biolink:has_phenotype', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
             'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
         'node_types': [
             {'name': 'phenio_nodes', 'categories': ['biolink:Disease', 'biolink:PhenotypicFeature'],
              'namespaces': ['HP', 'MONDO'], 'total_number': 3, 'missing': 3, 'taxon': ['missing taxon']}]},
        {'name': 'omim_gene_to_disease_edges', 'namespaces': ['HGNC', 'MONDO'],
         'categories': ['biolink:GeneToDiseaseAssociation'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
         'predicates': [
             {'uri': 'biolink:gene_associated_with_condition', 'total_number': 2, 'missing_subjects': 0,
              'missing_objects': 0, 'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
         'node_types': [
             {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'], 'total_number': 2,
              'missing': 2, 'taxon': ['NCBITaxon:9606']},
             {'name': 'phenio_nodes', 'categories': ['biolink:Disease'], 'namespaces': ['MONDO'], 'total_number': 2,
              'missing': 2, 'taxon': ['missing taxon']}]},
        {'name': 'panther_genome_orthologs_edges', 'namespaces': ['HGNC', 'NCBIGene'],
         'categories': ['biolink:GeneToGeneHomologyAssociation'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
         'predicates': [
             {'uri': 'biolink:orthologous_to', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
              'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
         'node_types': [
             {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'],
                         'total_number': 1, 'missing': 1, 'taxon': ['NCBITaxon:9606']},
             {'name': 'ncbi_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['NCBIGene'], 'total_number': 2,
              'missing': 2, 'taxon': ['NCBITaxon:9823', 'NCBITaxon:9913']}]},
        {'name': 'phenio_edges', 'namespaces': ['MONDO'], 'categories': ['missing category'], 'total_number': 2,
         'missing_old': 0, 'missing': 0,
         'predicates': [
             {'uri': 'biolink:subclass_of', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
              'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
         'node_types': [
             {'name': 'phenio_nodes', 'categories': ['biolink:Disease'], 'namespaces': ['MONDO'], 'total_number': 3,
              'missing': 3, 'taxon': ['missing taxon']}]},
        {'name': 'reactome_gene_to_pathway_edges', 'namespaces': ['HGNC', 'REACT'],
         'categories': ['biolink:GeneToPathwayAssociation'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
         'predicates': [
             {'uri': 'biolink:participates_in', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
              'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
         'node_types': [
             {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'], 'total_number': 2,
              'missing': 2, 'taxon': ['NCBITaxon:9606']},
             {'name': 'reactome_pathway_nodes', 'categories': ['biolink:Pathway'], 'namespaces': ['REACT'],
              'total_number': 2, 'missing': 2, 'taxon': ['NCBITaxon:9606']}]},
        {'name': 'string_protein_links_edges', 'namespaces': ['HGNC'],
         'categories': ['biolink:PairwiseGeneToGeneInteraction'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
         'predicates': [
             {'uri': 'biolink:interacts_with', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
              'missing_subject_namespaces': [], 'missing_object_namespaces': []}],
         'node_types': [
             {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'], 'total_number': 3,
              'missing': 3, 'taxon': ['NCBITaxon:9606']}]}]
    return report_values


@pytest.fixture
def edges_report_expected_dict() -> Dict:
    report_values = {
        'hpoa_disease_mode_of_inheritance_edges':
            {'name': 'hpoa_disease_mode_of_inheritance_edges', 'namespaces': ['HP', 'MONDO'],
             'categories': ['biolink:DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation'],
             'total_number': 2, 'missing_old': 0, 'missing': 0,
             'predicates': {
                 'biolink:has_mode_of_inheritance':
                     {'uri': 'biolink:has_mode_of_inheritance', 'total_number': 2, 'missing_subjects': 0,
                      'missing_objects': 0, 'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
             'node_types': {
                 'phenio_nodes':
                     {'name': 'phenio_nodes', 'categories': ['biolink:Disease', 'biolink:PhenotypicFeature'],
                      'namespaces': ['HP', 'MONDO'], 'total_number': 4, 'missing': 4, 'taxon': ['missing taxon']}}},
        'hpoa_disease_phenotype_edges':
            {'name': 'hpoa_disease_phenotype_edges', 'namespaces': ['HP', 'MONDO'],
             'categories': ['biolink:DiseaseToPhenotypicFeatureAssociation'],
             'total_number': 2, 'missing_old': 0, 'missing': 0,
             'predicates': {
                 'biolink:has_phenotype':
                     {'uri': 'biolink:has_phenotype', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
                      'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
             'node_types': {
                 'phenio_nodes':
                     {'name': 'phenio_nodes', 'categories': ['biolink:Disease', 'biolink:PhenotypicFeature'],
                      'namespaces': ['HP', 'MONDO'], 'total_number': 3, 'missing': 3, 'taxon': ['missing taxon']}}},
        'omim_gene_to_disease_edges':
            {'name': 'omim_gene_to_disease_edges', 'namespaces': ['HGNC', 'MONDO'],
             'categories': ['biolink:GeneToDiseaseAssociation'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
             'predicates': {
                 'biolink:gene_associated_with_condition':
                     {'uri': 'biolink:gene_associated_with_condition', 'total_number': 2, 'missing_subjects': 0,
                      'missing_objects': 0, 'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
             'node_types': {
                 'hgnc_gene_nodes':
                     {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'],
                      'total_number': 2, 'missing': 2, 'taxon': ['NCBITaxon:9606']},
                 'phenio_nodes':
                     {'name': 'phenio_nodes', 'categories': ['biolink:Disease'], 'namespaces': ['MONDO'],
                      'total_number': 2, 'missing': 2, 'taxon': ['missing taxon']}}},
        'panther_genome_orthologs_edges':
            {'name': 'panther_genome_orthologs_edges', 'namespaces': ['HGNC', 'NCBIGene'],
             'categories': ['biolink:GeneToGeneHomologyAssociation'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
             'predicates': {
                 'biolink:orthologous_to':
                     {'uri': 'biolink:orthologous_to', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
                      'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
             'node_types': {
                 'hgnc_gene_nodes':
                     {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'],
                      'total_number': 1, 'missing': 1, 'taxon': ['NCBITaxon:9606']},
                 'ncbi_gene_nodes':
                     {'name': 'ncbi_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['NCBIGene'],
                      'total_number': 2, 'missing': 2, 'taxon': ['NCBITaxon:9823', 'NCBITaxon:9913']}}},
        'phenio_edges':
            {'name': 'phenio_edges', 'namespaces': ['MONDO'], 'categories': ['missing category'],
             'total_number': 2, 'missing_old': 0, 'missing': 0,
             'predicates': {
                 'biolink:subclass_of':
                     {'uri': 'biolink:subclass_of', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
                      'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
             'node_types': {
                 'phenio_nodes':
                     {'name': 'phenio_nodes', 'categories': ['biolink:Disease'], 'namespaces': ['MONDO'],
                      'total_number': 3, 'missing': 3, 'taxon': ['missing taxon']}}},
        'reactome_gene_to_pathway_edges':
            {'name': 'reactome_gene_to_pathway_edges', 'namespaces': ['HGNC', 'REACT'],
             'categories': ['biolink:GeneToPathwayAssociation'], 'total_number': 2, 'missing_old': 0, 'missing': 0,
             'predicates': {
                 'biolink:participates_in':
                     {'uri': 'biolink:participates_in', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
                      'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
             'node_types': {
                 'hgnc_gene_nodes':
                     {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'],
                      'total_number': 2, 'missing': 2, 'taxon': ['NCBITaxon:9606']},
                 'reactome_pathway_nodes':
                     {'name': 'reactome_pathway_nodes', 'categories': ['biolink:Pathway'], 'namespaces': ['REACT'],
                      'total_number': 2, 'missing': 2, 'taxon': ['NCBITaxon:9606']}}},
        'string_protein_links_edges':
            {'name': 'string_protein_links_edges', 'namespaces': ['HGNC'],
             'categories': ['biolink:PairwiseGeneToGeneInteraction'],
             'total_number': 2, 'missing_old': 0, 'missing': 0,
             'predicates': {
                 'biolink:interacts_with':
                     {'uri': 'biolink:interacts_with', 'total_number': 2, 'missing_subjects': 0, 'missing_objects': 0,
                      'missing_subject_namespaces': [], 'missing_object_namespaces': []}},
             'node_types': {
                 'hgnc_gene_nodes':
                     {'name': 'hgnc_gene_nodes', 'categories': ['biolink:Gene'], 'namespaces': ['HGNC'],
                      'total_number': 3, 'missing': 3, 'taxon': ['NCBITaxon:9606']}}}}
    return report_values


def test_create_nodes_report_defaults(kg_report_edges_1, kg_report_nodes_1, edges_report_expected_list):
    test_report = create_edges_report(kg_report_edges_1, kg_report_nodes_1)

    assert type(test_report) is list
    assert len(test_report) == 7
    check_report_data(test_report, edges_report_expected_list)


def test_create_nodes_report_list(kg_report_edges_1, kg_report_nodes_1, edges_report_expected_list):
    test_report = create_edges_report(kg_report_edges_1, kg_report_nodes_1, data_type=list)

    assert type(test_report) is list
    assert len(test_report) == 7
    check_report_data(test_report, edges_report_expected_list)


def test_create_nodes_report_dict(kg_report_edges_1, kg_report_nodes_1, edges_report_expected_dict):
    test_report = create_edges_report(kg_report_edges_1, kg_report_nodes_1, data_type=dict)

    assert type(test_report) is dict
    assert len(test_report) == 7
    check_report_data(test_report, edges_report_expected_dict)
