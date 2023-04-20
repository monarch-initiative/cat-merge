import pytest
from tests.test_utils import string_df
from cat_merge.merge_utils import merge_kg
from typing import Tuple, List
from pandas.core.frame import DataFrame


@pytest.fixture
def nodes_and_edges() -> Tuple[List[DataFrame], List[DataFrame]]:
    nodes = []
    edges = []

    gene_nodes = u"""\
    id      category xref
    Gene:1  Gene     NCBI:10
    Gene:2  Gene     ZFIN:123 
    Gene:3  Gene     HGNC:11  
    Gene:4  Gene 
    Gene:4  Gene 
    """
    nodes.append(string_df(gene_nodes))

    phenotype_nodes = u"""\
    id      category
    Pheno:1  Disease
    Pheno:2  Disease
    Pheno:3  Disease
    Pheno:4  Disease
    """
    nodes.append(string_df(phenotype_nodes))

    disease_nodes = u"""\
    id      category
    Disease:1  Disease
    Disease:2  Disease
    Disease:3  Disease
    Disease:4  Disease
    """
    nodes.append(string_df(disease_nodes))

    g2d_edges = u"""\
    id      subject object 
    uuid:1  Gene:1  Disease:4 
    uuid:2  Gene:2  Disease:3
    uuid:3  Gene:3  Disease:2
    uuid:4  Gene:4  Disease:1
    """
    edges.append(string_df(g2d_edges))

    g2p_edges = u"""\
    id      subject object 
    uuid:5  Gene:1  Pheno:1 
    uuid:6  Gene:2  Pheno:2
    uuid:7  Gene:5  Pheno:5
    uuid:8  Gene:5  Pheno:1
    """
    edges.append(string_df(g2p_edges))

    d2p_edges = u"""\
    id      subject object 
    uuid:9  Disease:1 Pheno:2 
    uuid:10 Disease:2 Pheno:4
    uuid:11 Disease:1 Pheno:5
    uuid:12 Disease:5 Pheno:1
    """
    edges.append(string_df(d2p_edges))

    return nodes, edges


def test_merge_kg_node_count(nodes_and_edges):
    kg, qc = merge_kg(node_dfs=nodes_and_edges[0], edge_dfs=nodes_and_edges[1])
    assert(len(kg.nodes) == 12)


def test_merge_kg_edge_count(nodes_and_edges):
    kg, qc = merge_kg(node_dfs=nodes_and_edges[0], edge_dfs=nodes_and_edges[1])
    assert(len(kg.edges) == 8)


def test_merge_kg_dangling_edge_count(nodes_and_edges):
    kg, qc = merge_kg(node_dfs=nodes_and_edges[0], edge_dfs=nodes_and_edges[1])
    assert(len(qc.dangling_edges) == 4)


def test_merge_kg_duplicate_node_count(nodes_and_edges):
    kg, qc = merge_kg(node_dfs=nodes_and_edges[0], edge_dfs=nodes_and_edges[1])
    assert(len(qc.duplicate_nodes) == 2)
