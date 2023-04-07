import pytest
from tests.test_utils import string_df, value
from cat_merge.mapping_utils import apply_mappings

@pytest.fixture
def edges():
    edges = u"""\
    id      subject  object 
    uuid:1  Gene:1   Disease:1 
    uuid:2  XGene:2  Disease:2
    uuid:3  Gene:2   XDisease:3
    uuid:4  XGene:3  XDisease:4
    """
    return string_df(edges)


@pytest.fixture
def mapping():
    mapping = u"""\
    subject_id  predicate       object_id
    Gene:1     skos:exactMatch XGene:1
    Gene:2     skos:exactMatch XGene:2
    Gene:3     skos:exactMatch XGene:3
    Disease:1  skos:exactMatch XDisease:1
    Disease:2  skos:exactMatch XDisease:2
    Disease:3  skos:exactMatch XDisease:3
    Disease:4  skos:exactMatch XDisease:4
    """
    return string_df(mapping)


def test_apply_mappings(edges, mapping):
    mapped_edges = apply_mappings(edges, mapping)
    assert value(mapped_edges, 'uuid:3', 'subject') == 'Gene:2'
    assert value(mapped_edges, 'uuid:3', 'object') == 'Disease:3'
    assert value(mapped_edges, 'uuid:4', 'subject') == 'Gene:3'
    assert value(mapped_edges, 'uuid:4', 'object') == 'Disease:4'


def test_original_subject_and_object(edges, mapping):
    mapped_edges = apply_mappings(edges, mapping)

    assert value(mapped_edges, 'uuid:2', 'original_subject') == 'XGene:2'
    assert value(mapped_edges, 'uuid:3', 'original_object') == 'XDisease:3'
    assert value(mapped_edges, 'uuid:4', 'original_object') == 'XDisease:4'


def test_no_extra_columns(edges, mapping):
    edge_columns = list(edges.columns)
    mapped_edges = apply_mappings(edges, mapping)
    mapped_edge_columns = set(mapped_edges.columns)
    expected_columns = set(edge_columns + ["original_subject", "original_object"])
    assert mapped_edge_columns == expected_columns

