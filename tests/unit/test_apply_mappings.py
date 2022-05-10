import pytest
from tests.test_utils import string_df
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
def mapping():  # owl:sameAs
    mapping = u"""\
    subject_id  object_id
    XGene:1     Gene:1
    XGene:2     Gene:2
    XGene:3     Gene:3
    XDisease:1  Disease:1
    XDisease:2  Disease:2
    XDisease:3  Disease:3
    XDisease:4  Disease:4
    """
    return string_df(mapping, index_column_is_id=False)


def test_apply_mappings(edges, mapping):
    mapped_edges = apply_mappings(edges, mapping)

    assert mapped_edges.loc['uuid:3']['subject'] == 'Gene:2'
    assert mapped_edges.loc['uuid:3']['object'] == 'Disease:3'
    assert mapped_edges.loc['uuid:4']['subject'] == 'Gene:3'
    assert mapped_edges.loc['uuid:4']['object'] == 'Disease:4'


def test_original_subject_and_object(edges, mapping):
    mapped_edges = apply_mappings(edges, mapping)

    assert mapped_edges.loc['uuid:3']['original_subject'] == 'XGene:2'
    assert mapped_edges.loc['uuid:3']['original_object'] == 'XDisease:3'
    assert mapped_edges.loc['uuid:4']['original_object'] == 'XDisease:4'
