import pytest
from tests.test_utils import string_df
from cat_merge.merge_utils import clean_edges
from typing import Tuple
from pandas.core.frame import DataFrame


@pytest.fixture
def nodes_and_edges() -> Tuple[DataFrame, DataFrame]:
    nodes = u"""\
    id      category 
    Gene:1  Gene 
    Gene:2  Gene 
    Gene:3  Gene
    Gene:4  Gene
    Disease:1  Disease
    Disease:2  Disease
    Disease:3  Disease
    Disease:4  Disease
    """

    edges = u"""\
    id      subject object 
    uuid:1  Gene:1  Disease:1 
    uuid:2  Gene:2  Disease:2
    uuid:3  Gene:3  Disease:5
    uuid:4  Gene:5  Disease:3
    """

    return string_df(nodes), string_df(edges)


def test_clean_edges(nodes_and_edges):
    nodes = nodes_and_edges[0]
    edges = nodes_and_edges[1]

    cleaned_edges = clean_edges(edges=edges, nodes=nodes)

    assert len(cleaned_edges.shape) == 2
    assert list(cleaned_edges["subject"]) == ["Gene:1", "Gene:2"]
    assert list(cleaned_edges["object"]) == ["Disease:1", "Disease:2"]

